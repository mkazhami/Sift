// Copyright 2019 Mikhail Kazhamiaka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "include/kv_store/kv_server.h"
#include <thread>
#include <sys/time.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <assert.h>
#include <algorithm>
#include <thread>
#include <mutex>
#include "util/rpc/kv_connection.h"

#include "common/logging.h"
#include "kv_server.h"
#include "kv_coordinator.h"

#define BACKLOG         128
#define NUM_IO_SERVERS  30


class RemoteKVServer {
public:

    RemoteKVServer(const std::string &host, const int &port, const uint32_t &serverId)
            : host(host), port(port) {
        kvServer = new KVServer(serverId);

        if (serverId == 0) {
//            LogInfo("Populating the kv store with " << KV_SIZE << " keys");
//            int populateThreads = 10;
//            int batchSize = KV_SIZE / populateThreads;
//            std::vector<std::thread> threads;
//            for (int i = 0; i < populateThreads; i++) {
//                int start = i * batchSize;
//                int end;
//                if (i == populateThreads - 1) {
//                    end = KV_SIZE;
//                } else {
//                    end = (i + 1) * batchSize;
//                }
//                threads.push_back(std::thread(&RemoteKVServer::populate, this, start, end));
//            }
//
//            for (int i = 0; i < populateThreads; i++) {
//                threads[i].join();
//            }
        }

        nextAcceptServer = 0;
    }

    ~RemoteKVServer() {}

    void Run() {
        int listen_sock = make_socket(port);
        if (listen(listen_sock, BACKLOG) < 0) {
            perror("listen");
            exit(EXIT_FAILURE);
        }
        for (int i = 0; i < NUM_IO_SERVERS; ++i) {
            connection_count[i] = 0;
        }

        kvServer->kvCoordinator->replicationServer->WaitForLeader();
        kvServer->kvCoordinator->waitForStatus(KVCoordinator::LEADER);

        std::thread t[NUM_IO_SERVERS];
        for (int i = 0; i < NUM_IO_SERVERS; ++i) {
            t[i] = std::thread(&RemoteKVServer::serve, this, i, listen_sock, kvServer);
        }

        LogInfo("Ready for client connections");

        for (int i = 0; i < NUM_IO_SERVERS; ++i) {
            t[i].join();
        }
    }

    void serve(int id, int listen_sock, KVServer *kv_server) {
        fd_set master_rdset, active_rdset, master_wrset, active_wrset;

        /* Initialize the set of active sockets. */
        FD_ZERO(&master_rdset);
        FD_ZERO(&master_wrset);
        FD_SET(listen_sock, &master_rdset);
        int max_fd = listen_sock;

        std::unordered_map<int, Connection*> connect_map;

        while (true) {
            /* Block until input arrives on one or more active sockets. */
            active_rdset = master_rdset;
            active_wrset = master_wrset;
            int rc = select(max_fd + 1, &active_rdset, &active_wrset, NULL, NULL);
            if (rc < 0) {
                perror("select");
                return;
            }
            if (rc == 0) {
                assert(false); // Timeout. Not expected for now.
            }
            int num_fds = rc;
            for (int i = 0; i <= max_fd && num_fds > 0; ++i) {
                bool close_connection = false;
                if (FD_ISSET(i, &active_rdset)) {
                    num_fds--;
                    if (i == listen_sock) {
                        // Accept one or more connections.
                        while (true) {
                            // Used to provide better load balance
                            std::unique_lock<std::mutex> l(global_lock);
                            if (nextAcceptServer != id) {
                                break;
                            }
                            l.unlock();

                            // Don't care about remote address for now
                            int new_sock = accept4(listen_sock, NULL, NULL,
                                                   SOCK_NONBLOCK);
                            if (new_sock < 0) {
                                if (errno == EWOULDBLOCK) {
                                    printf("Breaking out of loop (EWOULDBLOCK)\n");
                                    break; // Accepted all pending connections
                                } else {
                                    perror("accept");
                                    return;
                                }
                            }
                            // Turn off Nagle
                            int on = 1;
                            if (setsockopt(new_sock, SOL_TCP, TCP_NODELAY,
                                           (void *)&on, sizeof(on)) < 0 ) {
                                return;
                            }
                            printf("New connection using IO server %d\n", id);
                            // Create a new connection
                            Connection* c = new KVConnection(kv_server, new_sock);
                            connect_map[new_sock] = c;
                            // Add to read set and update max_fd
                            FD_SET(new_sock, &master_rdset);
                            max_fd = std::max(max_fd, new_sock);
                            // Increment connection counter
                            std::unique_lock<std::mutex> lock(global_lock);
                            connection_count[id]++;
                            nextAcceptServer = (nextAcceptServer + 1) % NUM_IO_SERVERS;
                        }
                    } else {
                        // Handling other sockets
                        Connection* c;
                        if (getConnection(&connect_map, &c, i, false) < 0) {
                            printf("Can't find connection\n");
                            return;
                        }
                        if (c->recvMsgs() == -1) {
                            close_connection = true;
                        } else {
                            // Check if there is data in the write buffer
                            if (c->hasBufferedWrites()) {
                                FD_SET(i, &master_wrset);
                            }
                        }
                    }
                }
                if (FD_ISSET(i, &active_wrset)) {
                    num_fds--;
                    Connection* c;
                    if (getConnection(&connect_map, &c, i, false) < 0) {
                        return;
                    }
                    if (c->flushWriteBuf() == -1) {
                        close_connection = true;
                    } else {
                        // Check if we need to keep this FD in the write set.
                        if (!c->hasBufferedWrites()) {
                            FD_CLR(i, &master_wrset);
                        }
                    }

                }
                // TODO: Update max_fd when removing a connection.
                if (close_connection) {
                    printf("Closing connection %d\n", i);
                    close(i);
                    FD_CLR(i, &master_rdset);
                    FD_CLR(i, &master_wrset);
                    getConnection(&connect_map, NULL, i, true);
                    // Decrement connection counter
                    std::unique_lock<std::mutex> lock(global_lock);
                    connection_count[id]--;
                }
            }
        }
    }

//private:
    int make_socket(uint16_t port) {
        /* Create the socket. */
        int sock = socket (AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror ("socket");
            exit(-1);
        }

        // Make it reuseable
        int on = 1;
        if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
                       (char*)&on, sizeof(on)) < 0) {
            perror("setsockopt");
            exit(-1);
        }

        // Set to nonblocking. Incoming connections should inherent
        // the non-blocking property, but it is not.
        if (fcntl(sock, F_SETFL, O_NONBLOCK) < 0) {
            perror("fcntl");
            exit(-1);
        }

        /* Give the socket a name. */
        struct sockaddr_in name;
        name.sin_family = AF_INET;
        name.sin_port = htons(port);
        name.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(sock, (struct sockaddr *) &name, sizeof(name)) < 0) {
            perror ("bind");
            exit (-1);
        }
        return sock;
    }

    int getConnection(std::unordered_map<int, Connection*>* cmap, Connection** out_c,
                      int sock, bool remove) {
        std::unordered_map<int, Connection*>::iterator find_it = cmap->find(sock);
        if (find_it == cmap->end()) {
            return -1;
        }
        Connection* c = find_it->second;
        if (remove) {
            cmap->erase(find_it);
            delete c;
            return 0;
        }
        *out_c = c;
        return 0;
    }

    void populate(int start, int end) {
        for (int i = start; i < end; i++) {
            std::string key("key" + std::to_string(i));
            std::string value("this is a test value " + std::to_string(i));
            kvServer->put(key, value);
        }
    }

    std::string host;
    int port;
    KVServer *kvServer;

    int nextAcceptServer;
    int connection_count[NUM_IO_SERVERS];
    std::mutex global_lock;

};
