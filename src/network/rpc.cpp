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

#include "include/network/rpc.h"
#include "common/common.h"
#include "common/logging.h"

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

void Rpc::send_message(int fd, const char *msg, size_t numBytes) {
    size_t bytesWritten = 0;

    while (bytesWritten < numBytes) {
        int ret = write(fd, msg + bytesWritten, numBytes - bytesWritten);
        LogAssert(ret >= 0, "write failed (" << ret << "): " << errno);

        if (ret == 0) {
            //LogInfo("Client connection closed");
            //exit(0);
        }

        //LogInfo("Sent " << ret << " bytes");
        bytesWritten += ret;
    }
}

void Rpc::recv_message(int fd, char *buf, size_t numBytes) {
    size_t bytesRead = 0;

    while (bytesRead < numBytes) {
        int ret = read(fd, buf + bytesRead, numBytes - bytesRead);
        LogAssert(ret >= 0, "read failed (" << ret << "): " << errno);

        if (ret == 0) {
            //LogInfo("Client connection closed");
            //exit(0);
        }
        //LogInfo("Read " << ret << " bytes");
        bytesRead += ret;
    }
}

void Rpc::send_int(int fd, int value) {
    //LogInfo("Sending int: " << value);
    send_message(fd, (char*)&value, sizeof(value));
}

int Rpc::recv_int(int fd) {
    int value;
    recv_message(fd, (char*)&value, sizeof(value));
    //LogInfo("Received int: " << value);
    return value;
}

void Rpc::send_str(int fd, const std::string &str) {
    //LogInfo("Sending string: " << str);

    const size_t buf_len = str.length() + 1 + sizeof(int);
    char buf[buf_len];

    // Append size of string
    int len = str.length() + 1;
    memcpy(buf, (char*)&len, sizeof(len));

    // Append string
    memcpy(buf + sizeof(len), str.c_str(), len);

    send_message(fd, buf, buf_len);
}

std::string Rpc::recv_str(int fd) {
    // Get length of string
    int len = 0;
    recv_message(fd, (char*)&len, sizeof(len));

    // Get string
    char buf[len];
    recv_message(fd, buf, len);

    //LogInfo("Received string: " << buf);

    return std::string(buf);
}


RpcClient::RpcClient(const std::string &host, int port) {
    struct sockaddr_in serveraddr;
    struct hostent *server;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    LogAssert(sockfd >= 0, "Could not create socket");

    server = gethostbyname(host.c_str());
    LogAssert(server != NULL, "No such host as " << host);

    bzero((char *)&serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serveraddr.sin_addr.s_addr,
          server->h_length);
    serveraddr.sin_port = htons(port);

    // To remove delay from ack messages
    int one = 1;
    setsockopt(sockfd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));

    LogAssert(connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) >= 0, "Could not connect");

    fd = sockfd;
}

std::string RpcClient::readRequest(const std::string &key) {
    // Send message type
    send_int(fd, READ_REQUEST);

    // Send key
    send_str(fd, key);

    // Wait for reply
    std::string readValue = recv_str(fd);

    return readValue;
}

void RpcClient::writeRequest(const std::string &key, const std::string &value) {
    // Send message type
    send_int(fd, WRITE_REQUEST);

    // Send key
    send_str(fd, key);

    // Send value
    send_str(fd, value);

    // Wait for ack
    recv_int(fd);
}




int create_and_bind(const char *port) {
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int s, sfd;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    s = getaddrinfo(NULL, port, &hints, &result);
    LogAssert(s == 0, "getaddrinfo");

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1) {
            continue;
        }

        int yes = 1;
        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        s = bind(sfd, rp->ai_addr, rp->ai_addrlen);
        if (s == 0) {
            break;
        }

        close(sfd);
    }

    LogAssert(rp != NULL, "Could not bind");

    freeaddrinfo(result);

    return sfd;
}

int make_socket_non_blocking(int sfd) {
    int flags, s;

    flags = fcntl(sfd, F_GETFL, 0);
    LogAssert(flags != -1, "fcntl");

    flags |= O_NONBLOCK;
    s = fcntl(sfd, F_SETFL, flags);
    LogAssert(s != -1, "fcntl");

    return 0;
}


RpcServer::RpcServer(KVServer *server, int port) {
    // TODO: spawn worker threads?

    listenfd = create_and_bind(std::to_string(port).c_str());
    LogAssert(listenfd != -1, "create_and_bind");

    //make_socket_non_blocking(listenfd);

    kvServer = server;

    for (int i = 0; i < NUM_WORKERS; i++) {
        std::thread(&RpcServer::handleRequest, this, i).detach();
    }
}

void RpcServer::listenForRequests() {
    LogAssert(listen(listenfd, MAX_CLIENTS) != -1, "listen failed");

    int nextWorkerAssign = 0;
    for (;;) {
        struct sockaddr_storage remoteaddr;
        socklen_t addrlen = sizeof(remoteaddr);
        int newfd = accept(listenfd, (struct sockaddr *)&remoteaddr, &addrlen);
        LogAssert(newfd != -1, "accept");

        std::unique_lock<std::mutex> workerLock(workersMtx[nextWorkerAssign]);
        incomingConnections[nextWorkerAssign].push(newfd);
        workersCv[nextWorkerAssign].notify_one();
        workerLock.unlock();
        //LogInfo("Accepted new connection. Assigning to worker " << nextWorkerAssign);
        nextWorkerAssign = (nextWorkerAssign + 1) % NUM_WORKERS;
    }
}

void RpcServer::handleRequest(int id) {
    FD_ZERO(&master[id]);
    FD_ZERO(&read_fds[id]);
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 5000;

    std::vector<int> fds;

    // Wait for initial connection to be assigned
    std::unique_lock<std::mutex> lock(workersMtx[id]);
    while (incomingConnections[id].empty()) {
        workersCv[id].wait(lock);
    }
    //LogInfo("Worker " << id << " got new connection");
    int newfd = incomingConnections[id].front();
    incomingConnections[id].pop();
    int fdmax = newfd;
    fds.push_back(newfd);
    FD_SET(newfd, &master[id]);
    lock.unlock();

    for (;;) {
        // Check for new connections
        std::unique_lock<std::mutex> lock(workersMtx[id]);
        while (!incomingConnections[id].empty()) {
            //LogInfo("Worker " << id << " got new connection");
            int newfd = incomingConnections[id].front();
            incomingConnections[id].pop();
            fds.push_back(newfd);
            FD_SET(newfd, &master[id]);
            if (newfd > fdmax) {
                fdmax = newfd;
            }
        }
        lock.unlock();

        read_fds[id] = master[id];
        LogAssert(select(fdmax + 1, &read_fds[id], NULL, NULL, &tv) != -1, "select");

        for (int i : fds) {
            if (FD_ISSET(i, &read_fds[id])) {
                int requestType = recv_int(i);

                if (requestType == READ_REQUEST) {
                    handleReadRequest(i);
                } else if (requestType == WRITE_REQUEST) {
                    handleWriteRequest(i);
                } else {
                    LogError("Invalid request type: " << requestType);
                }
            }
        }
    }
}

void RpcServer::handleReadRequest(int fd) {
    // Get key
    std::string key = recv_str(fd);

    // Submit read request
    std::string value = kvServer->get(key);

    // Send value
    send_str(fd, value);
}

void RpcServer::handleWriteRequest(int fd) {
    // Get key
    std::string key = recv_str(fd);
    std::string value = recv_str(fd);

    // Submit write request
    kvServer->put(key, value);

    // Send ack
    send_int(fd, WRITE_OK);
}

