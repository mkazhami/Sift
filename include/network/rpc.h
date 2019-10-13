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

#include <queue>
#include <mutex>
#include <string>
#include "include/kv_store/kv_server.h"

#define MSG_TYPE_SIZE sizeof(int)
#define READ_REQUEST 1
#define WRITE_REQUEST 2
#define WRITE_OK 1000

#define MAX_CLIENTS 1000
#define SERVER_ADDR "10.30.0.100"
#define SERVER_PORT 14141
#define NUM_WORKERS 10

class Rpc {
protected:
    void send_message(int fd, const char *msg, size_t numBytes);
    void recv_message(int fd, char *buf, size_t numBytes);

    void send_int(int fd, int value);
    int recv_int(int fd);
    void send_str(int fd, const std::string &str);
    std::string recv_str(int fd);
};

class RpcClient : public Rpc {
public:
    RpcClient(const std::string &host, int port);
    std::string readRequest(const std::string &key);
    void writeRequest(const std::string &key, const std::string &value);
private:
    int fd;
};

class RpcServer : public Rpc {
public:
    RpcServer(KVServer*, int port);
    void listenForRequests();
    void handleRequest(int id);
private:
    void handleReadRequest(int fd);
    void handleWriteRequest(int fd);

    int listenfd;
    KVServer *kvServer;

    fd_set master[NUM_WORKERS];
    fd_set read_fds[NUM_WORKERS];
    std::mutex workersMtx[NUM_WORKERS];
    std::condition_variable workersCv[NUM_WORKERS];
    std::queue<int> incomingConnections[NUM_WORKERS];
};


