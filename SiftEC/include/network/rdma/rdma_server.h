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

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <thread>
#include <rdma/rdma_cma.h>

#include "include/network/rdma/rdma.h"
#include "include/network/work_request.h"

struct server_connection {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *send_mr;
    struct ibv_mr *rdma_local_mr;
    struct message *send_msg;

    char *rdma_local_region;
    send_state ss;
    recv_state rs;

    struct context *s_ctx = NULL;
};

class Server {
    // memory server for the log
public:
    struct sockaddr_in6 addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_cm_id *conn_id = NULL;
    struct rdma_event_channel *ec = NULL;
    uint16_t port = 5000;
    struct ibv_mr *send_mr;
    struct ibv_mr *rdma_local_mr;
    struct message *send_msg;
    char *rdma_local_region;

    struct context *s_ctx = NULL;

    bool connected_once = false;

    Server(int port) : port(port) {}
};

class MemoryServer{
private:
    // default variables
    uint32_t back_log = 1024;
    const int QUEUE_SIZE = 16000;

    // server variables
    Server *log_server;

    // polling thread
    std::thread* t_log;

    // global registered memory
    /*local variables for globally registered memory*/
    int count = 0;

public:
    // common functions to build rdma connection
    MemoryServer();
    MemoryServer(const char* port);

    void CreateConnection();
    void callJoin();

    // for event based calls
    void* event_loop_log(void* param);

    int on_event(struct rdma_cm_event *event, bool isAdmin);
    int on_connect_request(struct rdma_cm_id *id, bool isAdmin);

    int on_connection(struct rdma_cm_id *id);
    int on_disconnect(struct rdma_cm_id *id);


    // common functions
    void build_connection(struct rdma_cm_id *id, bool isAdmin);
    void on_connect(void *context);
    void send_mr_message(void *context);
    void build_params(struct rdma_conn_param *params);

    void build_context(Server* server, struct ibv_context *verbs);
    void build_qp_attr(Server* sever, struct ibv_qp_init_attr *qp_attr);
    void register_memory(Server* server, WorkRequest *workRequest);
    void* poll_cq(struct context * s_ctx);

    void on_completion(struct ibv_wc *wc);
    void send_message(WorkRequest *workRequest);
};
