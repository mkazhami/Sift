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

#include <rdma/rdma_cma.h>
#include <thread>

enum send_state {
    SS_INIT,
    SS_MR_SENT,
    SS_DONE_SENT,
    SS_RECOVER_SENT
};

enum recv_state {
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV,
    RS_RECOVER_RECV
};

enum node_state {
    INIT,
    RECOVER,
    RUNNING,
    FAILED
};


struct message {
    enum {
        MSG_MR,
        MSG_DONE
    } msg_type;

    union {
        struct ibv_mr mr;
    } data;
};

struct context {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    std::thread* cq_poller_thread;
};

struct client {
    struct addrinfo *addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;

    char *address = NULL;
    char *port = NULL;

    pthread_mutex_t *mutex_conn;
    pthread_cond_t *cv_conn;
    int *connected;
};

struct connection {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;
    int *connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *rdma_remote_mr;
    struct ibv_mr *rdma_local_mr;

    struct ibv_mr peer_mr;

    struct message *recv_msg;

    char *rdma_remote_region;
    char *rdma_local_region;
    recv_state rs;

    pthread_mutex_t *mutex_conn;
    pthread_cond_t *cv_conn;
};
