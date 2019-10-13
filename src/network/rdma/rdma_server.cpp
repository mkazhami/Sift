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

#include "include/network/rdma/rdma_server.h"
#include "common/logging.h"
#include "common/test.h"

#include <sys/mman.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        DIE("usage: ./memory_replication port\n");
    }
    MemoryServer srv((const char*) argv[1] );
    srv.CreateConnection();
    srv.callJoin();
    return 0;
}


MemoryServer::MemoryServer() {}

MemoryServer::MemoryServer(const char * port_) {
    uint16_t port = atoi(port_);
    log_server = new Server(port);
}

void MemoryServer::CreateConnection() {
    // set up connection and listen for each region
    LogInfo("Starting memory server on port " << log_server->port);

    LogDebug("Declaring the memory server variables");
    memset(&(log_server->addr), 0, sizeof((log_server->addr)));
    (log_server->addr).sin6_family = AF_INET;
    (log_server->addr).sin6_port = htons(log_server->port);

    LogDebug("Declaring rdma variables");
    TEST_Z((log_server->ec) = rdma_create_event_channel());
    TEST_NZ(rdma_create_id((log_server->ec), &(log_server->listener), NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr((log_server->listener), (struct sockaddr *)&(log_server->addr)));
    TEST_NZ(rdma_listen((log_server->listener), back_log));

    log_server->port = ntohs(rdma_get_src_port(log_server->listener));
    LogDebug("Memory sever is initialized");

    // set up poller threads
    LogInfo("Setting up poller threads");

    LogDebug("Setting up poller thread for log region");
    t_log = new std::thread(&MemoryServer::event_loop_log, this, (void*)NULL);
}


// must call join after creation of connection
void MemoryServer::callJoin() {
    t_log->join();
}


void* MemoryServer::event_loop_log (void* param) {
    while (rdma_get_cm_event(log_server->ec, &(log_server->event)) == 0) {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, log_server->event, sizeof(*(log_server->event)));
        rdma_ack_cm_event(log_server->event);

        if (on_event(&event_copy, false))
            break;

    }
    return NULL;
}


int MemoryServer::on_event(struct rdma_cm_event *event, bool isAdmin) {
    int r = 0;

    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        r = on_connect_request(event->id, isAdmin);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
        r = on_connection(event->id);
    } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
        r = on_disconnect(event->id);
    } else if (event->event == RDMA_CM_EVENT_UNREACHABLE) {
        LogError("Unreachable memory_replication, possible network partition");
    } else if (event->event == RDMA_CM_EVENT_REJECTED) {
        LogError("Rejected, port closed");
    } else if (event-> event == RDMA_CM_EVENT_ADDR_ERROR) {
        LogError("RDMA_CM_EVENT_ADDR_ERROR");
    } else if (event-> event == RDMA_CM_EVENT_CONNECT_RESPONSE) {
        LogError("RDMA_CM_EVENT_CONNECT_RESPONSE");
    } else if (event-> event == RDMA_CM_EVENT_CONNECT_ERROR) {
        LogError("RDMA_CM_EVENT_CONNECT_ERROR");
    } else if (event-> event == RDMA_CM_EVENT_DEVICE_REMOVAL) {
        LogError("RDMA_CM_EVENT_DEVICE_REMOVAL");
    } else if (event-> event == RDMA_CM_EVENT_MULTICAST_JOIN) {
        LogError("RDMA_CM_EVENT_MULTICAST_JOIN");
    } else if (event-> event == RDMA_CM_EVENT_MULTICAST_ERROR) {
        LogError("RDMA_CM_EVENT_MULTICAST_ERROR");
    } else if (event-> event == RDMA_CM_EVENT_ROUTE_ERROR) {
        LogError("RDMA_CM_EVENT_ROUTE_ERROR");
    } else if (event-> event == RDMA_CM_EVENT_ADDR_CHANGE) {
        LogError("RDMA_CM_EVENT_ADDR_CHANGE");
    } else if (event-> event == RDMA_CM_EVENT_TIMEWAIT_EXIT) {
        LogError("RDMA_CM_EVENT_TIMEWAIT_EXIT");
    } else {
        LogError("Weird (unknown) thing happened");
    }
    return r;
}


int MemoryServer::on_connect_request(struct rdma_cm_id *id, bool isAdmin) {
    /*logic to add more connections on requests, essentially a way
    to ensure that the memory_replication isnt getting connected in a rougue manner*/
    LogInfo("On connect request called, received connection request");

    struct rdma_conn_param cm_params;

    build_connection(id, isAdmin);
    build_params(&cm_params);

    TEST_NZ(rdma_accept(id, &cm_params));

    return 0;
}

int MemoryServer::on_connection(struct rdma_cm_id *id) {
    LogInfo("Connection established");

    on_connect(id->context);
    return 0;
}

int MemoryServer::on_disconnect(struct rdma_cm_id *id) {
    LogInfo("Peer disconnected.");

    rdma_disconnect(id);

    return 0;
}


void MemoryServer::build_connection(struct rdma_cm_id *id, bool isAdmin) {
    // read from files and register and pin memory
    struct server_connection *conn;
    struct ibv_qp_init_attr qp_attr;
    Server *server = NULL;

    LogInfo("Building connection");

    server = log_server;

    LogInfo("Building context");

    build_context(server, id->verbs);

    LogInfo("Building queue pair attributes");

    build_qp_attr(server,&qp_attr);

    LogInfo("Building QP");

    TEST_NZ(rdma_create_qp(id, server->s_ctx->pd, &qp_attr));

    WorkRequest *workRequest = new WorkRequest();
    conn = (struct server_connection *)malloc(sizeof(struct server_connection));
    workRequest->setConnection(conn);

    id->context = workRequest;

    conn->id = id;
    conn->qp = id->qp;
    conn->ss = SS_INIT;
    conn->rs = RS_INIT;
    conn->connected = 0;
    conn->s_ctx = server->s_ctx;
    log_server->conn_id = id;

    LogInfo("Registering memory");

    register_memory(server, workRequest);
}


void MemoryServer::build_context(Server* server, struct ibv_context *verbs) {
    /*do only once to build memory_replication vals.,
    it has completion queue decleration, of only 10 size, this is arbitrary,
    refactor later to include config variable*/

    if (server->s_ctx) {
        if (server->s_ctx->ctx != verbs)
            DIE("Cannot handle events in more than one context.");
        return;
    }

    server->s_ctx = (struct context *)malloc(sizeof(struct context));

    server->s_ctx->ctx = verbs;

    TEST_Z(server->s_ctx->pd = ibv_alloc_pd(server->s_ctx->ctx));
    TEST_Z(server->s_ctx->comp_channel = ibv_create_comp_channel(server->s_ctx->ctx));
    TEST_Z(server->s_ctx->cq = ibv_create_cq(server->s_ctx->ctx, QUEUE_SIZE, NULL, server->s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary, to be updated using the queue size (refactor this code) */
    TEST_NZ(ibv_req_notify_cq(server->s_ctx->cq, 0));

    server->s_ctx->cq_poller_thread = new std::thread(&MemoryServer::poll_cq, this, (server->s_ctx) );
}


void* MemoryServer::poll_cq(struct context* s_ctx) {
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1) {
        void* x = static_cast<void*> (s_ctx->ctx);
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &x));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc)) {
            on_completion(&wc);
        }
        // break;
    }

    return NULL;
}

void MemoryServer::build_qp_attr(Server* server, struct ibv_qp_init_attr *qp_attr) {
    // for passive memory_replication we might not actually need many things here
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = server->s_ctx->cq;
    qp_attr->recv_cq = server->s_ctx->cq;

    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = QUEUE_SIZE;
    qp_attr->cap.max_recv_wr = QUEUE_SIZE;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void MemoryServer::register_memory(Server* server, WorkRequest *workRequest) {
    LogInfo("Registering memory");

    struct server_connection *conn = (struct server_connection*)workRequest->getConnection();

    if (!server->connected_once) {
        LogDebug("Pinning memory");

        server->connected_once = true;

        conn->send_msg = static_cast<struct message*>(malloc(sizeof(struct message)));

        conn->rdma_local_region = static_cast<char*>(calloc(1,RDMA_MEMORY_SIZE));
        if (mlock(conn->rdma_local_region, RDMA_MEMORY_SIZE) == -1) {
            DIE("Could not lock local RDMA buffer");
        }

        TEST_Z(conn->send_mr = ibv_reg_mr(
                server->s_ctx->pd,
                conn->send_msg,
                sizeof(struct message),
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));

        TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
                server->s_ctx->pd,
                conn->rdma_local_region,
                RDMA_MEMORY_SIZE,
                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));


        server->send_msg = conn->send_msg;
        server->rdma_local_region = conn->rdma_local_region;
        server->send_mr = conn->send_mr;
        server->rdma_local_mr = conn->rdma_local_mr;
    } else {
        LogDebug("Linking memory");

        conn->send_mr = server->send_mr;
        conn->rdma_local_mr = server->rdma_local_mr;
        conn->send_msg = server->send_msg;
        conn->rdma_local_region = server->rdma_local_region;
    }

}


void MemoryServer::build_params(struct rdma_conn_param *params) { // this basically controls number of simultaneous read requests
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void MemoryServer::on_connect(void *context) {
    LogInfo("In on_connect");

    WorkRequest *workRequest = (WorkRequest*)context;
    ((struct server_connection*)workRequest->getConnection())->connected = 1;

    send_mr_message(workRequest);
}


void MemoryServer::on_completion(struct ibv_wc *wc){
    WorkRequest *workRequest = (WorkRequest*)(uintptr_t)wc->wr_id;
    struct server_connection *conn = (struct server_connection*)workRequest->getConnection();//(struct connection *)(uintptr_t)wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS)
        DIE("on_completion: status is not IBV_WC_SUCCESS (" << wc->status << ")");

    if (wc->opcode & IBV_WC_SEND) {
        if (conn->ss == SS_INIT) {
            conn->ss = SS_MR_SENT;
        } else if (conn->ss == SS_MR_SENT) {
            conn->ss = SS_DONE_SENT;
        }

        LogInfo("Send completed successfully.");
    }

    if (wc->opcode & IBV_WC_RECV) {
        LogInfo("IBV_WC_RECV successful");
        if (USE_PERSISTENCE && conn->rs == RS_INIT) {
            conn->rs = RS_RECOVER_RECV;
            LogInfo("Received recover message");
            recover();
        }
    }
}

void MemoryServer::send_mr_message(void *context) {
    LogInfo("Sending mr message to client");

    WorkRequest *workRequest = (WorkRequest*)context;
    struct server_connection *conn = (struct server_connection*)workRequest->getConnection();

    conn->send_msg->msg_type = message::MSG_MR;
    memcpy(&conn->send_msg->data.mr, conn->rdma_local_mr, sizeof(struct ibv_mr));

    send_message(workRequest);
}

void MemoryServer::send_message(WorkRequest *workRequest) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    struct server_connection *conn = (struct server_connection*)workRequest->getConnection();

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)workRequest;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)conn->send_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->send_mr->lkey;

    while (!conn->connected);

    LogInfo("Posting send");

    ibv_post_send(conn->qp, &wr, &bad_wr);
}

void MemoryServer::recover() {
#if USE_PERSISTENCE
    LogInfo("Recovering memory");

    WorkRequest *workRequest = (WorkRequest*) log_server->conn_id->context;

    persistentStore = PersistentStore::openSnapshot();
    auto entries = persistentStore->scanTable();
    persistentStore->closeSnapshot();

    if (entries.size() == 0) {
        LogInfo("No snapshot exists - nothing to recover");
    }

    for (auto entry : entries) {
        uint64_t address = entry.first;
        std::string value = entry.second;
        memcpy(RM_REPLICATED_MEMORY_OFFSET + ((struct server_connection*)workRequest->getConnection())->rdma_local_region + address, value.c_str(), value.length());
    }
    LogInfo("Recovered " << entries.size() << " address-value pairs");

    send_message(workRequest);
#endif
}
