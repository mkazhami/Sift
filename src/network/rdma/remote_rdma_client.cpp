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

#include "include/util/serializable_address_value_pair.h"
#include "include/network/rdma/remote_rdma_client.h"
#include "include/memory_replication/coordinator_admin.h"
#include "common/logging.h"
#include "common/test.h"

#include <sys/mman.h>
#include <fcntl.h>
#include <poll.h>

CoordinatorAdmin *RemoteCoordinator::admin;
void (*RemoteCoordinator::failure_callback)(int);
void (*RemoteCoordinator::retry_callback)(WorkRequest*);

RemoteCoordinator::RemoteCoordinator(char *address, char *port, int node_id) {
    this->node_id = node_id;
    state = INIT;
    for (uint64_t i = 0; i < NUM_INDEX_BUFFER_BLOCKS; i++) {
        local_free_index_buffer_blocks.enqueue(i * INDEX_BUFFER_BLOCK_SIZE);
        remote_free_index_buffer_blocks.enqueue(i * INDEX_BUFFER_BLOCK_SIZE);
    }
    for (uint64_t i = 0; i < NUM_DATA_BUFFER_BLOCKS; i++) {
        local_free_data_buffer_blocks.enqueue(DATA_BUFFER_OFFSET + i * DATA_BUFFER_BLOCK_SIZE);
        remote_free_data_buffer_blocks.enqueue(DATA_BUFFER_OFFSET + i * DATA_BUFFER_BLOCK_SIZE);
    }

    c = new client();
    c->port = port;
    c->address = address;

    send_ticket = 0;
    sent = 0;
    completed = 0;

    LogDebug("Address is : " << c->address);
    LogDebug("Port is : " << c->port);

    LogDebug("Setting up client variables");
    TEST_Z((c->ec) = rdma_create_event_channel());

    LogDebug("Declaring variables and connected int");

    pthread_mutex_init(&mutex_conn_new, NULL);
    pthread_cond_init (&cv_conn_new, NULL);

    c->mutex_conn = &mutex_conn_new;

    c->cv_conn = &cv_conn_new;

    c->connected = new int();
    *(c->connected) = 0;
    poller_exit = false;

    event_thread = new std::thread(&RemoteCoordinator::event_loop_client, this, (void*)NULL );
}

RemoteCoordinator::~RemoteCoordinator() {
    LogInfo("Destroying the coordinator");
    pthread_mutex_destroy(c->mutex_conn );
    pthread_cond_destroy( c->cv_conn );
    rdma_destroy_event_channel(c->ec);
    delete c;
}

uint64_t RemoteCoordinator::getRDMABufferBlock(size_t size, WorkRequestCommand type) {
    uint64_t buffer_offset = -1;
    if (size <= INDEX_BUFFER_BLOCK_SIZE) {
        if (type == WRITE_) {
            remote_free_index_buffer_blocks.wait_dequeue(buffer_offset);
        } else {
            local_free_index_buffer_blocks.wait_dequeue(buffer_offset);
        }
    } else {
        if (type == WRITE_) {
            remote_free_data_buffer_blocks.wait_dequeue(buffer_offset);
        } else {
            local_free_data_buffer_blocks.wait_dequeue(buffer_offset);
        }
    }

    if (buffer_offset == (uint64_t)-1) {
        DIE("Buffer offset cannot be -1");
    }

    return buffer_offset;
}

void RemoteCoordinator::clearRDMABufferBlock(connection *conn, uint64_t offset, WorkRequestCommand type) {
    if (offset == -1) return;

    if (offset < DATA_BUFFER_OFFSET) {
        if (type == READ_ || type == CAS) {
            memset(conn->rdma_local_region + offset, '\0', INDEX_BUFFER_BLOCK_SIZE);
        } else if (type == WRITE_) {
            memset(conn->rdma_remote_region + offset, '\0', INDEX_BUFFER_BLOCK_SIZE);
        }
    } else {
        if (type == READ_ || type == CAS) {
            memset(conn->rdma_local_region + offset, '\0', DATA_BUFFER_BLOCK_SIZE);
        } else  if (type == WRITE_) {
            memset(conn->rdma_remote_region + offset, '\0', DATA_BUFFER_BLOCK_SIZE);
        }
    }
}

void RemoteCoordinator::freeRDMABufferBlock(connection *conn, uint64_t offset, WorkRequestCommand type) {
    if (offset == -1) return;

    if (offset < DATA_BUFFER_OFFSET) {
        if (type == WRITE_) {
            remote_free_index_buffer_blocks.enqueue(offset);
        } else {
            local_free_index_buffer_blocks.enqueue(offset);
        }
    } else {
        if (type == WRITE_) {
            remote_free_data_buffer_blocks.enqueue(offset);
        } else {
            local_free_data_buffer_blocks.enqueue(offset);
        }
    }
}


int RemoteCoordinator::Read(WorkRequest * workReq, bool force) {
    if (state != RUNNING && !force) {
        return -1;
    }

    workReq->setWorkRequestCommand(READ_);

    struct connection * conn = (struct connection*)((WorkRequest *)c->conn->context)->getConnection();
    workReq->setConnection(conn);

    rdma_read(workReq, workReq->getOffset(), workReq->getSize());

    return 1;
}

int RemoteCoordinator::CompSwap(WorkRequest * workReq, bool force) {
    if (state != RUNNING && !force) {
        return -1;
    }

    workReq->setWorkRequestCommand(CAS);

    struct connection * conn = (struct connection*)((WorkRequest *)c->conn->context)->getConnection();
    workReq->setConnection(conn);

    rdma_compswap(workReq, workReq->getOffset(), workReq->getCompare(), workReq->getSwap());

    return 1;
}

int RemoteCoordinator::Write(WorkRequest * workReq, bool force) {
    if (state != RUNNING && !force) {
        return -1;
    }

    workReq->setWorkRequestCommand(WRITE_);

    struct connection * conn = (struct connection*)((WorkRequest *)c->conn->context)->getConnection();
    workReq->setConnection(conn);

    rdma_write(workReq, workReq->getOffset(), workReq->getSize(), workReq->getValue());

    return 1;
}

int RemoteCoordinator::Write(std::vector<WorkRequest*> &workRequests, bool force) {
    if (state != RUNNING && !force) {
        return -1;
    }

    std::vector<uint64_t> offsets(workRequests.size());
    std::vector<size_t> sizes(workRequests.size());
    std::vector<char*> buffers(workRequests.size());
    for (int i = 0; i < workRequests.size(); i++) {
        WorkRequest *workRequest = workRequests[i];
        workRequest->setWorkRequestCommand(WRITE_);
        workRequest->setConnection((struct connection*)((WorkRequest *)c->conn->context)->getConnection());
        offsets[i] = workRequest->getOffset();
        sizes[i] = workRequest->getSize();
        buffers[i] = workRequest->getValue();
    }

    rdma_write(workRequests, offsets, sizes, buffers);

    return 1;
}

bool RemoteCoordinator::CreateConnection() {
    if (c->conn != nullptr && ((WorkRequest*)c->conn->context)->getConnection() != nullptr) {
        destroy_connection(c->conn->context);
    }

    int err;

    err = getaddrinfo((c->address), c->port, NULL, &(c->addr));
    if (err != 0) {
        DIE("getaddrinfo failed: " << gai_strerror(err));
    }
    TEST_NZ(rdma_create_id((c->ec), &(c->conn), NULL, RDMA_PS_TCP));
    int ret = rdma_resolve_addr((c->conn), NULL, (c->addr)->ai_addr, TIMEOUT_IN_MS);
    freeaddrinfo(c->addr);

    if (ret != 0) {
        LogError("Failed to resolve address - " << strerror(errno));
        return false;
    }

    // Wait for connection to be established
    // TODO: check every X ms for connection
    std::this_thread::sleep_for(std::chrono::seconds(5));

    if (*(c->connected) != 1) {
        LogError("Failed to connect to RDMA server");
        *(c->connected) = 0;
    }

    state = RECOVER;

    // TODO: call client_failure() here? have to be careful in case connection is established after timeout

    return *(c->connected) == 1;
}

void* RemoteCoordinator::event_loop_client(void *param) {
    while (rdma_get_cm_event((c->ec), &(c->event)) == 0) {
        struct rdma_cm_event event_copy;
        memcpy(&event_copy, c->event, sizeof(*(c->event)));
        rdma_ack_cm_event(c->event);

        if (RemoteCoordinator::on_event(&event_copy))
            LogError("on_event return non-zero");
    }
    return NULL;
}

int RemoteCoordinator::on_event(struct rdma_cm_event *event) {
    int r = 0;

    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
        r = on_addr_resolved(event->id);
    } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
        r = on_route_resolved(event->id);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
        LogDebug("RDMA_CM_EVENT_ESTABLISHED");
        r = on_connection(event->id);
    } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
        LogError("RDMA_CM_EVENT_DISCONNECTED");
        r = on_disconnect(event->id);
    } else if (event->event == RDMA_CM_EVENT_UNREACHABLE) {
        LogError("Unreachable memory_replication, possible network partition");
    } else if (event->event == RDMA_CM_EVENT_REJECTED) {
        LogError("Rejected, port closed");
        on_failed_connect(c->conn->context);
    } else if (event-> event == RDMA_CM_EVENT_ADDR_ERROR) {
        LogError("RDMA_CM_EVENT_ADDR_ERROR");
    } else if (event-> event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        LogError("RDMA_CM_EVENT_CONNECT_REQUEST");
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
        LogError("on_event: " << event->event);
        return -1;
    }

    return r;
}

int RemoteCoordinator::on_addr_resolved(struct rdma_cm_id *id) {
    LogDebug("Addr resolved");
    build_connection(id, c);
    TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

    return 0;
}

int RemoteCoordinator::on_connection(struct rdma_cm_id *id) {
    //on_connect(id->context); // called after MR is received, ignore this since we are not sending any MR to memory_replication to do ops
    return 0;
}


int RemoteCoordinator::on_disconnect(struct rdma_cm_id *id) {
    LogDebug("Disconnected");
    client_failure();
    return 1;
}


int RemoteCoordinator::on_route_resolved(struct rdma_cm_id *id) {
    LogDebug("Route resolved");
    struct rdma_conn_param cm_params;

    build_params(&cm_params);
    TEST_NZ(rdma_connect(id, &cm_params));

    return 0;
}

void RemoteCoordinator::build_connection(struct rdma_cm_id *id, client *c) {
    WorkRequest *workRequest = new WorkRequest();
    struct connection *conn;

    struct ibv_qp_init_attr qp_attr;

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
    conn = (struct connection *)malloc(sizeof(struct connection));
    workRequest->setConnection(conn);

    id->context = workRequest;
    conn->id = id;
    conn->qp = id->qp;
    conn->rs = RS_INIT;
    conn->connected = (c->connected);
    conn->mutex_conn =  (c->mutex_conn);
    conn->cv_conn = (c->cv_conn);

    register_memory(workRequest);
    post_recv();

    LogDebug("Connection built");
}

void RemoteCoordinator::build_context(struct ibv_context *verbs) {
    if (s_ctx) {
        if (s_ctx->ctx != verbs)
            DIE("Cannot handle events in more than one context.");
        return;
    }

    LogDebug("building context");

    s_ctx = (struct context *)malloc(sizeof(struct context));

    s_ctx->ctx = verbs;

    LogDebug("Building pd, comp channel and setting up poller thread");

    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, CQ_QUEUE_SIZE, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
    std::unique_lock<std::mutex> l(state_lock);
    state = INIT;
    l.unlock();
    LogDebug("Creating poller thread");
    s_ctx->cq_poller_thread = new std::thread(&RemoteCoordinator::poll_cq, this, (void*)NULL);
}

void RemoteCoordinator::build_qp_attr(struct ibv_qp_init_attr *qp_attr) {
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    // confirm what these attributes do and make them generic to caller functions on top
    qp_attr->cap.max_send_wr = CQ_QUEUE_SIZE;
    qp_attr->cap.max_recv_wr = CQ_QUEUE_SIZE;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void RemoteCoordinator::build_params(struct rdma_conn_param *params) {
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void *RemoteCoordinator::poll_cq(void * ctx) {
    LogDebug("poll_cq");
    struct ibv_cq *cq;
    struct ibv_wc wc;

    void* x = static_cast<void*> (s_ctx->ctx);
    int ret;

    // Set completion channel to be non-blocking
    // Otherwise failure detection becomes difficult due to ibv_get_cq_event() blocking
    int flags = fcntl(s_ctx->comp_channel->fd, F_GETFL);
    int rc = fcntl(s_ctx->comp_channel->fd, F_SETFL, flags | O_NONBLOCK);
    if (rc < 0) {
        DIE("Failed to change file descriptor of completion event channel");
    }
    struct pollfd my_pollfd;
    my_pollfd.fd = s_ctx->comp_channel->fd;
    my_pollfd.events = POLLIN;
    my_pollfd.revents = 0;
    const int ms_timeout = 5;

    bool done_poll = false;
    while (1) {
        // Poll completion channel for new event
        do {
            // If server has failed, continue waiting for events until all completions have returned, then break
            if (state == FAILED && sent == completed) {
                done_poll = true;
                break;
            }
            rc = poll(&my_pollfd, 1, ms_timeout);
        } while (rc == 0 || (rc == -1 && errno == EINTR));

        if (done_poll) break;

        //TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &x));
        while (ibv_get_cq_event(s_ctx->comp_channel, &cq, &x) != 0);
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        while ((ret = ibv_poll_cq(cq, 1, &wc))) {
            on_completion(&wc);
        }

        if (ret < 0) {
            LogError("ibv_poll_cq returned error");
        }
    }

    LogDebug("poll_cq thread exiting");
    poller_exit = false;

    return NULL;
}

void RemoteCoordinator::register_memory(WorkRequest *workRequest) {
    LogDebug("Registering memory");
    struct connection *conn = (struct connection*)workRequest->getConnection();
    conn->recv_msg = static_cast<struct message*>(malloc(sizeof(struct message)));

    conn->rdma_remote_region = static_cast<char*>(calloc(1, RDMA_BUFFER_SIZE));
    conn->rdma_local_region  = static_cast<char*>(calloc(1, RDMA_BUFFER_SIZE));
    if (mlock(conn->rdma_remote_region, RDMA_BUFFER_SIZE) == -1) {
        DIE("Could not lock remote RDMA buffer");
    }
    if (mlock(conn->rdma_local_region, RDMA_BUFFER_SIZE) == -1) {
        DIE("Could not lock local RDMA buffer");
    }

    TEST_Z(conn->recv_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->recv_msg,
            sizeof(struct message),
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));


    TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->rdma_remote_region,
            RDMA_BUFFER_SIZE,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));

    TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->rdma_local_region,
            RDMA_BUFFER_SIZE,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
}

void RemoteCoordinator::on_connect(void *context) {
    LogDebug("on_connect");
    WorkRequest *workRequest = (WorkRequest *)context;
    struct connection * conn = (struct connection*)workRequest->getConnection();
    pthread_mutex_lock((conn->mutex_conn));

    *(conn->connected) = 1;

    pthread_cond_signal((conn->cv_conn));
    pthread_mutex_unlock((conn->mutex_conn));
}

void RemoteCoordinator::on_failed_connect(void *context) {
    LogDebug("on_failed_connect");
    WorkRequest *workRequest = (WorkRequest *)context;
    struct connection * conn = (struct connection*)workRequest->getConnection();
    pthread_mutex_lock((conn->mutex_conn));

    *(conn->connected) = -1;

    pthread_cond_signal((conn->cv_conn));
    pthread_mutex_unlock((conn->mutex_conn));
}

void RemoteCoordinator::client_failure() {
    LogDebug("client_failure");
    std::unique_lock<std::mutex> l(state_lock);
    if (state == RUNNING) {
        state = FAILED;
        failure_callback(node_id);
    }
}

/*
  TODO: add remaining possible errors codes and handling, instead of just dying
  check if the QP does not go into a bad state and if it does, inform the client and ask him to recreate a message, or establish a valid contractr
*/

void RemoteCoordinator::post_recv() {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    WorkRequest *workRequest = (WorkRequest*)c->conn->context;
    struct connection *conn = (struct connection*)workRequest->getConnection();

    wr.wr_id = (uintptr_t)workRequest;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->recv_mr->lkey;

    LogDebug("Posting recv");
    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void RemoteCoordinator::post_send() {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    WorkRequest *workRequest = (WorkRequest*)c->conn->context;
    struct connection *conn = (struct connection*)workRequest->getConnection();

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)workRequest;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)conn->recv_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->recv_mr->lkey;

    LogDebug("Posting send");
    ibv_post_send(conn->qp, &wr, &bad_wr);
}

/* new API use struct enqueue wait for reply pop/memcpy function increment*/
void RemoteCoordinator::on_completion(struct ibv_wc *wc) {
    count_cq++;

    WorkRequest *workRequest = (WorkRequest *)(uintptr_t)wc->wr_id;
    struct connection *conn = (struct connection*)workRequest->getConnection();

    if (state != INIT) completed.fetch_add(1);
    else LogDebug("Got INIT or RECOVER completion");

    if (wc->status == IBV_WC_REM_ACCESS_ERR) {
        DIE("PROTECTED DOMAIN ERROR! offset: " << workRequest->getOffset()
             << " size: " << workRequest->getSize()
             << " command type: " << workRequest->getWorkRequestCommand()
             << " manage conflicts: " << workRequest->getManageConflictFlag());
    }

    if (wc->status != IBV_WC_SUCCESS) {
//        LogError("RDMA request unsuccessful - status code: " << wc->status << " - request type: " << workRequest->getRequestType()
//            << " - size: " << workRequest->getSize() << " - offset: " << workRequest->getOffset());
        // TODO: mark workrequest somehow so that it is still destroyed / progress is made
        freeRDMABufferBlock(conn, workRequest->getRDMABufferOffset(node_id), workRequest->getWorkRequestCommand());
        client_failure();
        retry_callback(workRequest);
        return;
    }

    if (state == RUNNING || (state == RECOVER && !USE_PERSISTENCE)) {
        uint64_t buffer_offset = workRequest->getRDMABufferOffset(node_id);
        WorkRequestCommand type = workRequest->getWorkRequestCommand();

        workRequest->getCallback()(workRequest, node_id); // notify caller of result

        freeRDMABufferBlock(conn, buffer_offset, type);
    }

    if (wc->opcode & IBV_WC_RECV) {
        if (conn->rs == RS_INIT) {
            conn->rs = RS_MR_RECV;
        } else {
            conn->rs = RS_DONE_RECV;
        }

        if (USE_PERSISTENCE && state == RECOVER) {
            LogDebug("Got recovery complete message from memory server");
            state = RUNNING;
        } else if (conn->recv_msg->msg_type == message::MSG_MR) {
            memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
            on_connect(conn->id->context);
            //state = USE_PERSISTENCE ? RECOVER : RUNNING;
            state = RECOVER;
        }
    }
}



void RemoteCoordinator::destroy_connection(void *context) {
    struct connection *conn = (struct connection*)((WorkRequest*)context)->getConnection();

    if (s_ctx->cq_poller_thread != nullptr) {
        std::unique_lock<std::mutex> l(state_lock);
        state = FAILED;
        l.unlock();
        poller_exit = true;

        LogDebug("Destroying connection - sent=" << sent << " completed=" << completed);
        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_ERR;
        TEST_NZ(ibv_modify_qp(conn->qp, &attr, IBV_QP_STATE));

        // Wait for all completions to return before destroying qp
        while (sent != completed) {
            LogDebug("Waiting for all work completions to come back (sent=" << sent << " completed=" << completed
                                                                           << ")");
            //std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Wait for polling thread to exit
        // TODO: no need to wait? no problems with having two polling threads for some time
        while (poller_exit) {
            LogDebug("Waiting for poller thread to exit");
        }
    }

    rdma_destroy_qp(conn->id);

    ibv_dereg_mr(conn->recv_mr);
    ibv_dereg_mr(conn->rdma_remote_mr);

    free(conn->recv_msg);
    free(conn->rdma_remote_region);

    rdma_destroy_id(conn->id);

    free(conn);
    ((WorkRequest*)context)->setConnection(nullptr);
    c->conn = nullptr;
    s_ctx->cq_poller_thread = nullptr;

    free(s_ctx);
    s_ctx = nullptr;
}


int RemoteCoordinator::rdma_read(WorkRequest *workRequest, uint64_t offset, size_t size) {
    uint64_t buffer_offset = getRDMABufferBlock(size, workRequest->getWorkRequestCommand());
    workRequest->setRDMABufferOffset(node_id, buffer_offset);

    struct connection *conn = (struct connection*)workRequest->getConnection();

    count_wr++;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)workRequest;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uint64_t)conn->peer_mr.addr + offset;
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_local_region + buffer_offset;
    sge.length = size;
    sge.lkey = conn->rdma_local_mr->lkey;

    uint64_t ticket = send_ticket.fetch_add(1);
    while (ticket > completed && ticket - completed > CQ_QUEUE_SIZE) {}

    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret != 0) {
        LogError("ibv_post_send failed");
        send_ticket.fetch_sub(1);
        freeRDMABufferBlock(conn, buffer_offset, workRequest->getWorkRequestCommand());
        client_failure();
        retry_callback(workRequest);
    }
    else {
        sent.fetch_add(1);
    }

    return 1;

}

int RemoteCoordinator::rdma_write(WorkRequest *workRequest, uint64_t offset, size_t size, char* buffer) {
    uint64_t buffer_offset = getRDMABufferBlock(size, workRequest->getWorkRequestCommand());
    workRequest->setRDMABufferOffset(node_id, buffer_offset);

    struct connection *conn = (struct connection*)workRequest->getConnection();

    memcpy(conn->rdma_remote_region + buffer_offset, buffer,  size); // copy to registered memory

    count_wr++;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)workRequest;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uint64_t)conn->peer_mr.addr + offset;
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_remote_region + buffer_offset;
    sge.length = size;
    sge.lkey = conn->rdma_remote_mr->lkey;

    uint64_t ticket = send_ticket.fetch_add(1);
    while (ticket > completed && ticket - completed > CQ_QUEUE_SIZE) {}//LogError("More than CQ_QUEUE_SIZE requests waiting - ticket=" << ticket << " completed=" << completed);}

    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret != 0) {
        LogError("ibv_post_send failed");
        send_ticket.fetch_sub(1);
        freeRDMABufferBlock(conn, buffer_offset, workRequest->getWorkRequestCommand());
        client_failure();
    }
    else {
        sent.fetch_add(1);
    }

    return 1;
}

int RemoteCoordinator::rdma_write(const std::vector<WorkRequest *> &wrs, const std::vector<uint64_t> &offsets, const std::vector<size_t> &sizes, const std::vector<char*> &buffers) {
    struct ibv_send_wr *send_wrs = new struct ibv_send_wr[wrs.size()];
    struct ibv_send_wr *bad_wr = NULL;
    struct ibv_sge *sges = new struct ibv_sge[wrs.size()];

    struct connection * conn = (struct connection*)((WorkRequest *)c->conn->context)->getConnection();

    for (int i = 0; i < wrs.size(); i++) {
        WorkRequest *workRequest = wrs[i];
        size_t size = sizes[i];
        uint64_t offset = offsets[i];
        char *buffer = buffers[i];

        uint64_t buffer_offset = getRDMABufferBlock(size, workRequest->getWorkRequestCommand());
        workRequest->setRDMABufferOffset(node_id, buffer_offset);

        memcpy(conn->rdma_remote_region + buffer_offset, buffer, size); // copy to registered memory

        memset(&send_wrs[i], 0, sizeof(struct ibv_send_wr));

        struct ibv_send_wr *wr = &send_wrs[i];
        struct ibv_sge *sge = &sges[i];

        wr->wr_id = (uintptr_t)workRequest;
        wr->opcode = IBV_WR_RDMA_WRITE;
        wr->sg_list = sge;
        wr->num_sge = 1;
        wr->send_flags = IBV_SEND_SIGNALED;
        wr->wr.rdma.remote_addr = (uint64_t)conn->peer_mr.addr + offset;
        wr->wr.rdma.rkey = conn->peer_mr.rkey;

        sge->addr = (uintptr_t)conn->rdma_remote_region + buffer_offset;
        sge->length = size;
        sge->lkey = conn->rdma_remote_mr->lkey;

        if (i != 0) {
            send_wrs[i-1].next = wr;
        }
    }

    uint64_t ticket = send_ticket.fetch_add(wrs.size());
    while (ticket > completed && ticket + wrs.size() - completed > CQ_QUEUE_SIZE) {}//LogError("More than CQ_QUEUE_SIZE requests waiting - ticket=" << ticket << " completed=" << completed)}

    int ret = ibv_post_send(conn->qp, &send_wrs[0], &bad_wr);
    if (ret != 0) {
        if (ret == ENOMEM) {
            DIE("ibv_post_send failed with ENOMEM - list of requests might be too large");
        }
        LogError("ibv_post_send failed: " << ret);
        struct ibv_send_wr *wr = &send_wrs[0];
        int successful = 0;
        while (wr != bad_wr) {
            successful++;
            wr = wr->next;
        }
        send_ticket.fetch_sub(wrs.size() - successful);
        for (WorkRequest *workRequest : wrs) {
            freeRDMABufferBlock(conn, workRequest->getRDMABufferOffset(node_id), workRequest->getWorkRequestCommand());
        }
        client_failure();
    }
    else {
        // TODO: check which wr failed, only increment up to that one
        sent.fetch_add(wrs.size());
    }

    return 1;
}

// Compare and swap data over this RdmaConnection
int RemoteCoordinator::rdma_compswap(WorkRequest *workRequest, uint64_t offset, uint64_t compare, uint64_t swap)
{
    uint64_t buffer_offset = getRDMABufferBlock(sizeof(uint64_t), workRequest->getWorkRequestCommand());
    workRequest->setRDMABufferOffset(node_id, buffer_offset);

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    struct connection *conn = (struct connection*)workRequest->getConnection();

    wr.wr_id = (uintptr_t)workRequest;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    wr.wr.atomic.remote_addr = (uint64_t)conn->peer_mr.addr + offset;
    wr.wr.atomic.rkey = conn->peer_mr.rkey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;

    sge.addr = (uintptr_t)conn->rdma_local_region + buffer_offset;
    sge.length = sizeof(uint64_t);
    sge.lkey = conn->rdma_local_mr->lkey;

    uint64_t ticket = send_ticket.fetch_add(1);
    while (ticket > completed && ticket - completed > CQ_QUEUE_SIZE) {}

    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret != 0) {
        LogError("ibv_post_send failed");
        send_ticket.fetch_sub(1);
        freeRDMABufferBlock(conn, buffer_offset, workRequest->getWorkRequestCommand());
        client_failure();
    }
    else {
        sent.fetch_add(1);
    }

    return 1;
}