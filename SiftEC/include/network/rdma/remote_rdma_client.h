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

#include "include/network/rdma/rdma_client.h"

class CoordinatorAdmin;

class RemoteCoordinator : public Coordinator {
public:
    RemoteCoordinator() {}
    RemoteCoordinator(char *address, char *port, int node_id);
    ~RemoteCoordinator();
    bool CreateConnection();
    int Read(WorkRequest* workReq);
    int Write(WorkRequest* workReq);
    int Write(std::vector<WorkRequest*> &workReqs);
    int CompSwap(WorkRequest* workReq);

    // common functions
    void build_connection(struct rdma_cm_id *id, client *c);
    void build_params(struct rdma_conn_param *params);
    void on_connect(void *context);
    void on_failed_connect(void *context);
    void destroy_connection(void *context);
    int rdma_read(WorkRequest *wr, uint64_t offset, size_t size);
    int rdma_write(WorkRequest *wr, uint64_t offset, size_t size, char* buffer);
    int rdma_write(const std::vector<WorkRequest *> &wrs, const std::vector<uint64_t> &offsets, const std::vector<size_t> &sizes, const std::vector<char*> &buffers);
    int rdma_compswap(WorkRequest *wr, uint64_t offset, uint64_t compare, uint64_t swap);

    //connection funcitons
    void* event_loop_client(void *param);
    int on_event(struct rdma_cm_event *event);
    int on_addr_resolved(struct rdma_cm_id *id);
    int on_connection(struct rdma_cm_id *id);
    int on_disconnect(struct rdma_cm_id *id);
    int on_route_resolved(struct rdma_cm_id *id);

    void build_context(struct ibv_context *verbs);
    void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
    void post_recv();
    void post_send();
    void register_memory(WorkRequest *workRequest);
    void on_completion(struct ibv_wc *);
    void * poll_cq(void*);

    static CoordinatorAdmin *admin;
    static void (*failure_callback)(int);
    static void (*retry_callback)(WorkRequest*);

private:
    uint64_t getRDMABufferBlock(size_t, WorkRequestCommand);
    void clearRDMABufferBlock(connection *, uint64_t, WorkRequestCommand);
    void freeRDMABufferBlock(connection *, uint64_t, WorkRequestCommand);
    void client_failure();

    // default values
    int TIMEOUT_IN_MS = 500; /* ms */
    const int CQ_QUEUE_SIZE = 16000;

    const uint64_t DATA_BUFFER_BLOCK_SIZE = RM_LOG_BLOCK_SIZE * 10; // set it to the largest block we'll need
    const uint64_t INDEX_BUFFER_BLOCK_SIZE = 16; // for small updates (indices)
    const uint64_t NUM_DATA_BUFFER_BLOCKS = RM_LOG_SIZE*2;
    const uint64_t NUM_INDEX_BUFFER_BLOCKS = RM_LOG_SIZE;
    const uint64_t RDMA_BUFFER_SIZE = (NUM_INDEX_BUFFER_BLOCKS * INDEX_BUFFER_BLOCK_SIZE) + (NUM_DATA_BUFFER_BLOCKS * DATA_BUFFER_BLOCK_SIZE);
    // First NUM_INDEX_BUFFER_BLOCKS * INDEX_BUFFER_BLOCK_SIZE of the buffer is reserved for index writes
    const uint64_t DATA_BUFFER_OFFSET = (NUM_INDEX_BUFFER_BLOCKS * INDEX_BUFFER_BLOCK_SIZE);

    Queue<uint64_t> local_free_data_buffer_blocks;
    Queue<uint64_t> local_free_index_buffer_blocks;
    Queue<uint64_t> remote_free_data_buffer_blocks;
    Queue<uint64_t> remote_free_index_buffer_blocks;

    int count_wr = 0;
    int count_cq = 0;
    //class variables
    client *c;

    pthread_mutex_t mutex_conn_new;
    pthread_cond_t cv_conn_new;
    std::thread* event_thread;

    int connected_ = 0;
    struct context* s_ctx = NULL;
    bool ops_started = false;
    std::atomic<uint64_t> send_ticket;
    std::atomic<uint64_t> sent;
    std::atomic<uint64_t> completed;
    std::atomic<bool> poller_exit;
};
