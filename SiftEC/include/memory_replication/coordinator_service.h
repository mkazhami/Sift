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

#include "common/common.h"
#include "include/memory_replication/coordinator_config.h"
#include "include/network/rdma/rdma_client.h"
#include "include/memory_replication/file_writer.h"
#include "include/network/work_request.h"
#include "include/memory_replication/request_processor.h"
#include "include/memory_replication/memory_servers.h"
#include "include/util/serializable_address_value_pair.h"
#include "util/queue.h"
#include "util/weighted_bounded_queue.h"
#include "util/lru_cache.h"
#include "util/lock_table.h"
#include "util/bitmap.h"

#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <atomic>
#include <pthread.h>
#include <string>
#include <random>

class CoordinatorService {
    friend class CoordinatorAdmin;
public:
    CoordinatorService(int nodeId);

    ~CoordinatorService();

    void init(Coordinator *clients[]);
    void apply();

    bool submitReadRequest(std::shared_ptr<RequestProcessor> requestProcessor);
    bool submitWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor);
    bool submitUnloggedWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor);

    void processLogWriteCompletion(WorkRequest*, int, int);
    void processWriteCompletion(WorkRequest*, int, int);
    void processReadCompletion(WorkRequest*, int, int);

    void applyWriteRequest(WorkRequest*);

    void retryRequest(WorkRequest*);

    void stopWrites();
    void continueWrites();

    Coordinator *rdma_clients[TOTAL_MEMORY_SERVERS];
    // Used to keep track of which servers have the most up-to-date data
    std::atomic<uint64_t> serverVersion[TOTAL_MEMORY_SERVERS];
    std::atomic<uint64_t> upToDateVersion;

private:
    bool terminate = false;

    void logPersistent();

    // Random number generator for picking memory server to read from
    std::mt19937 rand_num_generator;
    std::uniform_int_distribution<> uni_dist;

    Queue<WorkRequest *> applyQueue;

    std::mutex committedIndexLock;
    uint32_t next_commit_index;
    std::atomic<uint32_t> commit_index;
    std::atomic<uint32_t> last_applied_index;
    int nodeId;

    std::thread *applyThread;
    std::thread *persistenceThread;

    // Structures used to track replicated memory state
    LockTable<(RM_REPLICATED_MEMORY_SIZE / RM_MEMORY_BLOCK_SIZE)/1> blockLocks;
    LRUCache<uint64_t, char, KV_CACHE_SIZE> cache;
    Bitmap<RM_REPLICATED_MEMORY_SIZE/RM_MEMORY_BLOCK_SIZE> usedBlocks;
    std::mutex usedBlocksBitmapMtx;

    uint64_t sent_msgs;
    std::atomic<uint64_t> completed_msgs;
};
