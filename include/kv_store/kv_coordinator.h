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

#include "util/cache/simple_lru.h"
#include "util/cache/multi_lru.h"
#include "include/memory_replication/local_replication_server.h"
#include "include/kv_store/kv_index_table.h"
#include "include/kv_store/kv_request.h"
#include "include/kv_store/batched_log_entry.h"
#include "include/kv_store/log_entry.h"
#include "util/queue.h"
#include "util/priority_queue.h"
#include "util/bitmap.h"
#include "util/lock_table.h"

struct ApplyFuture {
    std::shared_ptr<RequestProcessor> rp;
    KVRequest *request;

    ApplyFuture() : rp(nullptr), request(nullptr) {}
    ApplyFuture(std::shared_ptr<RequestProcessor> rp, KVRequest *request) : rp(rp), request(request) {}
};

struct CompareApplyFuture {
    bool operator()(const ApplyFuture& af1, const ApplyFuture& af2) {
        // Flip the comparison sign to have priority queue in ascending order
        return af1.request->getLogIndex() > af2.request->getLogIndex();
    }
};

class KVCoordinator {
public:
    enum Status {BACKUP, LEADER};

    KVCoordinator(uint32_t serverID);
    ~KVCoordinator();

    void init();
    void recover();

    bool processGetRequest(KVRequest *);
    bool processPutRequest(KVRequest *);

    void waitForApplies();
    void waitForStatus(Status);

    LocalReplicationServer *replicationServer;
    std::atomic<Status> status;

private:
    void submitLogWrites();
    void logWriteFutureLoop();
    void applyFutureLoop();
    void applyLoop();

    void applyGetRequest(KVRequest *);
    void applyPutRequest(KVRequest *);

    size_t getHash(const std::string &s) {
        return str_hash(s) % KV_INDEX_SIZE;
    }
    char *getDataBlock(uint64_t);
    uint32_t getNewDataBlock();

    void readLog(int, int, std::vector<LogEntry>&);

    std::thread *logWriteFutureLoopThread;
    std::thread *applyFutureLoopThread;
    std::thread *applyLoopThreads[KV_APPLY_THREADS];

    static const size_t INDEX_ENTRY_SIZE = sizeof(uint64_t);
    static const size_t DATA_ENTRY_SIZE = (KV_KEY_SIZE + KV_VALUE_SIZE + KV_NEXT_PTR_SIZE);

    static const uint64_t COMMITTED_INDEX_SIZE = sizeof(uint32_t);
    static const uint64_t APPLIED_INDEX_SIZE = sizeof(uint32_t);
    static const uint64_t INDEX_TABLE_SIZE = INDEX_ENTRY_SIZE * KV_INDEX_SIZE;
    static const uint64_t DATA_TABLE_SIZE = DATA_ENTRY_SIZE * KV_SIZE;
    static const uint64_t BITMAP_SIZE = KV_SIZE + RM_MEMORY_BLOCK_SIZE - 1 - (KV_SIZE - 1) % RM_MEMORY_BLOCK_SIZE; // Must be multiple of 8 and RM_MEMORY_BLOCK_SIZE
    static const uint64_t WRITE_AHEAD_LOG_SIZE = KV_LOG_BLOCK_SIZE * KV_LOG_SIZE;
    // TODO: make sure write-ahead log blocks match up with the size of the replicated memory's blocks

    static const uint64_t COMMITTED_INDEX_OFFSET = 0;
    static const uint64_t APPLIED_INDEX_OFFSET = COMMITTED_INDEX_OFFSET + COMMITTED_INDEX_SIZE;
    static const uint64_t INDEX_TABLE_OFFSET = APPLIED_INDEX_OFFSET + APPLIED_INDEX_SIZE;
    static const uint64_t DATA_TABLE_OFFSET = INDEX_TABLE_OFFSET + INDEX_TABLE_SIZE;
    static const uint64_t BITMAP_OFFSET = DATA_TABLE_OFFSET + DATA_TABLE_SIZE;
    static const uint64_t WRITE_AHEAD_LOG_OFFSET = BITMAP_OFFSET + BITMAP_SIZE;

    Queue<std::pair<KVRequest*, char*>> logWriteQueue;
    Queue<BatchedLogEntry*> logWriteFutureQueue;
#if(USE_APPLIED_INDEX)
    PriorityQueue<ApplyFuture, CompareApplyFuture> applyFutureQueue;
#else
    Queue<ApplyFuture> applyFutureQueue;
#endif

    Queue<KVRequest *> applyQueue;
    std::unordered_set<std::string> applyingKeys;
    std::unordered_map<std::string, uint32_t> applyIndexMap;
    std::mutex applyingKeysMtx;
    std::mutex applyIndexMapMtx;

    LockTable<KV_SIZE / 100> lockTable;
    std::mutex pendingAppliesMtx;

    IndexTable indexTable;
    SimpleCacheFactory scf;
    LRUMultiCache *cache;
    Bitmap<BITMAP_SIZE> bitmap;
    std::atomic<uint32_t> next_committed_index;
    std::atomic<uint32_t> applied_index;

    std::hash<std::string> str_hash;

    std::atomic<uint64_t> pending_puts;
    std::atomic<uint64_t> applying_puts;

};
