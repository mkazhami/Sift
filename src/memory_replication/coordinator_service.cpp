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

#include "include/memory_replication/coordinator_service.h"

#include <assert.h>
#include <algorithm>

void onRequestCompletion(WorkRequest*, int);

CoordinatorService::CoordinatorService(int nodeId, PersistentStore* persistentStore)
        : uni_dist(0, TOTAL_MEMORY_SERVERS-1), nodeId(nodeId), persistentStore(persistentStore) {
    next_commit_index = 0;
    commit_index.store(0);
    last_applied_index.store(-1);
    terminate = false;
}

CoordinatorService::~CoordinatorService() {
    applyQueue.enqueue(nullptr);
    applyThread->join();
    if (USE_PERSISTENCE) {
        persistentLogQueue.enqueue(std::make_pair(-1, nullptr), 1);
        persistenceThread->join();
    }
}

void CoordinatorService::init(Coordinator *clients[]) {
    applyThread = new std::thread(&CoordinatorService::apply, this);
    if (USE_PERSISTENCE) {
        persistenceThread = new std::thread(&CoordinatorService::logPersistent, this);
    }
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        this->rdma_clients[i] = clients[i];
    }
}

void CoordinatorService::retryRequest(WorkRequest *workRequest) {
    if (workRequest->getRequestType() == WorkRequestType::READ) {
        LogInfo("Retrying read request for address " << workRequest->getOffset());
        submitReadRequest(workRequest->getOriginalRequest());
        delete workRequest;
    }
}

void CoordinatorService::apply() {
    while (true) {
        WorkRequest *workRequest;
        applyQueue.wait_dequeue(workRequest);

        // Termination entry
        if (workRequest == nullptr || terminate) break;

        if (workRequest->getRequestType() == WorkRequestType::WRITE) {
            applyWriteRequest(workRequest);
        } else {
            DIE("Unable to apply work request that is not a WRITE");
        }
    }
}

void CoordinatorService::logPersistent() {
#if USE_PERSISTENCE
    const uint64_t snapshotTimeThreshold = 5; // seconds
    auto lastSnapshotTime = std::chrono::high_resolution_clock::now();
    size_t numSoftEntries = 0;

    // Only have one thread manage the persistent logging
    // Simplifies logic and should not be a performance bottleneck since rocksDB writes are async
    while (true) {
        bool t = false;
        auto entries = persistentLogQueue.dequeue(RM_LOG_SIZE - numSoftEntries);
        size_t batch_size = 0;
        for (auto entry : entries) {
            uint64_t commitIndex = entry.first;
            auto requestProcessor = entry.second;
            if (commitIndex == -1 && requestProcessor == nullptr) {
                t = true;
                entries.pop_back();
                break;
            }
            batch_size += requestProcessor->getAddresses().size();
        }

        LogDebug("batch size: " << entries.size() << " queue size: " << persistentLogQueue.size());

        persistentStore->write(entries);

        // Can pop() here if fsync is used, otherwise must wait for snapshot to be created
        persistentLogQueue.pop(batch_size);

        numSoftEntries += batch_size;
        auto curTime = std::chrono::high_resolution_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(curTime - lastSnapshotTime).count();

        // Create snapshot if:
        //  1) # of soft entries is approaching RM_LOG_SIZE (threshold of 1000 is arbitrary)
        //  2) We've gone some # of seconds without creating a snapshot (threshold of 5 seconds is arbitrary)
        if (RM_LOG_SIZE - numSoftEntries < 1000 || diff > snapshotTimeThreshold) {
            persistentStore->holdWrites();
            persistentStore->createSnapshot();
            persistentStore->continueWrites();
            numSoftEntries = 0;
            lastSnapshotTime = std::chrono::high_resolution_clock::now();
        }

        if (t || terminate) break;
    }
#endif
}

bool CoordinatorService::submitReadRequest(std::shared_ptr<RequestProcessor> requestProcessor) {
    // Read requests only contain one address
    uint64_t address = requestProcessor->getAddress(0);

#if(USE_RM_CACHE)
    char *cache_value;
    if (cache.get(address, cache_value)) {
        requestProcessor->setResponse(cache_value);
        requestProcessor->sendResponse();
        return true;
    }
#endif

    // TODO: allow for variable sized reads

    WorkRequest *workRequest = new WorkRequest();
    workRequest->service = this;
    workRequest->setOriginalRequest(requestProcessor);
    workRequest->setRequestType(READ);
    workRequest->setSize(requestProcessor->getValueSize(0));
    workRequest->setCallback(onRequestCompletion);
    workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + address);
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        workRequest->setStage(i, READ);
    }

    blockLocks.readLock((address + RM_REPLICATED_MEMORY_OFFSET) / RM_MEMORY_BLOCK_SIZE);

    while (true) {
        int i;
        if (COORDINATOR_MEMORY_NODE) {
            i = LOCAL_MEMORY_NODE;
        } else {
            // Pick a random memory server to read from
            do {
                i = uni_dist(rand_num_generator);
                //i = 1;
            } while (rdma_clients[i]->state != RUNNING || upToDateVersion.load() > serverVersion[i].load());
        }

        if (rdma_clients[i]->Read(workRequest) > 0) break;
    }

    blockLocks.readUnlock((address + RM_REPLICATED_MEMORY_OFFSET) / RM_MEMORY_BLOCK_SIZE);

    return true;
}

bool CoordinatorService::submitWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor) {
    int numRequests = requestProcessor->getNumRequests();

    requestProcessor->start = std::chrono::high_resolution_clock::now();

    std::vector<WorkRequest *> workRequests(numRequests);

    // Ensure that the request will fit in the log
    assert(numRequests < KV_LOG_SIZE);

    std::unique_lock<std::mutex> bitmapLock(usedBlocksBitmapMtx);
    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        usedBlocks.setBit(address / RM_MEMORY_BLOCK_SIZE);
    }
    bitmapLock.unlock();

    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        char *value = requestProcessor->getValue(i);
        size_t value_size = requestProcessor->getValueSize(i);
        bool manage_conflict = requestProcessor->getManageConflictFlag(i);

        LogDebug("Processing write request for address " << address);

        SerializableAVPair sv;
        sv.setAddress(address);
        sv.setValue(value, value_size);
        char *sv_value = sv.serialize();

        WorkRequest *logWorkRequest = new WorkRequest();
        logWorkRequest->service = this;
        logWorkRequest->setOriginalRequest(requestProcessor);
        logWorkRequest->setValue(sv_value);
        logWorkRequest->setWriteValue(sv_value);
        logWorkRequest->setWriteSize(value_size);
        logWorkRequest->setSize(RM_LOG_BLOCK_SIZE);
        logWorkRequest->setRequestType(LOG_WRITE);
        logWorkRequest->setCallback(onRequestCompletion);
        logWorkRequest->setManageConflictFlag(manage_conflict);

        for (int j = 0; j < TOTAL_MEMORY_SERVERS; j++) {
            logWorkRequest->setStage(j, LOG_WRITE);
        }

        workRequests[i] = logWorkRequest;

#if(USE_RM_CACHE)
        // TODO: add 'toCache' flag
        char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
        memcpy(cache_value, value, value_size);
        cache.add(address, cache_value);
#endif
    }

    std::vector<uint64_t> blocks;
    for (int i = 0; i < numRequests; i++) {
        if (requestProcessor->getManageConflictFlag(i)) {
            blocks.push_back((RM_REPLICATED_MEMORY_OFFSET + requestProcessor->getAddress(i)) / RM_MEMORY_BLOCK_SIZE);
        }
    }
    std::sort(blocks.begin(), blocks.end());
    for (uint64_t block : blocks) {
        blockLocks.writeLock(block);
    }

    // Assign each operation a log index atomically
    committedIndexLock.lock();
    uint32_t min_commit_index = next_commit_index;
    next_commit_index += numRequests;
    // If the log is full of unapplied operations, wait until there is space for all of the requests
    // It's ok to wait here while holding a lock because if the log is full, no one else can progress anyways
    while (next_commit_index - last_applied_index.load() >= RM_LOG_SIZE) {
        LogDebug("RM log is full (min_commit_index:" << min_commit_index << " applied_index: " << last_applied_index.load());
    }
    committedIndexLock.unlock();

    for (int i = 0; i < numRequests; i++) {
        WorkRequest *workRequest = workRequests[i];
        workRequest->setLogIndex(min_commit_index);
        ((SerializableAVPair::AVPair*)workRequest->getValue())->log_index = min_commit_index;
        workRequest->setOffset(RM_WRITE_AHEAD_LOG_OFFSET + (workRequest->getLogIndex() % RM_LOG_SIZE) * RM_LOG_BLOCK_SIZE);
        min_commit_index++;
    }

    // TODO: Write contiguous log entries in a single RDMA operation
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        rdma_clients[i]->Write(workRequests);
    }

    return true;
}

bool CoordinatorService::submitUnloggedWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor) {
    int numRequests = requestProcessor->getNumRequests();

    requestProcessor->start = std::chrono::high_resolution_clock::now();

    // No conflict detection needed

    std::unique_lock<std::mutex> bitmapLock(usedBlocksBitmapMtx);
    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        usedBlocks.setBit(address / RM_MEMORY_BLOCK_SIZE);
    }
    bitmapLock.unlock();

    std::vector<WorkRequest *> workRequests(numRequests);
    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        char *value = requestProcessor->getValue(i);
        size_t value_size = requestProcessor->getValueSize(i);
        bool manage_conflict = requestProcessor->getManageConflictFlag(i);

        LogDebug("Processing write request for address " << address);

        WorkRequest *workRequest = new WorkRequest();
        workRequest->service = this;
        workRequest->setOriginalRequest(requestProcessor);
        workRequest->setValue(value);
        workRequest->setSize(value_size);
        workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + address);
        workRequest->setRequestType(WRITE);
        workRequest->setCallback(onRequestCompletion);
        workRequest->setManageConflictFlag(manage_conflict);

        for (int j = 0; j < TOTAL_MEMORY_SERVERS; j++) {
            workRequest->setStage(j, WRITE);
        }

        workRequests[i] = workRequest;

#if(USE_RM_CACHE)
        // TODO: add 'toCache' flag
//        char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
//        memcpy(cache_value, value, value_size);
//        cache.add(address, cache_value);
#endif
    }

    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        rdma_clients[i]->Write(workRequests);
    }

    return true;
}

void CoordinatorService::processLogWriteCompletion(WorkRequest *workRequest, int node_id, int votes) {
    if (votes == QUORUM) {
        LogDebug("Processing LogWrite completion");

        if (USE_PERSISTENCE) {
            // Write the requests asynchronously to disk
            // Can block if there are more than RM_LOG_SIZE entries in the queue already
            persistentLogQueue.enqueue(std::make_pair(workRequest->getLogIndex(), workRequest->getOriginalRequest()),
                                       workRequest->getOriginalRequest()->getAddresses().size());
        }

        // Reply to client
        workRequest->getOriginalRequest()->end = std::chrono::high_resolution_clock::now();
        workRequest->getOriginalRequest()->sendResponse();

        // Pass on request to apply queue for async application to replicated memory
        SerializableAVPair::AVPair *avPair = (SerializableAVPair::AVPair*) workRequest->getWriteValue();

        // Create copy since old work request will be deleted
        char *value = new char[sizeof(SerializableAVPair::AVPair::value)];
        memcpy(value, avPair->value, sizeof(SerializableAVPair::AVPair::value));

        WorkRequest *newRequest = new WorkRequest();
        newRequest->service = this;
        newRequest->setRequestType(WRITE);
        newRequest->setValue(value);
        newRequest->setSize(workRequest->getWriteSize());
        newRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + avPair->address);
        newRequest->setCallback(onRequestCompletion);
        newRequest->setLogIndex(workRequest->getLogIndex());
        newRequest->setManageConflictFlag(workRequest->getManageConflictFlag());
        newRequest->setOriginalRequest(workRequest->getOriginalRequest());

        applyQueue.enqueue(newRequest);
    }
}

void CoordinatorService::processReadCompletion(WorkRequest *workRequest) {
    LogDebug("Processing read completion");
    // Assuming that read request is only sent to one memory server
    char *value = workRequest->getValue();

#if(USE_RM_CACHE)
    char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
    memcpy(cache_value, value, RM_MEMORY_BLOCK_SIZE);
    cache.add(workRequest->getOffset() - RM_REPLICATED_MEMORY_OFFSET, cache_value);
#endif

    workRequest->getOriginalRequest()->setResponse(value);
    workRequest->getOriginalRequest()->sendResponse();
}

void CoordinatorService::processWriteCompletion(WorkRequest *workRequest, int node_id, int votes) {
    serverVersion[node_id].fetch_add(1);
    if (votes == QUORUM) {
        upToDateVersion.fetch_add(1);

        LogDebug("Processing write completion");

        LogDebug("Successfully applied write with value " << workRequest->getValue());

        bool logged = workRequest->getOriginalRequest()->getLogFlag();

        if (!logged) {
            // Reply to client
            if (USE_PERSISTENCE) {
                persistentLogQueue.enqueue(std::make_pair(0, workRequest->getOriginalRequest()),
                                           workRequest->getOriginalRequest()->getAddresses().size());
            }
            workRequest->getOriginalRequest()->end = std::chrono::high_resolution_clock::now();
            workRequest->getOriginalRequest()->sendResponse();
            return;
        }

        // Update applied index
        int new_applied_index = workRequest->getLogIndex();

        // If someone has already submitted a larger applied index, no need to send another
        // Does not affect correctness because of RDMA ordering guarantees
        if (last_applied_index.fetch_add(1) >= new_applied_index) {
            return;
        }

#if(USE_APPLIED_INDEX)
        WorkRequest *newRequest = new WorkRequest();
        newRequest->service = this;
        newRequest->setSize(sizeof(uint32_t));
        newRequest->setRequestType(APPLIED_INDEX_WRITE);
        newRequest->setValue(static_cast<char *>(static_cast<void *>(&new_applied_index)));
        newRequest->setOffset(RM_APPLIED_INDEX_OFFSET);
        newRequest->setCallback(onRequestCompletion);

        for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
            newRequest->setStage(i, APPLIED_INDEX_WRITE);
            rdma_clients[i]->Write(newRequest);
        }

        LogDebug("Updating last_applied_index to " << new_applied_index);
#endif
    }
}

void CoordinatorService::applyWriteRequest(WorkRequest *workRequest) {
    uint64_t offset = workRequest->getOffset();
    bool conflict_flag = workRequest->getManageConflictFlag();\

    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        workRequest->setStage(i, WRITE);
        rdma_clients[i]->Write(workRequest);
    }

    // Now that memory update has been sent, reads are safe because of RDMA ordering
    if (conflict_flag) {
        blockLocks.writeUnlock(offset / RM_MEMORY_BLOCK_SIZE);
    }
}

void onRequestCompletion(WorkRequest *workRequest, int node_id) {
    WorkRequestType rt = workRequest->getRequestType();
    bool logged = workRequest->getOriginalRequest()->getLogFlag();

    int votes = workRequest->prepare();
    if (workRequest->getWorkRequestCommand() == READ_) {
        if (!COORDINATOR_MEMORY_NODE || node_id != LOCAL_MEMORY_NODE) {
            struct connection *conn = (struct connection *) workRequest->getConnection();
            char *serialValue = conn->rdma_local_region + workRequest->getRDMABufferOffset(node_id);

            // Only let the first connection to return copy the value
            if (votes == 0) {
                workRequest->copyValue(serialValue);
            }
            // All servers must wait until the value has been copied
            while (!workRequest->ready()) {}
        }
        // Can now notify the coordinator thread that the value is ready
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == WRITE_) {
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == CAS) {
        if (!COORDINATOR_MEMORY_NODE || node_id != LOCAL_MEMORY_NODE) {
            struct connection *conn = (struct connection *) workRequest->getConnection();
            workRequest->copyValueACS(conn->rdma_local_region + workRequest->getRDMABufferOffset(node_id));
        }
        workRequest->vote(node_id);
    } else {
        DIE("Invalid work request command");
    }

    if (workRequest->service != NULL) {
        switch (rt) {
            case WorkRequestType::LOG_WRITE:
                workRequest->service->processLogWriteCompletion(workRequest, node_id, votes + 1);
                break;
            case WorkRequestType::APPLIED_INDEX_WRITE:
                // Nothing to do
                break;
            case WorkRequestType::READ:
                workRequest->service->processReadCompletion(workRequest);
                break;
            case WorkRequestType::WRITE:
                workRequest->service->processWriteCompletion(workRequest, node_id, votes + 1);
                break;
            default:
                DIE("Invalid work request type");
        }
    } else {
        DIE("CoordinatorService was not defined");
    }

    int done_count = workRequest->done();
    // Read request can always be deleted since only one request will be sent
    if (rt == WorkRequestType::READ) {
        delete workRequest;
    } else if (rt != WorkRequestType::NONE_REQUEST_TYPE) {
        // Once the last server has completed, we can delete the work request
        if (done_count == TOTAL_MEMORY_SERVERS-1) {
            if (rt == WorkRequestType::WRITE && logged) {
                delete[] workRequest->getValue();
            } else if (rt == WorkRequestType::LOG_WRITE) {
                delete[] workRequest->getWriteValue();
            }
            delete workRequest;
        }
    } else {
        LogError("Got work request with NONE type");
    }
}