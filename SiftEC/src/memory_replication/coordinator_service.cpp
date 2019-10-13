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
#include "include/memory_replication/erasure_coding.h"

#include <assert.h>
#include <algorithm>
#include <unordered_set>

void onRequestCompletion(WorkRequest*, int);

CoordinatorService::CoordinatorService(int nodeId)
        : uni_dist(0, TOTAL_MEMORY_SERVERS-1), nodeId(nodeId) {
    next_commit_index = 0;
    commit_index.store(0);
    last_applied_index.store(-1);
    terminate = false;
    ErasureCoding::erasure_init();
}

CoordinatorService::~CoordinatorService() {
    applyQueue.enqueue(nullptr);
    applyThread->join();
    std::shared_ptr<RequestProcessor> ptr;
}

void CoordinatorService::init(Coordinator *clients[]) {
    applyThread = new std::thread(&CoordinatorService::apply, this);
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

    // TODO: extend reads to allow for variable sized reads

    WorkRequest *workRequest = new WorkRequest();
    workRequest->service = this;
    workRequest->setOriginalRequest(requestProcessor);
    workRequest->setRequestType(READ);
    workRequest->setSize(CHUNK_SIZE);
    workRequest->setCallback(onRequestCompletion);
    workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + ErasureCoding::erasure_translate_address(address));
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        workRequest->setStage(i, READ);
    }

    //LogInfo("Reading chunks from address " << RM_REPLICATED_MEMORY_OFFSET + erasure_translate_address(address));

    assert(address % RM_MEMORY_BLOCK_SIZE == 0);

    blockLocks.readLock((ErasureCoding::erasure_translate_address(address) + RM_REPLICATED_MEMORY_OFFSET) / RM_MEMORY_BLOCK_SIZE);

    std::unordered_set<int> nodes_to_read;
    int i = 0;
    const uint64_t lastVersion = upToDateVersion;
    while (true) {
        if (rdma_clients[i]->state == RUNNING) { //&& serverVersion[i] >= lastVersion) {
            nodes_to_read.insert(i);
        }

        i = (i+1) % TOTAL_MEMORY_SERVERS;

        if (nodes_to_read.size() == NUM_ORIGINAL_CHUNKS) {
            break;
        }
    }

    for (const int &node : nodes_to_read) {
        if (rdma_clients[node]->Read(workRequest) <= 0) {
            DIE("Could not submit read to node " << node);
        }
    }

    blockLocks.readUnlock((ErasureCoding::erasure_translate_address(address) + RM_REPLICATED_MEMORY_OFFSET) / RM_MEMORY_BLOCK_SIZE);

    return true;
}

bool CoordinatorService::submitWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor) {
    int numRequests = requestProcessor->getNumRequests();

    std::vector<WorkRequest *> workRequests(numRequests);

    // Ensure that the request will fit in the log
    assert(numRequests < KV_LOG_SIZE);

    // Lock each of the blocks
    std::vector<uint64_t> blocks;
    for (int i = 0; i < numRequests; i++) {
        if (requestProcessor->getManageConflictFlag(i)) {
            blocks.push_back((RM_REPLICATED_MEMORY_OFFSET + ErasureCoding::erasure_translate_address(requestProcessor->getAddress(i))) / RM_MEMORY_BLOCK_SIZE);
        }
    }
    std::sort(blocks.begin(), blocks.end());
    // Remove duplicates to avoid deadlock
    blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
    for (uint64_t block : blocks) {
        blockLocks.writeLock(block);
    }

    std::vector<SerializableAVPair> avPairs(numRequests);
    // Assign each operation a log index atomically
    committedIndexLock.lock();
    uint32_t minCommitIndex = next_commit_index;
    uint32_t maxCommitIndex = minCommitIndex + numRequests;
    for (int i = 0; i < numRequests; i++) {
        avPairs[i].setLogIndex(next_commit_index);
        next_commit_index++;
    }
    committedIndexLock.unlock();

    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        char *value = requestProcessor->getValue(i);
        size_t value_size = requestProcessor->getValueSize(i);
        bool manage_conflict = requestProcessor->getManageConflictFlag(i);

        LogDebug("Processing write request for address " << address);

        assert(address % RM_MEMORY_BLOCK_SIZE == 0);

        //SerializableAVPair &sv = avPairs[i];
        SerializableAVPair sv;
        sv.setAddress(address);
        sv.setValue(value, value_size);
        char *sv_value = sv.serialize();

        WorkRequest *logWorkRequest = new WorkRequest();
        logWorkRequest->service = this;
        logWorkRequest->setOriginalRequest(requestProcessor);
        logWorkRequest->setWriteValue(sv_value);
        logWorkRequest->setWriteSize(value_size);
        logWorkRequest->setSize(RM_LOG_BLOCK_SIZE);
        logWorkRequest->setOffset(RM_WRITE_AHEAD_LOG_OFFSET + (sv.getLogIndex() % RM_LOG_SIZE) * RM_LOG_BLOCK_SIZE);
        logWorkRequest->setLogIndex(sv.getLogIndex());
        logWorkRequest->setRequestType(LOG_WRITE);
        logWorkRequest->setCallback(onRequestCompletion);
        logWorkRequest->setManageConflictFlag(manage_conflict);

        for (int j = 0; j < TOTAL_MEMORY_SERVERS; j++) {
            logWorkRequest->setValue(j, sv_value);
            logWorkRequest->setStage(j, LOG_WRITE);
        }

        workRequests[i] = logWorkRequest;

#if(USE_RM_CACHE)
        // Technically not safe to cache value here with no other checks, but we assume that a failure will also cause
        // the client (KVS, in this case) to crash, and that they know how to recover
        // Assume that full block write -> important enough to cache
        // TODO: add 'toCache' flag
        char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
        memcpy(cache_value, value, value_size);
        cache.add(address, cache_value);
#endif
    }

    // TODO: Write contiguous log entries in a single RDMA operation
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        rdma_clients[i]->Write(workRequests);
    }

    return true;
}

bool CoordinatorService::submitUnloggedWriteRequest(std::shared_ptr<RequestProcessor> requestProcessor) {
    int numRequests = requestProcessor->getNumRequests();

    // No conflict detection needed

    std::vector<WorkRequest *> workRequests(numRequests);

    // Ensure that the request will fit in the log
    assert(numRequests < KV_LOG_SIZE);

    for (int i = 0; i < numRequests; i++) {
        uint64_t address = requestProcessor->getAddress(i);
        char *value = requestProcessor->getValue(i);
        size_t value_size = requestProcessor->getValueSize(i);
        bool manage_conflict = requestProcessor->getManageConflictFlag(i);

        LogDebug("Processing write request for address " << address);

        CM256::cm256_block *chunks = ErasureCoding::generate_chunks(value);

        //TODO: allow for variable sized chunks - needed for KV logging

        WorkRequest *workRequest = new WorkRequest();
        workRequest->service = this;
        workRequest->setOriginalRequest(requestProcessor);
        workRequest->setSize(CHUNK_SIZE);
        workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + ErasureCoding::erasure_translate_address(address));
        workRequest->setRequestType(WRITE);
        workRequest->setCallback(onRequestCompletion);
        workRequest->setManageConflictFlag(manage_conflict);

        //LogInfo("Writing chunks to address " << RM_REPLICATED_MEMORY_OFFSET + erasure_translate_address(address));

        for (int j = 0; j < TOTAL_MEMORY_SERVERS; j++) {
            workRequest->setValue(j, (char*)chunks[j].Block);
            workRequest->setStage(j, WRITE);
        }

        workRequests[i] = workRequest;

        delete[] chunks;

#if(USE_RM_CACHE)
        // TODO: add 'toCache' flag
        char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
        memcpy(cache_value, value, value_size);
        cache.add(address, cache_value);
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

        // Reply to client
        workRequest->getOriginalRequest()->sendResponse();

        // Pass on request to apply queue for async application to replicated memory
        SerializableAVPair::AVPair *avPair = (SerializableAVPair::AVPair*) workRequest->getWriteValue();

        // Create copy since old work request will be deleted
        char *value = new char[sizeof(SerializableAVPair::AVPair::value)];
        memcpy(value, avPair->value, sizeof(SerializableAVPair::AVPair::value));

        // TODO: eliminate extra copy of 'value'
        CM256::cm256_block *chunks = ErasureCoding::generate_chunks(value);
        delete[] value;

        assert(workRequest->getWriteSize() == RM_MEMORY_BLOCK_SIZE);

        WorkRequest *newRequest = new WorkRequest();
        newRequest->service = this;
        newRequest->setRequestType(WRITE);
        newRequest->setSize(CHUNK_SIZE);
        newRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + ErasureCoding::erasure_translate_address(avPair->address));
        newRequest->setCallback(onRequestCompletion);
        newRequest->setLogIndex(workRequest->getLogIndex());
        newRequest->setManageConflictFlag(workRequest->getManageConflictFlag());
        newRequest->setOriginalRequest(workRequest->getOriginalRequest());

        //LogInfo("Writing chunks to address " << RM_REPLICATED_MEMORY_OFFSET + erasure_translate_address(avPair->address));

        for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
            newRequest->setValue(i, (char*)chunks[i].Block);
        }

        delete[] chunks;

        applyQueue.enqueue(newRequest);
    }
}

void CoordinatorService::processReadCompletion(WorkRequest *workRequest, int node_id, int votes) {
    //LogInfo("Processing read completion, votes: " << votes << " node_id: " << node_id);

    if (votes == QUORUM) {
        // Wait until all chunks have been copied
        while (!workRequest->ready()) {}

        // Assuming that read request is only sent to one memory server
        char **value = workRequest->getValue();

        CM256::cm256_block *chunks = new CM256::cm256_block[NUM_ORIGINAL_CHUNKS];
        unsigned char count = 0;
        for (unsigned char i = 0; i < NUM_ORIGINAL_CHUNKS; i++) {
            if (value[i] != nullptr) {
                chunks[i].Block = value[i];
                chunks[i].Index = i;
                count++;
            } else {
                chunks[i].Block = nullptr;
            }
        }

        bool needRebuild = count != NUM_ORIGINAL_CHUNKS;
        if (needRebuild) {
            // Replace missing original chunks
            for (unsigned char i = NUM_ORIGINAL_CHUNKS; i < TOTAL_MEMORY_SERVERS; i++) {
                for (unsigned char j = 0; j < NUM_ORIGINAL_CHUNKS; j++) {
                    if (value[i] != nullptr && chunks[j].Block == nullptr) {
                        chunks[j].Block = value[i];
                        chunks[j].Index = i;
                        count++;
                    }
                }
            }
        }

        LogAssert(count == NUM_ORIGINAL_CHUNKS, "Only received " << (unsigned int)count << "/" << NUM_ORIGINAL_CHUNKS << " read chunks");

        char *block_value = ErasureCoding::rebuild_block(chunks, needRebuild);
//        if (block_value[0] == 0) {
//            LogError("Read return value appears to be empty");
//        }

        delete[] chunks;

#if(USE_RM_CACHE)
        char *cache_value = new char[RM_MEMORY_BLOCK_SIZE];
        memcpy(cache_value, block_value, RM_MEMORY_BLOCK_SIZE);
        cache.add(workRequest->getOffset()*NUM_ORIGINAL_CHUNKS - RM_REPLICATED_MEMORY_OFFSET, cache_value);
#endif

        workRequest->getOriginalRequest()->setResponse(block_value);
        workRequest->getOriginalRequest()->sendResponse();


    }
}

void CoordinatorService::processWriteCompletion(WorkRequest *workRequest, int node_id, int votes) {
    serverVersion[node_id].fetch_add(1);
    if (votes == QUORUM) {
        upToDateVersion.fetch_add(1);
        completed_msgs.fetch_add(1);

        LogDebug("Processing write completion");

        //LogDebug("Successfully applied write with value " << workRequest->getValue());

        bool logged = workRequest->getOriginalRequest()->getLogFlag();

        if (!logged) {
            // Reply to client
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
    uint64_t size = workRequest->getSize();
    bool conflict_flag = workRequest->getManageConflictFlag();

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
        struct connection *conn = (struct connection *) workRequest->getConnection(node_id);
        char *serialValue = conn->rdma_local_region + workRequest->getRDMABufferOffset(node_id);
        workRequest->copyValue(node_id, serialValue);
        // Can now notify the coordinator thread that the value is ready
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == WRITE_) {
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == CAS) {
        if (!COORDINATOR_MEMORY_NODE || node_id != LOCAL_MEMORY_NODE) {
            struct connection *conn = (struct connection *) workRequest->getConnection(node_id);
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
                workRequest->service->processReadCompletion(workRequest, node_id, votes + 1);
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
    if (rt == WorkRequestType::READ && done_count == NUM_ORIGINAL_CHUNKS-1) {
        delete workRequest;
    } else if (rt != WorkRequestType::NONE_REQUEST_TYPE) {
        // Once the last server has completed, we can delete the work request
        if (done_count == TOTAL_MEMORY_SERVERS-1) {
            if (rt == WorkRequestType::WRITE && logged) {
                delete[] workRequest->getValue()[0];
            } else if (rt == WorkRequestType::LOG_WRITE) {
                delete[] workRequest->getWriteValue();
            }
            delete workRequest;
        }
    } else {
        LogError("Got work request with NONE type");
    }
}