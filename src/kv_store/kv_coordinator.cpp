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

#include "include/kv_store/kv_coordinator.h"
#include "include/kv_store/data_entry.h"
#include "include/kv_store/batched_log_entry.h"
#include "include/kv_store/kv_request.h"
#include "../../include/kv_store/kv_coordinator.h"
#include "include/network/rdma/remote_rdma_client.h"

#include <algorithm>
#include <assert.h>

KVCoordinator::KVCoordinator(uint32_t serverID) {
    replicationServer = new LocalReplicationServer(serverID);
    replicationServer->Run();

    cache = new LRUMultiCache(100, 10, KV_CACHE_SIZE, 1, scf);

    status = BACKUP;

    next_committed_index = 0;
    applied_index = 0;

    pending_puts = 0;
    applying_puts = 0;

    // Ensure these conditions are met by the configuration
    assert(KV_KEY_SIZE + KV_VALUE_SIZE + KV_NEXT_PTR_SIZE <= RM_MEMORY_BLOCK_SIZE);
    assert(sizeof(LogEntry::entry) == KV_LOG_BLOCK_SIZE);
    assert(BITMAP_SIZE % RM_MEMORY_BLOCK_SIZE == 0);
    assert(DATA_TABLE_SIZE % RM_MEMORY_BLOCK_SIZE == 0);
    assert(sizeof(DataEntry::entry) == DATA_ENTRY_SIZE);
    assert(DATA_ENTRY_SIZE == RM_MEMORY_BLOCK_SIZE);
}

KVCoordinator::~KVCoordinator() {
    logWriteFutureLoopThread->join();

    for (int i = 0; i < KV_APPLY_THREADS; i++) {
        applyQueue.enqueue(nullptr);
    }
    for (int i = 0; i < KV_APPLY_THREADS; i++) {
        applyLoopThreads[i]->join();
    }

    ApplyFuture af;
    applyFutureQueue.enqueue(af);
    applyFutureLoopThread->join();

    delete replicationServer;
    delete cache;
}

void KVCoordinator::init() {
    std::thread(&KVCoordinator::submitLogWrites, this).detach();

    logWriteFutureLoopThread = new std::thread(&KVCoordinator::logWriteFutureLoop, this);
    std::thread(&KVCoordinator::logWriteFutureLoop, this).detach();
    applyFutureLoopThread = new std::thread(&KVCoordinator::applyFutureLoop, this);
    std::thread(&KVCoordinator::applyFutureLoop, this).detach();
    for (int i = 0; i < KV_APPLY_THREADS; i++) {
        applyLoopThreads[i] = new std::thread(&KVCoordinator::applyLoop, this);
    }
}

void KVCoordinator::waitForApplies() {
    while (!applyQueue.isEmpty()) {std::this_thread::sleep_for(std::chrono::milliseconds(50));}
    while (next_committed_index != applied_index) {std::this_thread::sleep_for(std::chrono::milliseconds(50));}
}

void KVCoordinator::waitForStatus(Status newStatus) {
    while (status != newStatus) {std::this_thread::sleep_for(std::chrono::milliseconds(50));}
}

std::chrono::time_point<std::chrono::high_resolution_clock> latency_timer;


void KVCoordinator::submitLogWrites() {
    while (true) {
        auto queueSize = std::min((long unsigned int)10, std::min(logWriteQueue.size(), KV_LOG_SIZE - (next_committed_index % KV_LOG_SIZE)));

        if (queueSize == 0) {
            logWriteQueue.waitNonEmpty();
            continue;
        }

        auto ble= new BatchedLogEntry();

        char *buf = new char[sizeof(LogEntry::entry) * queueSize];

        uint32_t cindex = next_committed_index.fetch_add(queueSize);

        while (cindex + queueSize > applied_index && cindex + queueSize - applied_index >= KV_LOG_SIZE) {
            LogDebug("Waiting for KV log to shrink");
        }

        for (int i = 0; i < queueSize; i++) {
            std::pair<KVRequest*, char*> r;
            logWriteQueue.wait_dequeue(r);
            KVRequest *request = r.first;
            char *requestLogBuf = r.second;

            uint32_t logIndex = cindex + i;
            request->setLogIndex(logIndex);
            // Set serialized log entry's log index
            memcpy(requestLogBuf, &logIndex, sizeof(uint32_t));

            std::unique_lock<std::mutex> applyIndexMapLock(applyIndexMapMtx);
            applyIndexMap[request->getKey()] = cindex + i;
            applyIndexMapLock.unlock();

            ble->addRequest(request);

            memcpy(buf + i*sizeof(LogEntry::entry), requestLogBuf, sizeof(LogEntry::entry));
            delete[] requestLogBuf;
        }

        std::shared_ptr<RequestProcessor> rp = replicationServer->writeRequest(WRITE_AHEAD_LOG_OFFSET + (cindex % KV_LOG_SIZE) * sizeof(LogEntry::entry), buf, sizeof(LogEntry::entry) * queueSize, false, false);
        ble->setRequestProcessor(rp);
        logWriteFutureQueue.enqueue(ble);

        LogDebug("Submitted batch of size " << queueSize);
    }
}

void KVCoordinator::logWriteFutureLoop() {
    while (true) {
        BatchedLogEntry *ble;
        logWriteFutureQueue.wait_dequeue(ble);


        int batchSize = ble->getSize();
        std::vector<KVRequest*> requests(batchSize);
        for (int i = 0; i < batchSize; i++) {
            KVRequest *request = ble->getRequest(i);
            auto key = std::move(request->getKey());
            auto value = std::move(request->getValue());

            KVRequest *kvRequest = new KVRequest(KVRequestType::PUT);
            kvRequest->setKey(key);
            kvRequest->setValue(value);
            kvRequest->setLogIndex(request->getLogIndex());

            requests[i] = kvRequest;
        }

        ble->getRequestProcessor()->wait();

        for (int i = 0; i < batchSize; i++) {
            auto key = requests[i]->getKey();
            applyQueue.enqueue(requests[i]);
            ble->getRequest(i)->completePutRequest();
            lockTable.writeUnlock(getHash(key));
        }

        delete[] ble->getRequestProcessor()->getValue(0);
        delete ble;
    }
}

void KVCoordinator::applyLoop() {
    while (true) {
        KVRequest *request;

        // Take the first request from the queue. If it can't be processed, put it back to the end
        // of the queue and take the next one.
        while (true) {
            applyQueue.wait_dequeue(request);

            if (request == nullptr) return;

            // Request is allowed to be processed if:
            //   a) No other request with this key is being processed; and
            //   b) Other queued requests with this key all have higher log indices
            std::unique_lock<std::mutex> applyIndexMapLock(applyIndexMapMtx);
            auto applyIndexMapIter = applyIndexMap.find(request->getKey());
            if (applyIndexMapIter != applyIndexMap.end() && request->getLogIndex() != applyIndexMapIter->second) {
                // Don't add request back to queue, search for new one
                applied_index.fetch_add(1);
                pending_puts.fetch_sub(1);
                continue;
            }

            std::unique_lock<std::mutex> applyingKeysLock(applyingKeysMtx);
            if (applyingKeys.find(request->getKey()) != applyingKeys.end()) {
                applyIndexMapLock.unlock();
                applyingKeysLock.unlock();
                applyQueue.enqueue(request);
                continue;
            }

            applyIndexMapLock.unlock();
            applyingKeysLock.unlock();

            break;
        }

        applyPutRequest(request);
    }
}

void KVCoordinator::applyFutureLoop() {
    while (true) {
        ApplyFuture applyFuture;

#if(USE_APPLIED_INDEX)
        // Spin on priority queue until the next applied index comes up
        uint32_t next_applied_index = applied_index.load();
        do {
            applyFutureQueue.wait_peek(applyFuture);
        } while (applyFuture.request->getLogIndex() != next_applied_index);

        applyFutureQueue.wait_dequeue(applyFuture);
        assert(applyFuture.request->getLogIndex() == next_applied_index);

        std::vector<ApplyFuture> consecutiveApplies = {applyFuture};
        while (true) {
            if (applyFutureQueue.isEmpty()) break;

            ApplyFuture af = applyFutureQueue.peek();
            if (af.request->getLogIndex() != next_applied_index + 1) {
                break;
            }
            consecutiveApplies.push_back(af);
            applyFutureQueue.pop();
            next_applied_index++;
        }

        for (ApplyFuture &future : consecutiveApplies) {
            std::shared_ptr<RequestProcessor> rp = future.rp;
            KVRequest *request = future.request;
            std::string key = request->getKey();

            // Wait for memory writes to complete
            rp->wait();

            // Remove do-not-evict flag from cache entry
            cache.clearDoNotEvictFlag(key);

            // Remove this request's index from the global set to allow other queued requests on this key to progress
            std::unique_lock<std::mutex> pendingAppliesLock(pendingAppliesMtx);
            std::vector<uint32_t> &logIndexVec = applyQueueLogIndices[key];
            logIndexVec.erase(std::find(logIndexVec.begin(), logIndexVec.end(), request->getLogIndex()));
            if (logIndexVec.size() == 0) {
                applyQueueLogIndices.erase(key);
            }
            pendingAppliesLock.unlock();

            for (int i = 0; i < rp->getNumRequests(); i++) {
                delete[] rp->getValue(i);
            }

            delete request;
        }

        // Send applied index update
        uint32_t new_applied_index = next_applied_index;
        std::shared_ptr<RequestProcessor> index_rp =
                replicationServer->writeRequest(APPLIED_INDEX_OFFSET,
                                                (char *) &new_applied_index,
                                                APPLIED_INDEX_SIZE,
                                                false);

        if (new_applied_index - applied_index.load() > 500) {
            LogInfo("Updating applied index by " << new_applied_index - applied_index.load());
        }

        applied_index.store(new_applied_index + 1);

        index_rp->wait();
#else
        const int max_lookahead = 10;
        int position = 0;
        bool found = false;

        // If the queue is empty or none of the first few are ready, dequeue and wait on the first one
        if (!found) {
            applyFutureQueue.wait_dequeue(applyFuture);
        }

        if (applyFuture.request == nullptr && applyFuture.rp == nullptr) break;

        std::shared_ptr<RequestProcessor> rp = applyFuture.rp;
        KVRequest *request = applyFuture.request;
        std::string key = request->getKey();

        // Wait for memory writes to complete
        rp->wait();

        // Remove do-not-evict flag from cache entry
        cache->setClean(key);

        // Remove this request's index from the global set to allow other queued requests on this key to progress
        std::unique_lock<std::mutex> applyingKeysLock(applyingKeysMtx);
        applyingKeys.erase(key);
        applyingKeysLock.unlock();


        applied_index.fetch_add(1);

        pending_puts.fetch_sub(1);
        applying_puts.fetch_sub(1);

        delete request;
#endif
    }
}

bool KVCoordinator::processGetRequest(KVRequest *request) {
    std::string key = request->getKey();
    LogDebug("Processing get request for key " << key);

    // Wait until any pending writes to this key have been processed (put in the cache)
    size_t keyHash = getHash(key);
    lockTable.readLock(keyHash);

    std::string value;
    cache->get(key, &value);
    if (value.length() == 0) {
        // Value was not in the cache, so must get value from memory
        applyGetRequest(request);
    } else {
        request->completeGetRequest(value);
    }

    lockTable.readUnlock(keyHash);
    return true;
}

bool KVCoordinator::processPutRequest(KVRequest *request) {
    std::string key = request->getKey();
    LogDebug("Processing put request for key " << key);

    LogEntry logEntry;
    logEntry.setKey(key.c_str());
    logEntry.setValue(request->getValue().c_str());
    char *log_entry = logEntry.serialize();

    // Wait until any other pending writes to this key have been logged
    // Only one write to the same key can be logged at a time to preserve cache consistency with the log order
    lockTable.writeLock(getHash(key));

    pending_puts.fetch_add(1);

    // Apply update to cache
    if (cache->insert(key, request->getValue(), true) == -1) {
        DIE("Failed to update cache");
    }

    logWriteQueue.enqueue(std::make_pair(request, log_entry));

    return true;
}

char *KVCoordinator::getDataBlock(uint64_t data_ptr) {
    std::shared_ptr<RequestProcessor> rp = replicationServer->readRequest(data_ptr);
    rp->wait();
    char *ret = rp->getReturnValue();
    return ret;
}

void KVCoordinator::applyGetRequest(KVRequest *request) {
    LogDebug("Applying get request for key " << request->getKey());

    std::string key = request->getKey();

    // Don't need to check for unapplied requests with the same key, because their values would have been in the cache
    // The caller of this function doesn't allow any new PUT requests until we are done

    size_t keyHash = getHash(key);
    indexTable.readLock(keyHash);
    uint64_t dataPtr = indexTable.getIndex(keyHash);

    // Key's hash does not exist in index table -> key does not exist
    if (dataPtr == 0) {
        DIE("Key from GET request (" << key << ") does not exist");
    }

    LogDebug("Starting search through chain (for key " << key << ")");

    // Search through the hash chain for the key we're looking for
    //int chainLength = 0;
    while (true) {
        //chainLength++;
        DataEntry dataEntry;
        char *data_block = getDataBlock(dataPtr);
        dataEntry.deserialize(data_block);

        std::string read_key(dataEntry.getKey());
        LogDebug("At block with key " << key);

        if (read_key == key) {
            LogDebug("Found block!");
            std::string value(dataEntry.getValue());
            request->completeGetRequest(value);
            // Try to add to cache - may not succeed if cache is full
            cache->insert(key, value, false);
            delete[] data_block;
            break;
        } else {
            dataPtr = dataEntry.getNextPtr();
            // Reached end of hash chain without finding key -> key does not exist
            if (dataPtr == 0) {
                LogError("Reached end of hash chain without finding key " << request->getKey());
            }
        }
        delete[] data_block;
    }

    //LogInfo("Chain length: " << chainLength);

    indexTable.readUnlock(keyHash);
}

uint32_t KVCoordinator::getNewDataBlock() {
    // Assume bitmap is locked
    uint32_t bit = bitmap.setNextAvailableBit();
    if (bit == -1) {
        DIE("No more data blocks available");
    }
    return bit;
}

void KVCoordinator::applyPutRequest(KVRequest *request) {
    LogDebug("Applying put request for key " << request->getKey());

    applying_puts.fetch_add(1);

    std::string key = request->getKey();
    assert(key.length() < KV_KEY_SIZE);

    size_t keyHash = getHash(key);
    // Lock index table to prevent apply threads from overwriting a new hash's pointer
    indexTable.writeLock(keyHash);
    uint64_t dataPtr = indexTable.getIndex(keyHash);

    LogDebug("Key's hash is: " << keyHash << " data pointer is: " << dataPtr);

    // Key's hash doesn't exist in the table yet
    if (dataPtr == 0) {
        // Get new data block location from bitmap
        bitmap.lock();
        uint32_t availableBlock = getNewDataBlock();
        // Update index table to point to new first block in chain
        indexTable.setIndex(keyHash, DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE);

        // Create new data block
        DataEntry dataEntry;
        dataEntry.setKey(key.c_str());
        dataEntry.setValue(request->getValue().c_str());
        dataEntry.setNextPtr(0);
        char *newDataBlock = dataEntry.serialize();

        // Create new index table entry
        uint64_t *newIndexDataPtr = new uint64_t;
        *newIndexDataPtr = DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE;

        // Create bitmap update
        // Get 4 byte block that contains our updated bit
        const uint32_t bitmap_block_size = 4;
        size_t bitmapBlock = availableBlock - (availableBlock % (bitmap_block_size * 8));
        uint8_t *newBitmapBlock = bitmap.toByteArray(bitmapBlock, bitmap_block_size);

        // Get memory locations of each block to be written
        uint64_t newIndexBlockLoc = INDEX_TABLE_OFFSET + keyHash * INDEX_ENTRY_SIZE;
        uint64_t newDataBlockLoc = DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE;
        uint64_t newBitmapBlockLoc = BITMAP_OFFSET + bitmapBlock / 8;

        std::vector<uint64_t> addresses = {newIndexBlockLoc, newDataBlockLoc, newBitmapBlockLoc};
        std::vector<char *> values = {(char*)newIndexDataPtr, newDataBlock, (char*)newBitmapBlock};
        std::vector<size_t> valueLengths = {INDEX_ENTRY_SIZE, DATA_ENTRY_SIZE, bitmap_block_size};
        std::vector<bool> manage_conflicts = {false, true, false};

        std::shared_ptr<RequestProcessor> rp = replicationServer->writeRequest(addresses, values, valueLengths, manage_conflicts, true);

#if(USE_APPLIED_INDEX)
        applyFutureQueue.emplace(rp, request);
#else
        ApplyFuture af(rp, request);
        applyFutureQueue.enqueue(af);
#endif

        // Can now safely unlock shared data structures
        bitmap.unlock();
        indexTable.writeUnlock(keyHash);

        delete[] newDataBlock;
        delete[] newBitmapBlock;
        delete newIndexDataPtr;
    } else {
        // Key's hash exists, now try to find the key in the chain
        DataEntry dataEntry;
        bool found = false;

#if(CACHE_KEY_ADDRESS)
        // First check the cache for the exact address (to avoid lookup)
        uint64_t address = cache.getAddress(key);
#else
        uint64_t address = 0;
#endif
        if (address != 0) {
            dataPtr = address;
            found = true;
        } else {
            char *dataEntryBlock;
            LogDebug("Starting search through chain for key " << key);
            // Search through the hash chain for the key we're looking for
            while (true) {
                dataEntryBlock = getDataBlock(dataPtr);
                dataEntry.deserialize(dataEntryBlock);

                LogDebug("At block with key " << dataEntry.getKey());

                if (strcmp(dataEntry.getKey(), key.c_str()) == 0) {
                    LogDebug("Key found!");
                    found = true;
#if(CACHE_KEY_ADDRESS)
                    // Try to add address to cache
                    cache.setAddress(key, dataPtr);
#endif
                    break;
                }

                uint64_t tempDataPtr = dataEntry.getNextPtr();
                // Reached end of hash chain without finding key -> key does not exist
                if (tempDataPtr == 0) {
                    LogDebug("Reached end of chain without finding key");
                    break;
                }
                dataPtr = tempDataPtr;
                delete[] dataEntryBlock;
            }
            delete[] dataEntryBlock;
        }

        // Key was found at dataPtr, so only need to update the value at that block
        if (found) {
            dataEntry.setKey(key.c_str());
            dataEntry.setValue(request->getValue().c_str());
            LogDebug("Writing value " << request->getValue().c_str() << " to key " << key);
            char *newDataEntryBlock = dataEntry.serialize();

            std::shared_ptr<RequestProcessor> rp = replicationServer->writeRequest(dataPtr, newDataEntryBlock, DATA_ENTRY_SIZE, true, true);

#if(USE_APPLIED_INDEX)
            applyFutureQueue.emplace(rp, request);
#else
            ApplyFuture af(rp, request);
            applyFutureQueue.enqueue(af);
#endif

            // Can now safely unlock shared data structures
            indexTable.writeUnlock(keyHash);

            delete[] newDataEntryBlock;
        } else {
            // Key was not found, so must create a new block, update all the metadata as well as the previous block

            // Get new data block location
            bitmap.lock();
            uint32_t availableBlock = getNewDataBlock();

            // Create new data block
            DataEntry newDataEntry;
            newDataEntry.setKey(key.c_str());
            newDataEntry.setValue(request->getValue().c_str());
            newDataEntry.setNextPtr(0);
            char *newDataBlock = newDataEntry.serialize();

            // Create previous entry update (block data and ptr are currently stored from chain search)
            LogDebug("Updating previous block in chain (with key " << dataEntry.getKey() << " and value " << dataEntry.getValue() << ")");
            dataEntry.setNextPtr(DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE);
            char *updatedDataBlock = dataEntry.serialize();

            // Get 4 byte block that contains our updated bit
            const uint32_t bitmap_block_size = 4;
            size_t bitmapBlock = availableBlock - (availableBlock % (bitmap_block_size * 8));
            uint8_t *newBitmapBlock = bitmap.toByteArray(bitmapBlock, bitmap_block_size);

            // Get memory locations of each block to be written
            uint64_t newDataBlockLoc = DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE;
            uint64_t updatedDataBlockLoc = dataPtr;
            uint64_t newBitmapBlockLoc = BITMAP_OFFSET + bitmapBlock / 8;

            std::vector<uint64_t> addresses = {newDataBlockLoc, updatedDataBlockLoc, newBitmapBlockLoc};
            std::vector<char *> values = {newDataBlock, updatedDataBlock, (char*)newBitmapBlock};
            std::vector<size_t> valueLengths = {DATA_ENTRY_SIZE, DATA_ENTRY_SIZE, bitmap_block_size};
            std::vector<bool> manage_conflicts = {true, true, false};

            std::shared_ptr<RequestProcessor> rp = replicationServer->writeRequest(addresses, values, valueLengths, manage_conflicts, true);

#if(USE_APPLIED_INDEX)
            applyFutureQueue.emplace(rp, request);
#else
            ApplyFuture af(rp, request);
            applyFutureQueue.enqueue(af);
#endif

            // Can now safely unlock shared data structures
            indexTable.writeUnlock(keyHash);
            bitmap.unlock();

            delete[] newDataBlock;
            delete[] updatedDataBlock;
            delete[] newBitmapBlock;

            LogDebug("Created new data block, added it to chain (updated prev block's (with key " << dataEntry.getKey() << ") pointer to " << DATA_TABLE_OFFSET + availableBlock * DATA_ENTRY_SIZE);
        }
    }
    LogDebug("Done applying put to key " << key);
}

void KVCoordinator::readLog(int start, int end, std::vector<LogEntry> &logEntries) {
    for (int i = start; i < end; i++) {
        auto rp = replicationServer->readRequest(WRITE_AHEAD_LOG_OFFSET + i * KV_LOG_BLOCK_SIZE, KV_LOG_BLOCK_SIZE);
        rp->wait();
        LogEntry logEntry;
        logEntry.deserialize(rp->getReturnValue());
        logEntries[i] = std::move(logEntry);
        delete[] rp->getReturnValue();
    }
}

void KVCoordinator::recover() {
    auto t1 = std::chrono::high_resolution_clock::now();

    // Read in index table
    char *indexBuf = new char[INDEX_TABLE_SIZE];
    uint64_t blockSize = RemoteCoordinator::DATA_BUFFER_BLOCK_SIZE;
    int numBlocks = INDEX_TABLE_SIZE / blockSize;
    std::vector<std::shared_ptr<RequestProcessor>> readRequests;
    int bytesRead = 0;
    int i = 0;
    // Submit async reads
    while (bytesRead < INDEX_TABLE_SIZE) {
        int size = std::min(blockSize, INDEX_TABLE_SIZE - bytesRead);
        bytesRead += size;

        auto rp = replicationServer->readRequest(INDEX_TABLE_OFFSET + i*blockSize, size);
        readRequests.push_back(rp);
        i++;
    }
    bytesRead = 0;
    i = 0;
    // Copy data to local buffer
    while (bytesRead < INDEX_TABLE_SIZE) {
        int size = std::min(blockSize, INDEX_TABLE_SIZE - bytesRead);
        bytesRead += size;

        auto rp = readRequests[i];
        rp->wait();
        char *buf = rp->getReturnValue();
        memcpy(indexBuf + i*blockSize, buf, size);
        delete[] buf;
        i++;
    }

    // Update index table
    indexTable.restore(indexBuf);
    delete[] indexBuf;

    // Read write-ahead log
    std::vector<LogEntry> logEntries(KV_LOG_SIZE);

    int numThreads = 12;
    std::vector<std::thread> readLogThreads;
    for (int i = 0; i < numThreads; i++) {
        int start = i * (KV_LOG_SIZE / numThreads);
        int end = (i + 1) * (KV_LOG_SIZE / numThreads);
        if (i == numThreads-1) {
            end = KV_LOG_SIZE;
        }
        readLogThreads.push_back(std::thread(&KVCoordinator::readLog, this, start, end, std::ref(logEntries)));
    }

    for (int i = 0; i < numThreads; i++) {
        readLogThreads[i].join();
    }

    // Collect writes to the same keys
    std::unordered_map<std::string,std::string> logValues;
    for (int i = 0; i < KV_LOG_SIZE; i++) {
        LogEntry& logEntry = logEntries[i];
        std::string key(logEntry.getKey());
        std::string value(logEntry.getValue());
        logValues[key] = value;
    }

    for (auto update : logValues) {
        auto key = update.first;
        auto value = update.second;
        std::unique_lock<std::mutex> applyIndexMapLock(applyIndexMapMtx);
        applyIndexMap[key] = 0;
        applyIndexMapLock.unlock();

        KVRequest *kvRequest = new KVRequest(KVRequestType::PUT);
        kvRequest->setKey(key);
        kvRequest->setValue(value);
        kvRequest->setLogIndex(0);

        if (cache->insert(key, value, true) == -1) {
            DIE("Failed to update cache");
        }

        next_committed_index.fetch_add(1);

        applyQueue.enqueue(kvRequest);
    }

    waitForApplies();

    auto t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    LogInfo("KV recovery took " << time_span.count() << "ms");

    status = LEADER;
}