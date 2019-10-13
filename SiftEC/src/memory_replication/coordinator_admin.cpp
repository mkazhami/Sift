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

#include "include/memory_replication/coordinator_admin.h"
#include "include/util/serializable_address_value_pair.h"
#include "include/network/rdma/local_rdma_client.h"
#include "include/network/rdma/remote_rdma_client.h"
#include "include/memory_replication/coordinator_service.h"
#include "../../include/util/serializable_address_value_pair.h"

#include <future>
#include <random>
#include <event.h>
#include <algorithm>
#include <unordered_map>

void _onRequestCompletion(WorkRequest*, int);
void _onFailure(int);
void _retryRequest(WorkRequest*);

CoordinatorAdmin::CoordinatorAdmin(std::string configFile, int node_id)
        : node_id(node_id), startBarrier(TOTAL_MEMORY_SERVERS+1), endBarrier(TOTAL_MEMORY_SERVERS+1), terminate(false) {
    RemoteCoordinator::admin = this;
    RemoteCoordinator::failure_callback = _onFailure;
    RemoteCoordinator::retry_callback = _retryRequest;

    memoryServers.parse(configFile.c_str());
    calculateValues();
    ServerList &servers = memoryServers.servers;
    for (int i = 0; i < servers.size(); i++) {
        try {
            if (i == LOCAL_MEMORY_NODE && COORDINATOR_MEMORY_NODE) {
                rdma_clients[i] = new LocalCoordinator(i);
                LogInfo("Memory node " << i << " is local to the coordinator");
            } else {
                std::string port = servers.at(i).port;
                LogInfo("Connecting to memory node at " << servers.at(i).ip << ":" << port);
                RemoteCoordinator *rc = new RemoteCoordinator(
                        (char *) servers.at(i).ip.c_str(), (char *) port.c_str(), i);
                rdma_clients[i] = rc;
                if (rdma_clients[i]->CreateConnection()) {
                    rdma_clients[i]->state = RUNNING;

                    LogInfo("Connected to memory node at " << servers.at(i).ip << ":" << port);
                } else {
                    LogError("Could not connect to memory server " << i);
                }
            }
        } catch (std::exception &e) {
            LogDebug("Caught exception: " << e.what());
        }
    }
}

CoordinatorAdmin::~CoordinatorAdmin() {
    terminate = true;
    startBarrier.Terminate();
    endBarrier.Terminate();
    for (auto t : heartbeatThreads) {
        t->join();
    }
}

void _onFailure(int node_id) {
    RemoteCoordinator::admin->memoryNodeFailure(node_id);
}

void _retryRequest(WorkRequest *workRequest) {
    LogDebug("Got retry request for type: " << workRequest->getRequestType());
    // TODO: account for failed heartbeat messages
    if (workRequest->service) {
        workRequest->service->retryRequest(workRequest);
    } else {
        delete workRequest;
    }
}

void _onRequestCompletion(WorkRequest *workRequest, int node_id) {
    int votes = workRequest->prepare();
    if (workRequest->getWorkRequestCommand() == READ_) {
        if (!COORDINATOR_MEMORY_NODE || node_id != LOCAL_MEMORY_NODE) {
            struct connection *conn = (struct connection*)workRequest->getConnection(node_id);
            char *serialValue = conn->rdma_local_region + workRequest->getRDMABufferOffset(node_id);
            if (votes == 0) {
                workRequest->copyValue(node_id, serialValue);
            }
        }
        // Can now notify the coordinator thread that the value is ready
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == WRITE_) {
        workRequest->vote(node_id);
    } else if (workRequest->getWorkRequestCommand() == CAS) {
        if (!COORDINATOR_MEMORY_NODE || node_id != LOCAL_MEMORY_NODE) {
            struct connection *conn = (struct connection*)workRequest->getConnection(node_id);
            workRequest->copyValueACS(conn->rdma_local_region + workRequest->getRDMABufferOffset(node_id));
        }
        workRequest->vote(node_id);
    }
}

void CoordinatorAdmin::memoryNodeFailure(int node_id) {
    if (rdma_clients[node_id]->state != RECOVER) {
        rdma_clients[node_id]->state = RECOVER;
        LogInfo("Starting recovery thread for memory node " << node_id);
        std::thread(&CoordinatorAdmin::recoverMemoryNode, this, node_id).detach();
    }
}

void CoordinatorAdmin::recoverMemoryNode(int node) {
    while (!rdma_clients[node]->CreateConnection()) {
        LogError("Failed to reconnect to memory server " << node);
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (terminate) return;
    }

    // Wait for connection to be fully established
    while (rdma_clients[node]->state != RECOVER) {}
    LogInfo("Successfully reconnected to memory server " << node);

    // Find most up-to-date server
    int upToDateServer = -1;
    uint64_t maxIndex = 0;
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        if (i != node && coordinatorService->serverVersion[i] > maxIndex) {
            maxIndex = coordinatorService->serverVersion[i];
            upToDateServer = i;
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    // Lock 16k blocks at a time, only send blocks that are used
    // When entire RM is copied, copy over the log
    const int numBlocks = RM_REPLICATED_MEMORY_SIZE / RM_MEMORY_BLOCK_SIZE;
    int lockedBlockIndex = 0;
    std::unordered_set<uint64_t> lockedBlocks;
    const size_t lockTableSize = (RM_REPLICATED_MEMORY_SIZE / RM_MEMORY_BLOCK_SIZE)/1;
    const int startBlock = RM_REPLICATED_MEMORY_OFFSET/RM_MEMORY_BLOCK_SIZE;

    long long read_time = 0;
    long long write_time = 0;
    while (true) {
        int numBlocksToCopy = std::min(numBlocks - lockedBlockIndex, 1000);
        if (numBlocksToCopy <= 0) break;

        LogDebug("Copying blocks " << lockedBlockIndex << " to " << lockedBlockIndex + numBlocksToCopy);

        std::vector<WorkRequest*> requests(numBlocksToCopy, nullptr);

        auto start = std::chrono::high_resolution_clock::now();
        LogDebug("Reading from up to date server");

        std::unique_lock<std::mutex> bitmapLock(coordinatorService->usedBlocksBitmapMtx);
        for (int i = 0; i < numBlocksToCopy; i++) {
            if (lockedBlocks.insert((startBlock + lockedBlockIndex + i) % lockTableSize).second) {
                coordinatorService->blockLocks.readLock(
                        startBlock + lockedBlockIndex + i);
            }

            if (coordinatorService->usedBlocks.getBit(lockedBlockIndex + i) != 0) {
                WorkRequest *workRequest = new WorkRequest;
                workRequest->setRequestType(READ);
                workRequest->setSize(RM_MEMORY_BLOCK_SIZE);
                workRequest->setCallback(_onRequestCompletion);
                workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + (lockedBlockIndex + i) * RM_MEMORY_BLOCK_SIZE);
                workRequest->setStage(upToDateServer, READ);
                rdma_clients[upToDateServer]->Read(workRequest);
                requests[i] = workRequest;
            }
        }
        bitmapLock.unlock();

        LogDebug("Waiting for reads and sending writes to recovering node");

        // Get read value and write it to the recovering node
        for (int i = 0; i < numBlocksToCopy; i++) {
            WorkRequest *workRequest = requests[i];
            if (workRequest != nullptr) {
                while (workRequest->getVote(upToDateServer, READ) != 1) {}
                char *value = workRequest->getValue()[upToDateServer];
                delete workRequest;

                workRequest = new WorkRequest;
                workRequest->setRequestType(WRITE);
                workRequest->setValue(node, value);
                workRequest->setSize(RM_MEMORY_BLOCK_SIZE);
                workRequest->setCallback(_onRequestCompletion);
                workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + (lockedBlockIndex + i) * RM_MEMORY_BLOCK_SIZE);
                workRequest->setStage(node, WRITE);
                rdma_clients[node]->Write(workRequest);
                requests[i] = workRequest;

                delete value;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        read_time += time_span.count();

        start = std::chrono::high_resolution_clock::now();

        // Wait for writes to complete
        // TODO: parallelize reading and writing phases
        for (int i = 0; i < numBlocksToCopy; i++) {
            WorkRequest *workRequest = requests[i];
            if (workRequest != nullptr) {
                while (workRequest->getVote(node, WRITE) != 1) {}
                delete workRequest;
            }
        }

        end = std::chrono::high_resolution_clock::now();
        time_span = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        write_time += time_span.count();

        lockedBlockIndex += numBlocksToCopy;
    }

    rdma_clients[node]->state = RUNNING;

    // Unlock all of the blocks
    std::vector<uint64_t> heldLocks(numBlocks);
    std::iota(heldLocks.begin(), heldLocks.end(), startBlock);
    coordinatorService->blockLocks.readUnlock(heldLocks);

    auto t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    LogInfo("Read time: " << read_time << "ms, Write time: " << write_time << "ms");
    LogInfo("Done recovery - took " << time_span.count() << "ms");
}

void CoordinatorAdmin::changeHeartbeatOp(enum HeartbeatOp heartbeatOp) {
    if (!this->terminate) {
        this->heartbeatOp = heartbeatOp;
    }
}

void CoordinatorAdmin::heartbeatLoop(int i) {
    while (true) {
        startBarrier.Wait();

        if (terminate) {
            break;
        }

        if (heartbeatOp == HB_READ) {
            readHeartBeat(i);
        } else if (heartbeatOp == HB_WRITE) {
            writeHeartBeat(i, tId, ts);
        } else if (heartbeatOp == HB_VOTE) {
            acquireVote(i);
        }

        endBarrier.Wait();
    }
}

void CoordinatorAdmin::readHeartBeat(int i) {
    if (rdma_clients[i]->state != RUNNING) return;

    WorkRequest *workRequest = new WorkRequest();
    workRequest->setOffset(RM_SERVER_ID_TERM_ID_OFFSET);
    workRequest->setSize(adminRegionSize);
    workRequest->setStage(i, READ);
    workRequest->setWorkRequestCommand(READ_);
    workRequest->setCallback(_onRequestCompletion);
    workRequest->resetVote(i);

    rdma_clients[i]->Read(workRequest);
    // Wait for request to complete or failure to occur
    while (workRequest->getVote(i, READ) != 1 && rdma_clients[i]->state == RUNNING) {}
    if (rdma_clients[i]->state != RUNNING) {
        return;
    }

    char *data = workRequest->getValue()[i];

    uint64_t rdata;
    memcpy(&rdata, data, sizeof(uint64_t));

    uint32_t rSId = (rdata & 0xFFFF000000000000) >> 48;
    uint32_t rTId = (rdata & 0x0000FFFF00000000) >> 32;
    uint32_t rTs = (rdata & 0x00000000FFFFFFFF);
    delete[] data;
    LogDebug("Read heartbeat - server id: " << rSId << " term id: " << rTId << " timestamp: " << rTs);

    memoryServers.servers[i].lastServerId = rSId;
    memoryServers.servers[i].lastTermId = rTId;
    bool r = termId.setTermId(rTId);
    if (r && rTs != memoryServers.servers[i].lastTimeStamp) {
        memoryServers.servers[i].lastTimeStamp = rTs;
        this->sc->incrementCount();
    } else {
        this->sc->incrementServerCount();
        LogDebug("Failed for memory_replication " << i << ". Current count: " << sc.get()->getCount()
                                                  << ". Term: " << rTId << ", TS: " << rTs);
    }
    delete workRequest;
}

void CoordinatorAdmin::writeHeartBeat(int i, uint32_t tId, uint32_t ts) {
    if (rdma_clients[i]->state != RUNNING) return;

    uint64_t data = ((uint64_t)(node_id & 0xFFFF)) << 48 | ((uint64_t)(tId & 0xFFFF)) << 32 | ts;
    uint32_t old_ts = memoryServers.servers[i].lastTimeStamp;
    uint64_t old_data = ((uint64_t)(node_id & 0xFFFF)) << 48 | ((uint64_t)(tId & 0xFFFF)) << 32 | old_ts;

    WorkRequest *workRequest = new WorkRequest();
    workRequest->setOffset(RM_SERVER_ID_TERM_ID_OFFSET);
    workRequest->setSize(sizeof(uint64_t));
    workRequest->setStage(i, LOG_WRITE);
    workRequest->setCompare(old_data);
    workRequest->setSwap(data);
    workRequest->setWorkRequestCommand(CAS);
    workRequest->setCallback(_onRequestCompletion);
    workRequest->resetVote(i);

    rdma_clients[i]->CompSwap(workRequest);
    // Wait for request to complete or failure to occur
    while (workRequest->getVote(i, LOG_WRITE) != 1 && rdma_clients[i]->state == RUNNING) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    if (rdma_clients[i]->state != RUNNING) {
        // TODO: handle error
        return;
    }

    uint64_t compared_value = workRequest->getValueACS();
    LogDebug("CAS heartbeat: value on server: " << compared_value << " compared value: " << old_data);
    int res = memcmp(&old_data, &compared_value, sizeof(uint64_t));

    if (res == 0) {
        memoryServers.servers[i].lastTimeStamp++;
        this->sc->incrementCount();
        LogDebug("Success for memory_replication " << i << ". Current count: " << sc.get()->getCount());
    } else {
        uint32_t rSId = (compared_value & 0xFFFF000000000000) >> 48;
        uint32_t rTId = (compared_value & 0x0000FFFF00000000) >> 32;
        uint32_t rTs = (compared_value & 0x00000000FFFFFFFF);
        memoryServers.servers[i].lastServerId = rSId;
        memoryServers.servers[i].lastTermId = rTId;
        memoryServers.servers[i].lastTimeStamp = rTs;

        this->sc->incrementServerCount();
        LogDebug("Failed for memory_replication " << i << ". Current count: " << sc.get()->getCount());
    }
    delete workRequest;
}

void CoordinatorAdmin::startWriteHeartBeat() {
    int failureCount = 0;
    while (failureCount < FAILURE_TOLERANCE) {
        sc->reset();
        uint32_t tId = termId.getTermId();
        uint32_t ts = 0;

        // Get next timestamp by incrementing the current max
        for (int i = 0; i < memoryServers.getTotal(); i++) {
            if (memoryServers.servers[i].lastTimeStamp > ts) {
                ts = memoryServers.servers[i].lastTimeStamp;
            }
        }
        ts++;

        LogDebug("Writing heartbeat - term id: " << tId << " ts: " << ts);
        this->ts = ts;
        this->tId = tId;
        changeHeartbeatOp(HB_WRITE);
        startBarrier.Wait();
        endBarrier.Wait();

        // TODO: sleep the difference, not entire timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(WRITE_TIMEOUT_MS));

        if (terminate) break;

        if (sc->getCount() < memoryServers.quorum) {
            LogDebug("Heartbeat failed within time limit (" << failureCount + 1 << "/" << FAILURE_TOLERANCE
                                                            << "). Count: " << sc->getCount());
            failureCount++;
        } else {
            LogDebug("Heartbeat succeeded. Count: " << sc->getCount());
            failureCount = 0;
        }
    }
    LogInfo("Writes failed. Stepping down...");
    coordinatorState.change_state(CoordinatorState::FOLLOWER);
}

void CoordinatorAdmin::acquireVote(int i) {
    LogDebug("Acquiring vote from server " << i);
    if (rdma_clients[i]->state != RUNNING) return;

    uint32_t lsId = memoryServers.servers[i].lastServerId;
    uint32_t ltId = memoryServers.servers[i].lastTermId;
    uint32_t lTs = memoryServers.servers[i].lastTimeStamp;
    uint64_t lastValue = ((uint64_t)(lsId & 0xFFFF)) << 48 | ((uint64_t)(ltId & 0xFFFF)) << 32 | lTs;

    uint32_t csId = node_id;
    uint32_t ctId = termId.getTermId();
    uint32_t cTs = lTs;
    uint64_t currentValue = ((uint64_t)(csId & 0xFFFF)) << 48 | ((uint64_t)(ctId & 0xFFFF)) << 32 | cTs;

    LogDebug("Trying to write vote - server id: " << csId << " term id: " << ctId << " timestamp: " << cTs);

    WorkRequest *workRequest = new WorkRequest();
    workRequest->setOffset(RM_SERVER_ID_TERM_ID_OFFSET);
    workRequest->setSize(sizeof(uint64_t));
    workRequest->setStage(i, LOG_WRITE);
    workRequest->setWorkRequestCommand(CAS);
    workRequest->resetVote(i);
    workRequest->setCompare(lastValue);
    workRequest->setSwap(currentValue);
    workRequest->setCallback(_onRequestCompletion);
    rdma_clients[i]->CompSwap(workRequest);
    // Wait for request to complete or failure to occur
    while (workRequest->getVote(i, LOG_WRITE) != 1 && rdma_clients[i]->state == RUNNING) {}
    if (rdma_clients[i]->state != RUNNING) {
        // TODO: handle error
        return;
    }
    delete workRequest;


    workRequest = new WorkRequest();
    workRequest->setOffset(RM_SERVER_ID_TERM_ID_OFFSET);
    workRequest->setSize(adminRegionSize);
    workRequest->setStage(i, READ);
    workRequest->setWorkRequestCommand(READ_);
    workRequest->resetVote(i);
    workRequest->setCallback(_onRequestCompletion);
    rdma_clients[i]->Read(workRequest);
    // Wait for request to complete or failure to occur
    while (workRequest->getVote(i, READ) != 1 && rdma_clients[i]->state == RUNNING) {}
    if (rdma_clients[i]->state != RUNNING) {
        // TODO: handle error
        return;
    }
    char *data = workRequest->getValue()[i];
    delete workRequest;

    uint64_t rdata;
    memcpy(&rdata, data, sizeof(uint64_t));
    uint32_t rSId = (rdata & 0xFFFF000000000000) >> 48;
    uint32_t rTId = (rdata & 0x0000FFFF00000000) >> 32;
    uint32_t rTs = (rdata & 0x00000000FFFFFFFF);
    delete[] data;

    LogDebug("Read back vote - server id: " << rSId << " term id: " << rTId << " timestamp: " << rTs);

    memoryServers.servers[i].lastServerId = rSId;
    memoryServers.servers[i].lastTermId = rTId;
    memoryServers.servers[i].lastTimeStamp = rTs;

    if (rSId == csId && rTId == ctId) {
        this->sc->incrementCount();
        LogDebug("Success for server " << i << ". Current count: " << sc->getCount()
                                       << ". Term: " << rTId << ", TS: " << rTs);
    } else {
        this->sc->incrementServerCount();
        termId.setTermId(rTId);
        LogDebug("Failed for server " << i << ". Current count: " << sc->getCount()
                                      << ". Term: " << rTId << ", TS: " << rTs);
    }
}

void CoordinatorAdmin::becomeLeader() {
    LogInfo("Became leader");
    //coordinatorState.change_state(CoordinatorState::LEADER_PREP);

    LogDebug("Starting write heartbeat");
    std::thread(&CoordinatorAdmin::startWriteHeartBeat, this).detach();

    LogInfo("Performing log recovery...");
    recover();
    LogInfo("Done recovery");

    coordinatorState.change_state(CoordinatorState::LEADER);
    LogDebug("State -> LEADER. Ready to serve client requests!");

    coordinatorState.wait_ne(CoordinatorState::LEADER);
    LogInfo("No longer a leader...");
}

void CoordinatorAdmin::becomeCandidate() {
    LogDebug("Becoming candidate");
    coordinatorState.change_state(CoordinatorState::CANDIDATE);

    LogDebug("Starting election");

    termId.increment();
    
    sc->reset();
    changeHeartbeatOp(HB_VOTE);
    startBarrier.Wait();
    endBarrier.Wait();

    if (this->sc->getCount() < memoryServers.quorum) {
        LogDebug("Failed to acquire majority votes. Count: " << sc->getCount());
    } else {
        LogDebug("Election succeeded.");
        becomeLeader(); // Blocking. Returns on failure.
        return;
    }
    LogDebug("Election failed!");
}

void CoordinatorAdmin::becomeFollower() {
    LogDebug("Became follower");
    coordinatorState.change_state(CoordinatorState::FOLLOWER);

    LogDebug("Starting read heartbeat");
    int failureCount = 0;
    while (failureCount < FAILURE_TOLERANCE) {
        sc->reset();
        changeHeartbeatOp(HB_READ);
        startBarrier.Wait();
        endBarrier.Wait();

        // TODO: sleep the difference, not entire timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(READ_TIMEOUT_MS));

        if (this->sc->getCount() < memoryServers.quorum) {
            LogDebug("Heartbeat failed within time limit (" << failureCount + 1 << "/" << FAILURE_TOLERANCE
                                                            << "). Count: " << sc->getCount());
            failureCount++;
        } else {
            LogDebug("Heartbeat succeeded.");
            failureCount = 0;
        }
    }
    LogDebug("Heartbeats failed!");
}

void CoordinatorAdmin::handleState() {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_real_distribution<float> dist(50.0, 100.0);

    LogInfo("Handling state");

    sc = std::unique_ptr<SuccessCount>(new SuccessCount(3));
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        heartbeatThreads.push_back(new std::thread(&CoordinatorAdmin::heartbeatLoop, this, i));
    }

    while (true) {
        becomeFollower(); // Blocking. Returns on failure.
        if (terminate) break;
        becomeCandidate(); // Blocking. Returns on failure.
        if (terminate) break;
        int backoff = (int) dist(mt);
        LogInfo("Random backoff " << backoff);
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
    }
}

void CoordinatorAdmin::writeLogEntry(int server, uint32_t idx, char *log_entry) {
    if (rdma_clients[server]->state != RUNNING) return;

    WorkRequest *workRequest = new WorkRequest;
    workRequest->setValue(server, log_entry);
    workRequest->setRequestType(LOG_WRITE);
    workRequest->setSize(RM_LOG_BLOCK_SIZE);
    workRequest->setCallback(_onRequestCompletion);
    workRequest->setOffset(RM_WRITE_AHEAD_LOG_OFFSET + idx * RM_LOG_BLOCK_SIZE);
    workRequest->setStage(server, LOG_WRITE);
    rdma_clients[server]->Write(workRequest);

    while (workRequest->getVote(server, LOG_WRITE) != 1) {}
    delete workRequest;
}

void CoordinatorAdmin::updateAppliedIndex(int server, uint32_t new_index) {
    if (rdma_clients[server]->state != RUNNING) return;

    WorkRequest *workRequest = new WorkRequest;
    workRequest->setValue(server, (char*)&new_index);
    workRequest->setSize(RM_APPLIED_INDEX_SIZE);
    workRequest->setRequestType(LOG_WRITE);
    workRequest->setOffset(RM_APPLIED_INDEX_OFFSET);
    workRequest->setCallback(_onRequestCompletion);
    workRequest->setStage(server, WRITE);
    rdma_clients[server]->Write(workRequest);

    while (workRequest->getVote(server, WRITE) != 1) {}
    delete workRequest;
}

void CoordinatorAdmin::applyToMemory(int server, uint64_t address, char *value) {
    if (rdma_clients[server]->state != RUNNING) return;

    WorkRequest *workRequest = new WorkRequest;
    workRequest->setValue(server, value);
    workRequest->setSize(RM_MEMORY_BLOCK_SIZE);
    workRequest->setRequestType(WRITE);
    workRequest->setOffset(RM_REPLICATED_MEMORY_OFFSET + address);
    workRequest->setCallback(_onRequestCompletion);
    workRequest->setStage(server, WRITE);
    rdma_clients[server]->Write(workRequest);

    while (workRequest->getVote(server, WRITE) != 1) {}
    delete workRequest;
}

void CoordinatorAdmin::readLog(int server, std::vector<SerializableAVPair::AVPair*>& log, Barrier *barrier) {
    // The max RDMA op queue size is ~16000, so can't have more than this many outstanding requests
    // TODO: max queue size is system-dependent, make it variable
    const uint64_t batch_size = 16000;
    uint64_t read_entries = 0;
    std::vector<WorkRequest *> requests(RM_LOG_SIZE);
    log.reserve(RM_LOG_SIZE);

    if (rdma_clients[server]->state != RUNNING) {
        barrier->Wait();
        return;
    }

    while (read_entries < RM_LOG_SIZE) {
        uint64_t entries_to_read = std::min(batch_size, RM_LOG_SIZE - read_entries);
        for (int i = 0; i < entries_to_read; i++) {
            WorkRequest *workRequest = new WorkRequest;
            workRequest->setRequestType(READ);
            workRequest->setSize(RM_LOG_BLOCK_SIZE);
            workRequest->setCallback(_onRequestCompletion);
            workRequest->setOffset(RM_WRITE_AHEAD_LOG_OFFSET + (read_entries+i) * RM_LOG_BLOCK_SIZE);
            workRequest->setStage(server, READ);
            rdma_clients[server]->Read(workRequest);

            requests[read_entries+i] = workRequest;
        }

        for (int i = 0; i < entries_to_read; i++) {
            WorkRequest *workRequest = requests[read_entries+i];
            while (workRequest->getVote(server, READ) != 1) {}
            SerializableAVPair::AVPair *log_entry = (SerializableAVPair::AVPair*)workRequest->getValue()[server];
            log[read_entries+i] = log_entry;
            delete workRequest;
        }
        read_entries += entries_to_read;
    }

    barrier->Wait();
}

uint32_t CoordinatorAdmin::readAppliedIndex(int server) {
    if (rdma_clients[server]->state != RUNNING) return 0;

    // Send applied index read
    WorkRequest *aiWorkRequest = new WorkRequest;
    aiWorkRequest->setRequestType(READ);
    aiWorkRequest->setSize(RM_APPLIED_INDEX_SIZE);
    aiWorkRequest->setCallback(_onRequestCompletion);
    aiWorkRequest->setOffset(RM_APPLIED_INDEX_OFFSET);
    aiWorkRequest->setStage(server, READ);
    rdma_clients[server]->Read(aiWorkRequest);

    // Get values
    while (aiWorkRequest->getVote(server, READ) != 1) {}
    uint32_t ai = *(uint32_t*)aiWorkRequest->getValue();

    delete aiWorkRequest;

    return ai;
};

std::pair<std::vector<uint32_t>, uint32_t> CoordinatorAdmin::getConsistentLog(const std::vector<std::vector<SerializableAVPair::AVPair*>>& logs) {
    std::vector<uint32_t> consistent_log(RM_LOG_SIZE);
    uint32_t up_to_date_server = 0;
    uint32_t max_index = 0;

    for (int i = 0; i < RM_LOG_SIZE; i++) {
        uint32_t max = 0;
        for (int j = 0; j < TOTAL_MEMORY_SERVERS; j++) {
            SerializableAVPair::AVPair *avPair = logs[j][i];
            if (avPair == nullptr) continue;
            if (avPair->log_index > max) {
                max = avPair->log_index;
                if (max > max_index) {
                    max_index = max;
                    up_to_date_server = j;
                }
            }
        }
        consistent_log[i] = max;
    }

    return std::make_pair(consistent_log, up_to_date_server);
}

void CoordinatorAdmin::recover() {
#if(USE_APPLIED_INDEX)
    LogInfo("Reading applied indices");

    std::vector<uint32_t> appliedIndices(TOTAL_MEMORY_SERVERS);
    // Get applied index from each server
    uint32_t max_applied_index = 0;
    uint32_t max_applied_server = 0;
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        uint32_t idx = readAppliedIndex(i);
        appliedIndices[i] = idx;

        if (idx > max_applied_index) {
            max_applied_index = idx;
            max_applied_server = i;
        }
    }
#endif

    LogInfo("Reading logs from memory servers");

    auto t1 = std::chrono::high_resolution_clock::now();

    Barrier barrier(TOTAL_MEMORY_SERVERS + 1);

    // Get logs from each server
    std::vector<std::vector<SerializableAVPair::AVPair*>> logs(TOTAL_MEMORY_SERVERS);
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        std::thread(&CoordinatorAdmin::readLog, this, i, std::ref(logs[i]), &barrier).detach();
    }

    barrier.Wait();

    LogDebug("Finished reading logs");

    // Get consistent log from majority
    auto ret = getConsistentLog(logs);
    auto consistent_log = ret.first;
    auto up_to_date_server = ret.second;

    LogDebug("Obtained consistent log, starting recovery process. Most up to date server is server " << up_to_date_server);

    // TODO: check for index integer overflow

    // Add missing committed log entries to each server
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        if (rdma_clients[i]->state != RUNNING) continue;
        for (uint32_t j = 0; j < RM_LOG_SIZE; j++) {
            if (logs[i][j] != nullptr && logs[i][j]->log_index != consistent_log[j]) {
                LogInfo("Missing committed entry on memory server " << i << ", log index " << j << "(has: " << logs[i][j]->log_index << " expected: " << consistent_log[j] << ")");
                writeLogEntry(i, j, (char*)logs[up_to_date_server][j]);
            }
        }
    }

    LogDebug("Done adding missing committed entries");

    // Apply all unapplied log entries to each server
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        if (rdma_clients[i]->state != RUNNING) continue;
#if(USE_APPLIED_INDEX)
        for (uint32_t j = appliedIndices[i]; j < max_applied_index; j++) {
            LogInfo("Missing applied entry on memory server " << i << ", log index " << j);
            applyToMemory(i, logs[max_applied_server][j]->address, logs[max_applied_server][j]->value);
        }
        updateAppliedIndex(i, max_applied_index);
#else
        for (uint32_t j = 0; j < RM_LOG_SIZE; j++) {
            applyToMemory(i, logs[up_to_date_server][j]->address, logs[up_to_date_server][j]->value);
        }
#endif
    }

    auto t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
    LogInfo("Done log recovery - took " << time_span.count() << "ms");
}

