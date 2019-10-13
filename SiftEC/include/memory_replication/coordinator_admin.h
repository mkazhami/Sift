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

#include <string>
#include <chrono>
#include <thread>
#include <iostream>
#include <memory>
#include <map>
#include <atomic>

#include "include/memory_replication/coordinator_state.h"
#include "include/memory_replication/memory_servers.h"
#include "include/memory_replication/success_count.h"
#include "include/memory_replication/term_id.h"
#include "include/util/serializable_address_value_pair.h"
#include "util/barrier.h"

class CoordinatorService;
class Coordinator;

class CoordinatorAdmin {
public:
    CoordinatorAdmin(std::string configFile, int node_id);
    ~CoordinatorAdmin();
    // Block and wait until the memory_replication shutdowns.
    void handleState();

    Coordinator *rdma_clients[TOTAL_MEMORY_SERVERS];
    CoordinatorService *coordinatorService;
    CoordinatorState coordinatorState;
    MemoryServers memoryServers;
    uint32_t node_id;

    void calculateValues() {
        for (int i = 0; i < memoryServers.total; i++) {
            remote_state_machine_address[i] = new std::map<std::string, int>();
        }
    }

    void memoryNodeFailure(int node_id);

private:
    void readHeartBeat(int i);
    void writeHeartBeat(int i, uint32_t, uint32_t);
    void acquireVote(int i);
    void startWriteHeartBeat();
    void becomeFollower();
    void becomeCandidate();
    void becomeLeader();
    void recover();
    void recoverMemoryNode(int node);

    void writeLogEntry(int server, uint32_t idx, char *log_entry);
    void updateAppliedIndex(int server, uint32_t new_index);
    void applyToMemory(int server, uint64_t address, char *value);
    void readLog(int server, std::vector<SerializableAVPair::AVPair*>& log, Barrier *barrier);
    uint32_t readAppliedIndex(int server);
    std::pair<std::vector<uint32_t>, uint32_t> getConsistentLog(const std::vector<std::vector<SerializableAVPair::AVPair*>>& logs);

    TermId termId;
    std::map<int, std::map<std::string, int> *> remote_state_machine_address;
    int READ_TIMEOUT_MS = 7;
    int WRITE_TIMEOUT_MS = 5;
    int FAILURE_TOLERANCE = 3;
    size_t adminRegionSize = sizeof(uint64_t);

    // Heartbeat threads shared variables
    enum HeartbeatOp {
        HB_READ, HB_WRITE, HB_VOTE
    };
    enum HeartbeatOp heartbeatOp;
    std::atomic<bool> terminate;
    std::vector<std::thread*> heartbeatThreads;
    Barrier startBarrier;
    Barrier endBarrier;
    uint32_t tId;
    uint32_t ts;
    std::unique_ptr<SuccessCount> sc;

    void changeHeartbeatOp(enum HeartbeatOp heartbeatOp);
    void heartbeatLoop(int i);
};