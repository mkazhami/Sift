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

#include <thread>

#include "include/memory_replication/coordinator_service.h"
#include "include/memory_replication/coordinator_admin.h"
#include "include/memory_replication/local_request_processor.h"
#include "common/logging.h"

class LocalReplicationServer {
public:

    LocalReplicationServer(const uint32_t &serverId) {
#if USE_PERSISTENCE
        coordinatorServer = new CoordinatorAdmin(CONFIG_FILE, serverId, &persistentStore);
        coordinatorService = new CoordinatorService(serverId, &persistentStore);
#else
        coordinatorServer = new CoordinatorAdmin(CONFIG_FILE, serverId, nullptr);
        coordinatorService = new CoordinatorService(serverId, nullptr);
#endif
        coordinatorServer->coordinatorService = coordinatorService;
        coordinatorService->init(coordinatorServer->rdma_clients);
        LogInfo("Created coordinator service");
    }

    ~LocalReplicationServer() {
        delete coordinatorServer;
        delete coordinatorService;
    }

    // There is no shutdown handling in this code.
    void Run(bool wait=true) {
        // Have a new thread handle state transitions
        std::thread(&CoordinatorAdmin::handleState, coordinatorServer).detach();
        if (wait) {
            coordinatorServer->coordinatorState.wait_e(CoordinatorState::LEADER);
        }
    }

    void WaitForLeader() {
        coordinatorServer->coordinatorState.wait_e(CoordinatorState::LEADER);
    }

    std::shared_ptr<RequestProcessor> readRequest(uint64_t address, size_t length=RM_MEMORY_BLOCK_SIZE) {
        std::shared_ptr<RequestProcessor> requestProcessor(new LocalRequestProcessor(WorkRequestType::READ));
        requestProcessor->reset();
        requestProcessor->addAddress(address);
        requestProcessor->addValue(nullptr, length);
        coordinatorService->submitReadRequest(requestProcessor);
        return requestProcessor;
    }

    std::shared_ptr<RequestProcessor> writeRequest(uint64_t address, char *value, size_t length, bool manage_conflict, bool log=true) {
        std::shared_ptr<RequestProcessor> requestProcessor(new LocalRequestProcessor(WorkRequestType::WRITE));
        requestProcessor->reset();
        requestProcessor->addAddress(address);
        requestProcessor->addValue(value, length);
        requestProcessor->addManageConflictFlag(manage_conflict);
        requestProcessor->setLogFlag(log);
        if (log) {
            coordinatorService->submitWriteRequest(requestProcessor);
        } else {
            coordinatorService->submitUnloggedWriteRequest(requestProcessor);
        }
        return requestProcessor;
    }

    std::shared_ptr<RequestProcessor> writeRequest(const std::vector<uint64_t> &addresses, const std::vector<char*> &values, const std::vector<size_t> &lengths, const std::vector<bool> &manage_conflicts, bool log=true) {
        std::shared_ptr<RequestProcessor> requestProcessor(new LocalRequestProcessor(WorkRequestType::WRITE));
        requestProcessor->reset();
        for (unsigned int i = 0; i < addresses.size(); i++) {
            requestProcessor->addAddress(addresses[i]);
            requestProcessor->addValue(values[i], lengths[i]);
            requestProcessor->addManageConflictFlag(manage_conflicts[i]);
        }
        requestProcessor->setLogFlag(log);
        if (log) {
            coordinatorService->submitWriteRequest(requestProcessor);
        } else {
            coordinatorService->submitUnloggedWriteRequest(requestProcessor);
        }
        return requestProcessor;
    }

    CoordinatorService *coordinatorService;
    CoordinatorAdmin *coordinatorServer;
#if USE_PERSISTENCE
    PersistentStore persistentStore;
#endif
};



