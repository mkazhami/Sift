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

#include "include/network/rdma/local_rdma_client.h"

LocalCoordinator::LocalCoordinator(int node_id) {
    // Nothing to set up
    this->node_id = node_id;
    state = RUNNING;
}

LocalCoordinator::~LocalCoordinator() {
    // Nothing to clean up
}

bool LocalCoordinator::CreateConnection() {
    // Nothing to set up for connection (yet)
    return true;
}

int LocalCoordinator::Read(WorkRequest* workReq) {
    uint64_t address = workReq->getOffset();
    uint64_t size = workReq->getSize();

    workReq->setWorkRequestCommand(READ_);

    // Copy data into buffer
    workReq->copyValue(node_id, data + address);

    // Signal completion to client
    // TODO: update callback to account for local client
    workReq->getCallback()(workReq, node_id);

    return 1;
}

int LocalCoordinator::Write(WorkRequest* workReq) {
    uint64_t address = workReq->getOffset();
    uint64_t size = workReq->getSize();
    char *value = workReq->getValue()[node_id];

    workReq->setWorkRequestCommand(WRITE_);

    memcpy(data + address, value, size);

    // Signal completion to client
    workReq->getCallback()(workReq, node_id);

    return 1;
}

int LocalCoordinator::Write(std::vector<WorkRequest*> &workReqs) {
    for (WorkRequest* workReq : workReqs) {
        Write(workReq);
    }
    return workReqs.size();
}

int LocalCoordinator::CompSwap(WorkRequest* workReq) {
    // TODO: make this atomic
    uint64_t address = workReq->getOffset();
    uint64_t compare = workReq->getCompare();
    uint64_t swap = workReq->getSwap();

    workReq->setWorkRequestCommand(CAS);

    uint64_t local_value;
    memcpy(&local_value, data + address, sizeof(uint64_t));
    if (local_value == compare) {
        memcpy(data + address, &swap, sizeof(uint64_t));
    }

    // Copy original value to be returned to client
    workReq->copyValueACS((char*)&local_value);

    // Signal completion to client
    workReq->getCallback()(workReq, node_id);

    return 1;
}

