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

#include <stdint.h>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <atomic>

#include "common/common.h"
#include "include/network/rdma/rdma.h"

class RequestProcessor;

enum WorkRequestType {
    NONE_REQUEST_TYPE, LOG_WRITE, APPLIED_INDEX_WRITE, READ, WRITE
};

enum WorkRequestCommand{
    NONE_REQUEST_COMMAND, READ_, WRITE_, CAS
};

class CoordinatorService;
class CoordinatorAdmin;

class WorkRequest {
public:
    WorkRequest();
    ~WorkRequest();

    uint64_t getOffset();
    char *getValue();
    char *getWriteValue();
    size_t getSize();
    size_t getWriteSize();
    uint64_t getValueACS();
    int getVotes();
    WorkRequestCommand getWorkRequestCommand();
    WorkRequestType getRequestType();
    uint64_t getCompare();
    uint64_t getSwap();
    void (*getCallback())(WorkRequest*, int);
    void *getConnection();
    std::shared_ptr<RequestProcessor> getOriginalRequest();
    uint32_t getLogIndex();
    uint64_t getRDMABufferOffset(int);
    bool getManageConflictFlag();

    void setOffset(uint64_t offset);
    void setRequestType(WorkRequestType requestType);
    void setWorkRequestCommand(WorkRequestCommand wrc);
    void setSize(size_t size);
    void setCompare(uint64_t comp);
    void setSwap(uint64_t swap);
    void setCallback(void (*f)(WorkRequest*, int));
    void setConnection(void *conn);
    void setOriginalRequest(std::shared_ptr<RequestProcessor> originalRequest);
    void setLogIndex(uint32_t log_index);
    void setWriteValue(char *);
    void setWriteSize(size_t);
    void setRDMABufferOffset(int, uint64_t);
    void setManageConflictFlag(bool);

    void copyValue(char* value);
    void copyValueACS(char* value);

    int prepare();
    void vote(int nodeId);
    void reset();
    void resetVote(int nodeId);
    void setValue(char *value);
    int getVote(int nodeId, WorkRequestType stage);
    void setStage(int nodeId, WorkRequestType stage);

    bool ready();
    int done();
    bool isDone();

    // Coordinator context
    CoordinatorService *service = nullptr;

private:
    size_t size = 0;
    std::atomic<int> total_votes;
    int votes[TOTAL_MEMORY_SERVERS];
    WorkRequestType stages[TOTAL_MEMORY_SERVERS];
    uint64_t rdma_buffer_offset[TOTAL_MEMORY_SERVERS];
    uint64_t offset;
    char *value;
    char *write_value;
    size_t write_size = 0;
    std::atomic<bool> valueReady;
    std::shared_ptr<RequestProcessor> originalRequest;
    WorkRequestType requestType;
    WorkRequestCommand workRequestCommand;
    uint64_t compare;
    uint64_t swap;
    uint64_t buf_ACS;
    std::atomic<int> done_votes;
    uint32_t log_index;
    bool manage_conflict;

    void (*callback)(WorkRequest*, int) = nullptr;
    void *conn;
};
