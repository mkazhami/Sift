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

#include <vector>
#include <mutex>
#include <condition_variable>

#include "include/memory_replication/request_processor.h"
#include "include/network/work_request.h"

class LocalRequestProcessor : public RequestProcessor{
public:
    LocalRequestProcessor(WorkRequestType requestType);
    ~LocalRequestProcessor();

    void finish();
    void setResponse(char *value);
    void sendResponse();
    void processReadRequest(char *value);
    void processWriteRequest();
    void sendNotLeaderError();

    uint64_t getAddress(int);
    char *getValue(int);
    size_t getValueSize(int);
    char *getReturnValue();
    bool getManageConflictFlag(int);

    WorkRequestType getRequestType();
    void wait();
    bool test();

    void reset();
    void addAddress(uint64_t);
    void addValue(char *, size_t);
    void addManageConflictFlag(bool);

private:
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<uint32_t> done;
};