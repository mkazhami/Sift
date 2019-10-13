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

#include "include/network/work_request.h"

class RequestProcessor {
public:
    virtual ~RequestProcessor() {}

    virtual void reset()=0;
    virtual void finish()=0;
    virtual void setResponse(char *)=0;
    virtual void sendResponse()=0;
    virtual void processReadRequest(char *)=0;
    virtual void processWriteRequest()=0;
    virtual void sendNotLeaderError()=0;

    virtual WorkRequestType getRequestType()=0;
    virtual void addAddress(uint64_t)=0;
    virtual void addValue(char*, size_t)=0;
    virtual void addManageConflictFlag(bool)=0;
    virtual uint64_t getAddress(int)=0;
    virtual char *getValue(int)=0;
    virtual size_t getValueSize(int)=0;
    virtual bool getManageConflictFlag(int)=0;
    virtual char *getReturnValue()=0;

    std::vector<uint64_t> &getAddresses() {
        return addresses;
    }
    std::vector<char*> &getValues() {
        return values;
    }
    std::vector<size_t> &getValueSizes() {
        return value_sizes;
    }
    uint32_t getNumRequests() {
        return requests;
    }
    void setLogFlag(bool flag) {
        log = flag;
    }
    bool getLogFlag() {
        return log;
    }

    virtual void wait()=0;
    virtual bool test()=0;

    enum CallStatus {
        CREATE, PROCESS, WAIT, FINISH
    };
    CallStatus status_;  // The current serving state.

    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;

protected:
    WorkRequestType requestType;
    uint32_t requests;
    std::vector<uint64_t> addresses;
    std::vector<char*> values;
    std::vector<size_t> value_sizes;
    std::vector<bool> manage_conflicts;
    char *return_value;
    bool log;
};