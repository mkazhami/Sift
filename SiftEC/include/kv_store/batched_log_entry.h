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

#include "kv_request.h"
#include "include/memory_replication/request_processor.h"

class BatchedLogEntry {
public:
    BatchedLogEntry() {
        size = 0;
    }

    void addRequest(KVRequest* r) {
        size += 1;
        requests.push_back(r);
    }
    void setRequestProcessor(std::shared_ptr<RequestProcessor> rp) {
        this->rp = rp;
    }


    KVRequest* getRequest(int i) {
        return requests[i];
    }
    std::shared_ptr<RequestProcessor> getRequestProcessor() {
        return rp;
    }
    int getSize() {
        return size;
    }

private:
    int size;
    std::vector<KVRequest*> requests;
    std::shared_ptr<RequestProcessor> rp;
};