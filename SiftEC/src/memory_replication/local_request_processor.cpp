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

#include "include/memory_replication/local_request_processor.h"

LocalRequestProcessor::LocalRequestProcessor(WorkRequestType requestType) {
    this->requestType = requestType;
    requests = 0;
    done.store(0);
}

LocalRequestProcessor::~LocalRequestProcessor() {
    addresses.clear();
    values.clear();
    value_sizes.clear();
}

void LocalRequestProcessor::reset() {
    addresses.clear();
    values.clear();
    value_sizes.clear();
    requests = 0;
    return_value = NULL;
    done.store(0);
}

void LocalRequestProcessor::finish() {

}

void LocalRequestProcessor::wait() {
    std::unique_lock<std::mutex> ulock(mtx);
    cv.wait(ulock, [this] { return done.load() >= requests; });
}

bool LocalRequestProcessor::test() {
    return done.load() >= requests;
}

void LocalRequestProcessor::addAddress(uint64_t address) {
    addresses.push_back(address);
    requests++;
}

void LocalRequestProcessor::addValue(char *value, size_t value_size) {
    values.push_back(value);
    value_sizes.push_back(value_size);
}

void LocalRequestProcessor::addManageConflictFlag(bool manage_conflict) {
    manage_conflicts.push_back(manage_conflict);
}

void LocalRequestProcessor::setResponse(char *value) {
    switch (requestType) {
        case WorkRequestType::READ:
            processReadRequest(value);
            break;
        case WorkRequestType::WRITE:
            processWriteRequest();
            break;
        default:
            DIE("Request type unknown");
    }
}

void LocalRequestProcessor::sendResponse() {
    uint32_t ret = done.fetch_add(1);
    if (ret == requests-1) {
        std::unique_lock<std::mutex> ulock(mtx);
        cv.notify_one();
    }
}

void LocalRequestProcessor::processReadRequest(char *value) {
    return_value = value;
}

void LocalRequestProcessor::processWriteRequest() {

}

WorkRequestType LocalRequestProcessor::getRequestType() {
    return requestType;
}

void LocalRequestProcessor::sendNotLeaderError() {}


uint64_t LocalRequestProcessor::getAddress(int i) {
    return addresses[i];
}

char *LocalRequestProcessor::getValue(int i) {
    if (requestType == WorkRequestType::WRITE) {
        return values[i];
    } else {
        DIE("Cannot get value from read request processor");
    }
}

size_t LocalRequestProcessor::getValueSize(int i) {
    if (requestType == WorkRequestType::WRITE) {
        return value_sizes[i];
    } else {
        DIE("Cannot get value size from read request processor");
    }
}

bool LocalRequestProcessor::getManageConflictFlag(int i) {
    if (requestType == WorkRequestType::WRITE) {
        return manage_conflicts[i];
    } else {
        DIE("Cannot sent manage conflict flag for read request processor");
    }
}

char *LocalRequestProcessor::getReturnValue() {
    return return_value;
}
