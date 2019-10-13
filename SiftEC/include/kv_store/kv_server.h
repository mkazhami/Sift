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

#include "include/kv_store/kv_coordinator.h"
#include "common/logging.h"

class KVServer {
public:
    KVServer(uint32_t serverID) {
        kvCoordinator = new KVCoordinator(serverID);
        kvCoordinator->init();
    }
    ~KVServer() {
        delete kvCoordinator;
    }

    void waitForApplies() {
        kvCoordinator->waitForApplies();
    }

    void put(const std::string &key, const std::string &value) {
        LogDebug("Adding put request for <" << key << "," << value << ">");
        KVRequest *request = new KVRequest(PUT);
        request->setKey(key);
        request->setValue(value);
        kvCoordinator->processPutRequest(request);
        request->wait();
        delete request;
    }

    std::string get(const std::string &key) {
        LogDebug("Adding get request for <" << key << ">");
        KVRequest *request = new KVRequest(GET);
        request->setKey(key);
        kvCoordinator->processGetRequest(request);
        // Don't need to wait for result since processGetRequest performs the GET to completion
        std::string read_value = request->getValue();
        delete request;
        return read_value;
    }

    void put(KVRequest *request) {
        LogDebug("Adding put request for <" << request->getKey() << "," << request->getValue() << ">");
        kvCoordinator->processPutRequest(request);
    }

    void get(KVRequest *request) {
        LogDebug("Adding get request for key " << request->getKey());
        kvCoordinator->processGetRequest(request);
    }

private:
    KVCoordinator *kvCoordinator;
};