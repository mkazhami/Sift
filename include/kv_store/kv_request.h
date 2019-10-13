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
#include <mutex>
#include <condition_variable>

enum KVRequestType {
    GET, PUT
};

class KVRequest {
public:
    KVRequest(KVRequestType type) : type(type), done(false) {}
    virtual ~KVRequest() {}

    virtual void finish() {

    }

    void setKey(const std::string &key) {
        this->key = key;
    }
    void setValue(const std::string &value) {
        this->value = value;
    }

    void setLogIndex(uint32_t index) {
        this->log_index = index;
    }

    virtual std::string getKey() {
        return key;
    }

    virtual std::string getValue() {
        return value;
    }

    uint32_t getLogIndex() {
        return log_index;
    }

    KVRequestType getRequestType() {
        return type;
    }

    virtual void completeGetRequest(std::string value) {
        setValue(value);
        std::unique_lock<std::mutex> mlock(mtx);
        done = true;
        cv.notify_one();
    }

    virtual void completePutRequest() {
        std::unique_lock<std::mutex> mlock(mtx);
        done = true;
        cv.notify_one();
    }

    void wait() {
        std::unique_lock<std::mutex> mlock(mtx);
        while (!done) {
            cv.wait(mlock);
        }
    }



private:
    KVRequestType type;
    std::string key;
    std::string value;
    uint32_t log_index;

    std::mutex mtx;
    std::condition_variable cv;
    bool done;
};