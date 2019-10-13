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

#include "include/memory_replication/success_count.h"

SuccessCount::SuccessCount(int m) {
    max = m;
}

SuccessCount::~SuccessCount() {}

void SuccessCount::reset() {
    std::unique_lock<std::mutex> l(mtx);
    count = 0;
    server_response = 0;
}

void SuccessCount::incrementCount() {
    std::unique_lock<std::mutex> l(mtx);
    count++;
    server_response++;
    cv.notify_all();
}

void SuccessCount::resetCount() {
    std::unique_lock<std::mutex> l(mtx);
    count = 0;
    cv.notify_all();
}

void SuccessCount::wait_ge(int expectedCount) {
    std::unique_lock<std::mutex> l(mtx);
    while (server_response < expectedCount) {
        cv.wait(l);
    }
}

void SuccessCount::wait_ge2(int expectedCount) {
    std::unique_lock<std::mutex> l(mtx);
    while (count < expectedCount) {
        cv.wait(l);
    }
}

int SuccessCount::getCount() {
    std::unique_lock<std::mutex> l(mtx);
    return count;
}

void SuccessCount::incrementServerCount() {
    std::unique_lock<std::mutex> l(mtx);
    server_response++;
    cv.notify_all();
}

int SuccessCount::getServerCount() {
    std::unique_lock<std::mutex> l(mtx);
    return server_response;
}
