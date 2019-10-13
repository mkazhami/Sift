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

//#include <pthread.h>
#include <mutex>
#include <condition_variable>

class SuccessCount {
public:
    SuccessCount(int);
    ~SuccessCount();
    // Change the internal state of SuccessCount.
    void incrementCount();
    void incrementServerCount();
    void resetCount();
    void reset();
    // Wait until state greater than or equals the parameter.
    void wait_ge(int expectedCount);
    void wait_ge2(int expectedCount);
    int getCount();
    int getServerCount();
    int max;
private:
    int count = 0;
    int server_response = 0;
    std::mutex mtx;
    std::condition_variable cv;
};