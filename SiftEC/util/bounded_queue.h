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

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>

#include "common/test.h"

template<typename T, size_t MAX>
class BoundedQueue {
public:
    bool isEmpty() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.empty();
    }

    void dequeue(T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (count == 0) {
            cons_cv.wait(mlock);
        }
        item = queue_.front();
        queue_.pop();
        count--;
        prod_cv.notify_one();
    }

    void enqueue(const T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (count > MAX) {
            prod_cv.wait(mlock);
        }
        queue_.push(item);
        count++;
        cons_cv.notify_one();
    }

    void enqueue(const std::vector<T> &items) {
        std::unique_lock<std::mutex> mlock(mutex_);
        for (auto item : items) {
            while (count > MAX) {
                prod_cv.wait(mlock);
            }
            queue_.push(item);
            count++;
            cons_cv.notify_one();
        }
    }

    size_t size() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return count;
    }

private:
    std::queue<T> queue_;
    size_t count;
    std::mutex mutex_;
    std::condition_variable prod_cv;
    std::condition_variable cons_cv;
};