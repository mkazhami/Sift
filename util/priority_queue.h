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
#include <initializer_list>

#include "common/logging.h"

template<typename T, typename C>
class PriorityQueue {
public:
    bool isEmpty() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.empty();
    }

    void wait_dequeue(T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        item = queue_.top();
        queue_.pop();
    }

    template<typename... Args>
    void emplace(Args... args) {
        std::unique_lock<std::mutex> mlock(mutex_);
        queue_.emplace(args...);
        cond_.notify_one();
    }

    void enqueue(const T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        queue_.push(item);
        cond_.notify_one();
        mlock.unlock();
    }

    T peek() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.top();
    }

    void pop() {
        std::unique_lock<std::mutex> mlock(mutex_);
        queue_.pop();
    }

    void wait_peek(T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        item = queue_.top();
    }

    uint64_t size() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.size();
    }

private:
    std::priority_queue<T, std::vector<T>, C> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;
};