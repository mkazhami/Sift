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

#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "common/test.h"

template<typename T>
class Queue {
public:
    T pop() {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        auto item = queue_.front();
        queue_.pop_front();
        return item;
    }

    void pop(int position) {
        std::unique_lock<std::mutex> mlock(mutex_);
        if (position >= queue_.size()) {
            DIE("Cannot pop at position " << position << ", deque is of size " << queue_.size());
        }
        queue_.erase(queue_.begin() + position);
    }

    bool peek(int position, T& item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        if (position >= queue_.size()) {
            return false;
        }
        item = queue_[position];
        return true;
    }

    bool isEmpty() {
        std::unique_lock<std::mutex> mlock(mutex_);
    	return queue_.empty();
    }

    void wait_dequeue(T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        item = queue_.front();
        queue_.pop_front();
    }

    void enqueue(const T &item) {
        std::unique_lock<std::mutex> mlock(mutex_);
        queue_.push_back(item);
        cond_.notify_one();
        mlock.unlock();
    }

    T peek() {
        return queue_.front();
    }

    T wait_peek() {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        return queue_.front();
    }

    size_t size() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.size();
    }

    void waitNonEmpty() {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cond_.wait(mlock);
        }
        cond_.notify_one();
    }

private:
    std::deque<T> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;
};