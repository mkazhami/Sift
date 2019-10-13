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
#include <assert.h>

#include "common/test.h"

template<typename T, size_t MAX>
class WeightedBoundedQueue {
public:
    bool isEmpty() {
        std::unique_lock<std::mutex> mlock(mutex_);
        return queue_.empty();
    }

    // NOTE: This function does change the total count of the queue. You must call
    //       pop( count ) to actually "remove" them and clear space.
    std::vector<T> dequeue(size_t limit) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (queue_.empty()) {
            cons_cv.wait(mlock);
        }
        std::vector<T> vec;
        size_t count = 0;
        while (!queue_.empty()) {
            if (count + counts.front() > limit) break;
            vec.push_back(queue_.front());
            count += counts.front();
            //total_count -= counts.front();
            queue_.pop();
            counts.pop();
            //prod_cv.notify_one();
        }
        return vec;
    }

    void enqueue(const T &item, size_t count) {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (total_count + count > MAX) {
            prod_cv.wait(mlock);
        }
        queue_.push(item);
        total_count += count;
        counts.push(count);
        cons_cv.notify_one();
    }

    void pop(size_t count) {
        std::unique_lock<std::mutex> mlock(mutex_);
        assert(total_count >= count);

        total_count -= count;
        prod_cv.notify_all();
    }

    size_t size() const {
        return queue_.size();
    }

private:
    std::queue<T> queue_;
    size_t total_count;
    std::queue<size_t> counts;
    std::mutex mutex_;
    std::condition_variable prod_cv;
    std::condition_variable cons_cv;
};