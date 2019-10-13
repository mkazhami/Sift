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

#include <mutex>
#include <condition_variable>
#include <list>
#include <unordered_map>
#include "common/common.h"

template<typename K, typename V, uint64_t MAX>
class LRUCache {
public:
    void add(const K &key, const V* value) {
        std::unique_lock<std::mutex> ulock(mtx);

        auto cacheEntry = cacheMap.find(key);

        if (cacheEntry != cacheMap.end()) {
            // Remove the current entry since we are replacing it
            delete[] cacheEntry->second->second;
            cacheList.erase(cacheEntry->second);
            cacheMap.erase(cacheEntry);
        } else {
            size++;
            if (size > MAX) {
                evict();
            }
        }

        cacheList.push_back(std::make_pair(key, (char*)value));
        auto iter = cacheList.end();
        iter--;
        cacheMap[key] = iter;
    }

    // Returns empty string if not found
    bool get(const K &key, V* &value) {
        std::unique_lock<std::mutex> ulock(mtx);

        auto cacheEntry = cacheMap.find(key);
        if (cacheEntry != cacheMap.end()) {
            value = new char[RM_MEMORY_BLOCK_SIZE];
            memcpy(value, cacheEntry->second->second, RM_MEMORY_BLOCK_SIZE);
            //value = strndup(cacheEntry->second->second, RM_MEMORY_BLOCK_SIZE);
            return true;
        }
        return false;
    }

private:
    void evict() {
        cacheMap.erase(cacheList.front().first);
        delete[] cacheList.front().second; // Assume it's a pointer
        cacheList.pop_front();
        size--;
    };

    std::mutex mtx;
    std::list<std::pair<K,V*>> cacheList;
    std::unordered_map<K, decltype(cacheList.begin())> cacheMap;
    uint32_t size;
};