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
#include <chrono>
#include <memory>

typedef std::chrono::high_resolution_clock::time_point hr_time;

class LRUCacheInterface {
  public:
    // Inserts an entry into the LRU cache. Return -1 on error, 0 if an entry 
    // was updated, and 1 if a new entry was added into the cache.
    virtual int insert(const std::string& key,
                       const std::string& val, 
                       bool dirty) = 0;
    virtual int get(const std::string& key, std::string* val) = 0;
    virtual int setClean(const std::string& key) = 0;
    virtual int evictOne() = 0;
    virtual int getOldestClean(hr_time* oldest_time) = 0;
    virtual ~LRUCacheInterface(){}
};

class CacheFactory {
  public:
    virtual LRUCacheInterface* createInstance() const = 0;
    virtual ~CacheFactory(){}
};
