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

#include "lru.h"
#include <string>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <ctime>
#include <chrono>

class SimpleCacheValue;
class SimpleLRU; 

typedef std::shared_ptr<SimpleCacheValue> SCVSharedPtr;
typedef std::unordered_map<std::string, SCVSharedPtr> KVTable;

class SimpleCacheValue {
  private:
    const std::string key_;
    std::string val_;
    bool dirty_;
    hr_time last_accessed_;
    std::list<SCVSharedPtr>::iterator pos_;

  public:
    SimpleCacheValue(const std::string& key, const std::string& val, bool dirty)
        : key_(key), val_(val), dirty_(dirty), 
          last_accessed_(std::chrono::high_resolution_clock::now()) {}

    ~SimpleCacheValue() {}

    friend class SimpleLRU;
};

class SimpleLRU : public LRUCacheInterface {
  public: 
    SimpleLRU(){}
    virtual int insert(const std::string& key,
                       const std::string& val,
                       bool dirty);
    virtual int get(const std::string& key, std::string* val);
    virtual int setClean(const std::string& key);
    virtual int evictOne();
    virtual int getOldestClean(hr_time* oldest_time);
    virtual ~SimpleLRU(){}

    int getNumClean() {
        std::unique_lock<std::mutex> lock(lock_);
        return order_.size();
    }
    int getNumTotal() {
        std::unique_lock<std::mutex> lock(lock_);
        return table_.size();
    }

  //private:
    std::list<SCVSharedPtr> order_;
    std::unordered_map<std::string, SCVSharedPtr> table_; 
    std::mutex lock_;

    void addToOrderList(SCVSharedPtr cval);
};

class SimpleCacheFactory : public CacheFactory {
  public:
    virtual LRUCacheInterface* createInstance() const {
        return new SimpleLRU();
    }
};
