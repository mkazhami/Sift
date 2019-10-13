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

class LockCacheValue;
class EntryLockLRU; 

typedef std::shared_ptr<LockCacheValue> LCVSharedPtr;
typedef std::unordered_map<std::string, LCVSharedPtr> LKVTable;

class LockCacheValue {
  private:
    const std::string key_;
    std::string val_;
    bool dirty_;
    hr_time last_accessed_;
    std::mutex lock_;
    std::condition_variable cv_;
    std::list<LCVSharedPtr>::iterator pos_;

  public:
    LockCacheValue(const std::string& key, const std::string& val, bool dirty)
        : key_(key), val_(val), dirty_(dirty), 
          last_accessed_(std::chrono::high_resolution_clock::now()) {}

    ~LockCacheValue() {}

    friend class EntryLockLRU;
};

class EntryLockLRU : public LRUCacheInterface {
  public: 
    EntryLockLRU(){}
    virtual int insert(const std::string& key,
                       const std::string& val,
                       bool dirty);
    virtual int get(const std::string& key, std::string* val);
    virtual int setClean(const std::string& key);
    virtual int evictOne();
    virtual int getOldestClean(hr_time* oldest_time);
    virtual ~EntryLockLRU(){}

  private:
    std::list<LCVSharedPtr> order_;
    std::unordered_map<std::string, LCVSharedPtr> table_; 
    std::mutex table_lock_;
    std::mutex order_lock_;

    void addToOrderList(LCVSharedPtr cval);
    int try_insert(const LCVSharedPtr& cval);
};

class EntryLockCacheFactory : public CacheFactory {
  public:
    virtual LRUCacheInterface* createInstance() const {
        return new EntryLockLRU();
    }
};
