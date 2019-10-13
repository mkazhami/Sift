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
#include <list>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "lru.h"

// TODO: Add namespace

class LRUMultiEvictThread;

class LRUMultiCache {
  //private:
public:
    int counter_;        // Number of items in the cache
    bool done_;          // Tracks whether the system is alive.
    int num_targets_;    // Number of caches to choose from on eviction.


    // Different watermark points used to determine which eviction
    // action to take. The cache will never have more than the high
    // watermark number of entries (it will block inserts). 
    int high_watermark_;
    int low_watermark_;
    int warning_mark_;

    // Eviction threads
    std::vector<LRUMultiEvictThread*> evict_threads_;

    constexpr static double low_fraction_  = 0.92;
    constexpr static double warn_fraction_ = 0.98;
    constexpr static uint64_t hash_seed_   = 0xDEADBEEF;

    // Synchronization primitives to communicate with the eviction thread.
    std::mutex lock_;
    std::condition_variable cv_int_;
    std::condition_variable cv_ext_;

    // Vector of LRUCache instances
    std::vector<LRUCacheInterface*> caches_;

    // Only called by the eviction threads. Evicts one entry from the cache if
    // the number of entries is above the warning watermark. Otherwise it blocks 
    // and waits until more entries are added.
    void evictOne(std::vector<LRUCacheInterface*>* evict_targets);

    // Decrement the counter value after evicting an entry. Will wake up other
    // threads that are waiting for the cache to have an available spot.
    void decrementCounter();

    // Increments the counter value after inserting an entry. Will block if
    // the cache is already full. 
    void incrementCounter();

    // Main function of the eviction thread.
    void evictThread(std::vector<LRUCacheInterface*>* evict_targets);

    friend class LRUMultiEvictThread;

  public:

    // Must pass in the number of internal cache instances to have, the number
    // of instances to randomly select when looking for an item to evict, and 
    // the size of the cache in terms of the number of entries.
    LRUMultiCache(int num_caches, int evict_size, int max_entries,
                  int num_evict_threads, const CacheFactory& cf);

    // Performs cleanup. Waits until the evict thread exits before returning.
    ~LRUMultiCache();

    // Adds a new entry to the cache. If key already exists, it updates
    // the value. If the existing entry is dirty, the call blocks until
    // the entry is no longer dirty before replacing it. Any other interface
    // would not be safe in our system due to the loose synchronization
    // between the caching layer and the wrteback layer.
    int insert(const std::string& key, const std::string& val, bool dirty);

    // Get the value of a key. Returns -1 if the key does not exist.
    int get(const std::string& key, std::string* val);

    // Set a dirty entry to clean.
    int setClean(const std::string& key);
};

class LRUMultiEvictThread {
  public:
    LRUMultiEvictThread(LRUMultiCache* multi_cache, 
                        const std::vector<LRUCacheInterface*>& caches);
    ~LRUMultiEvictThread();
    void run();

  private:
    LRUMultiCache* multi_cache_;
    std::thread* evict_thread_;
    std::vector<LRUCacheInterface*> targets_;
};

