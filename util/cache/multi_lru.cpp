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

#include <algorithm>
#include <random>
#include <iostream>
#include <assert.h>
#include <farmhash.h>

#include "multi_lru.h"
#include "simple_lru.h"
#include "common/logging.h"

LRUMultiEvictThread::LRUMultiEvictThread(
        LRUMultiCache* multi_cache,
        const std::vector<LRUCacheInterface*>& caches)
    : multi_cache_(multi_cache), targets_(caches) {
    evict_thread_ = new std::thread(&LRUMultiEvictThread::run, this);
    assert(evict_thread_ != NULL); 
}

void LRUMultiEvictThread::run() {
    multi_cache_->evictThread(&targets_);
}

LRUMultiEvictThread::~LRUMultiEvictThread() {
    if (evict_thread_ != NULL) {
        evict_thread_->join();
        delete evict_thread_;
    }
}

void LRUMultiCache::evictOne(std::vector<LRUCacheInterface*>* evict_targets) {
    // evict_targets is a copy of the caches array. On every call to evictOne,
    // we shuffle the order to select different caches to evict from. 
    std::random_shuffle(evict_targets->begin(), evict_targets->end());
    
    // Retrieve the timestamp of the oldest clean entry in the first num_targets_
    // number of caches with at least a single clean entry.
    int num_checked = 0;
    hr_time oldest;
    LRUCacheInterface* selected = NULL;
    for (auto c : *evict_targets) {
        hr_time c_time;
        if (c->getOldestClean(&c_time) == -1) {
            continue; // Empty cache, skip
        }
        // Keep track of the oldest entry.
        if (selected == NULL || c_time < oldest) {
            selected = c;
            oldest = c_time;
        }
        // Increment the number of targets that we have checked. Stop if 
        // it is bigger than or equal to num_targets.
        if (++num_checked >= num_targets_) {
            break;
        }
    }
    // Evict the selected entry and decrement counter if eviction was successful. 
    // Evict can fail if the target cache is empty, which can happen if another 
    // evict thread evicts all of the clean entries from that cache first.
    if (selected != NULL) {
        if (selected->evictOne() != -1) {
            decrementCounter();
        }
    } else {
        // TODO: Selected can be NULL if all entries are dirty. Currently 
        //       assuming that this does not happen often. If this is not true,
        //       then the eviction thread should block and wait until an entry is 
        //       no longer dirty. Should replace LRU with FIFO in that case.
        LogDebug("No target to evict. All entries are dirty.");
    }
}

void LRUMultiCache::decrementCounter() {
    std::unique_lock<std::mutex> lk(lock_);
    counter_--; // Remove an entry
    lk.unlock();
    // Wake up a thread waiting for the counter value to drop
    // below the high watermark.
    cv_ext_.notify_one();
}

void LRUMultiCache::incrementCounter() {
    std::unique_lock<std::mutex> lk(lock_);
    while (counter_ >= high_watermark_) {
        cv_ext_.wait(lk);
    }
    counter_++;
    // If we are above the warning point, we want to start the
    // eviction threads back up to start evicting entries.
    if (counter_ >= warning_mark_) {
        lk.unlock();
        cv_int_.notify_all();
    }
}

void LRUMultiCache::evictThread(std::vector<LRUCacheInterface*>* evict_targets) {
    while (true) {
        std::unique_lock<std::mutex> lk(lock_);
        // If it is below the low watermark, we should stop evicting
        // and wait for more entries to be added.
        while (!done_ && counter_ < low_watermark_) {
            cv_int_.wait(lk);
        }
        if (done_) {
            return;  // Destructor has been called.
        }
        lk.unlock(); // Release lock. Evict one will re-acquire as needed.
        evictOne(evict_targets);
    }
}

LRUMultiCache::LRUMultiCache(int num_caches, int evict_size, int max_entries,
                             int num_evict_threads, const CacheFactory& cf)
        : counter_(0), done_(false), num_targets_(evict_size) {
    assert(evict_size <= num_targets_);
    std::srand(1); // TODO: Seed with time in the future.

    // Create LRU cache instances.
    for (int i = 0; i < num_caches; ++i) {
        LRUCacheInterface* c = cf.createInstance();
        assert(c != NULL);
        caches_.push_back(c);
    }

    // Setup the watermarks for use in eviction.
    high_watermark_ = max_entries;
    low_watermark_  = max_entries * low_fraction_;
    warning_mark_   = max_entries * warn_fraction_;

    // Create eviction threads. 
    for (int i = 0; i < num_evict_threads; ++i) {
        LRUMultiEvictThread* ethread = new LRUMultiEvictThread(this, caches_);
        assert (ethread != NULL);
        evict_threads_.push_back(ethread);
    }
}

LRUMultiCache::~LRUMultiCache() {
    std::unique_lock<std::mutex> lk(lock_);
    done_ = true; // Done, shutting down
    lk.unlock();
    cv_int_.notify_all();

    // Wait until the eviction thread has completed.
    for (auto ethread : evict_threads_) {
        delete ethread;
    }

    // Delete cache instances.
    for (auto c : caches_) {
        delete c;
    }
}

int LRUMultiCache::insert(const std::string& key,
                          const std::string& val,
                          bool dirty) {
    // Using farm hash to pick a cache
    int c_index = util::Hash64WithSeed(key, hash_seed_) % caches_.size();
    // Assume an new entry will be required
    incrementCounter();
    // Call insert on the selected cache.
    int rc = caches_[c_index]->insert(key, val, dirty);
    // TODO: Should make this an enum
    if (rc <= 0) {
        // Received an error, or entry was overwritten. Can decrement
        // the counter since the size should not have changed.
        decrementCounter();
    }
    return rc;
}

int LRUMultiCache::get(const std::string& key, std::string* val) {
    // Using farm hash to pick a cache
    int c_index = util::Hash64WithSeed(key, hash_seed_) % caches_.size();
    return caches_[c_index]->get(key, val);
}

int LRUMultiCache::setClean(const std::string& key) {
    int c_index = util::Hash64WithSeed(key, hash_seed_) % caches_.size();
    return caches_[c_index]->setClean(key);
}

