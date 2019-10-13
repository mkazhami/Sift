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

#include "simple_lru.h"
#include <assert.h>

// Assumes that caller has lock.
void SimpleLRU::addToOrderList(SCVSharedPtr cval) {
    if (!cval->dirty_) {
        order_.push_back(cval);
        cval->pos_ = std::prev(order_.end());
    }
}

int SimpleLRU::insert(const std::string& key, 
                      const std::string& val, 
                      bool dirty) {
    // Create new entry and insert into the cache
    SCVSharedPtr cval(new SimpleCacheValue(key, val, dirty));
    std::unique_lock<std::mutex> lock(lock_);

    /// Check if this item already exists in the cache.
    KVTable::iterator find_it;
    if ((find_it = table_.find(key)) != table_.end()) {
        // Overwrite existing entry, even if dirty.
        SCVSharedPtr exist_val = find_it->second;

        // Remove existing entry (if it exists) from the list.
        if (!exist_val->dirty_) {
            order_.erase(exist_val->pos_);
        }

        // Replace entry in the table and re-add to the list
        // if this entry is not dirty. Note that since we are 
        // using smart pointers for the SimpleCacheValue object, 
        // we don't need to explicitly call deleted on it.
        find_it->second = cval;
        addToOrderList(cval);
        return 0; // Size did not change.
    }
    // New entry. Add to the list and to the table.
    table_[key] = cval;
    addToOrderList(cval);
    return 1; // Size of LRU increased by one.
}   


int SimpleLRU::get(const std::string& key, std::string* val) {
    std::unique_lock<std::mutex> lock(lock_);
    KVTable::iterator find_it = table_.find(key);
    if (find_it == table_.end()) {
        return -1;
    }
    SCVSharedPtr cval = find_it->second;

    // Retrieve the value and update the last accessed time.
    *val = cval->val_;
    cval->last_accessed_ = std::chrono::high_resolution_clock::now();

    // Update position in the order list
    if (!cval->dirty_) {
        // Move to the end of the list using splice. I believe 
        // the iterator (cval->pos_) is still valid after the
        // splice call.
        order_.splice(order_.end(), order_, cval->pos_);
    }
    return 0;
}

int SimpleLRU::setClean(const std::string& key) {
    std::unique_lock<std::mutex> lock(lock_);
    KVTable::iterator find_it = table_.find(key);
    if (find_it == table_.end()) {
        return -1;
    }  

    // If existing entry is not dirty, do nothing.
    SCVSharedPtr exist_val = find_it->second;
    if (!exist_val->dirty_) {
        return 0;
    }

    // Set clean and add to the LRU order list.
    exist_val->dirty_ = false;
    addToOrderList(exist_val);
    return 0;
}

int SimpleLRU::evictOne() {
    std::unique_lock<std::mutex> lock(lock_);
    if (order_.empty()) {
        return -1; // Nothing to do
    }
    SCVSharedPtr cval = *(order_.begin());
    table_.erase(cval->key_);

    // Should only evict items that are clean. Remove from the order list.
    assert(!cval->dirty_);
    order_.erase(cval->pos_);
    return 0;
}

int SimpleLRU::getOldestClean(hr_time* oldest_time) {
    std::unique_lock<std::mutex> lock(lock_);
    if (order_.empty()) {
        return -1; // Nothing to do
    }
    SCVSharedPtr cval = *(order_.begin());
    *oldest_time = cval->last_accessed_;
    return 0;
}

