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

#include "entry_lock_lru.h"
#include <assert.h>

// Assumes that caller has lock.
void EntryLockLRU::addToOrderList(LCVSharedPtr cval) {
    if (!cval->dirty_) {
        order_.push_back(cval);
        cval->pos_ = std::prev(order_.end());
    }
}

int EntryLockLRU::try_insert(const LCVSharedPtr& cval) {
    std::unique_lock<std::mutex> table_lock(table_lock_);

    /// Check if this item already exists in the cache.
    LKVTable::iterator find_it;
    if ((find_it = table_.find(cval->key_)) != table_.end()) {
        LCVSharedPtr exist_val = find_it->second;
        // Acquire entry lock
        std::unique_lock<std::mutex> entry_lock(exist_val->lock_);
        
        if (exist_val->dirty_) {
            table_lock.unlock();
            (exist_val->cv_).wait(entry_lock);
            return -1; // Retry from the beginning to be safe.
        } 

        // Change the entry. Don't have to explicitly delete the
        // old entry because of the smart pointers. 
        find_it->second = cval;

        // Acquire the new entry lock. Note that this cannot block 
        // (and therefore cannot deadlock) since no other thread has a
        // reference to the new entry until we release the table lock.
        std::unique_lock<std::mutex> new_entry_lock(cval->lock_);
        
        // We can now safely remove the table lock.
        table_lock.unlock();

        // Remove previous entry from the list and add the new
        // one to it (only if it is dirty). Currently holds
        // both the entry lock and the order lock while doing
        // this. Might be possible to release the entry lock
        // after acquiring the order lock if we are careful.
        // However, this is unlikely to yield much improvement.
        std::unique_lock<std::mutex> order_lock(order_lock_);
        order_.erase(exist_val->pos_);
        addToOrderList(cval);
        return 0; // Size did not change.
    }

    // New entry. Add to the list and to the table. Don't need the
    // entry lock since no has a reference to this entry until this
    // thread releases the table lock.
    table_[cval->key_] = cval;

    // Need to acquire the order lock. Don't need the entry lock
    // since we are not modifying the entry.
    std::unique_lock<std::mutex> order_lock(order_lock_); 
    addToOrderList(cval);
    return 1; // Size of LRU increased by one.
}

int EntryLockLRU::insert(const std::string& key, 
                         const std::string& val, 
                         bool dirty) {
    // Create new entry and insert into the cache
    LCVSharedPtr cval(new LockCacheValue(key, val, dirty));

    // Keep trying to insert until try_insert succeeds.
    int rc; 
    while ((rc = try_insert(cval)) == -1);
    return rc;
}

int EntryLockLRU::get(const std::string& key, std::string* val) {
    std::unique_lock<std::mutex> table_lock(table_lock_);
    LKVTable::iterator find_it = table_.find(key);
    if (find_it == table_.end()) {
        return -1;
    }
    LCVSharedPtr cval = find_it->second;

    // Acquire entry lock. Can now release the table lock.
    std::unique_lock<std::mutex> entry_lock(cval->lock_);
    table_lock.unlock();

    // Retrieve the value and update the last accessed time.
    *val = cval->val_;
    cval->last_accessed_ = std::chrono::high_resolution_clock::now();

    // Update position in the order list
    if (!cval->dirty_) {
        // Must acquire the order lock before changing the order list.
        std::unique_lock<std::mutex> order_lock(order_lock_); 
        order_.erase(cval->pos_);
        addToOrderList(cval);
    }
    return 0;
}

int EntryLockLRU::setClean(const std::string& key) {
    std::unique_lock<std::mutex> table_lock(table_lock_);
    LKVTable::iterator find_it = table_.find(key);
    if (find_it == table_.end()) {
        return -1;
    } 

    // If existing entry is not dirty, do nothing.
    LCVSharedPtr exist_val = find_it->second;
    std::unique_lock<std::mutex> entry_lock(exist_val->lock_);
    if (!exist_val->dirty_) {
        return 0;
    }

    // No longer need the table lock. Set clean, acquire the 
    // order lock, and add to the entry to the order list.
    table_lock.unlock();
    exist_val->dirty_ = false;
    std::unique_lock<std::mutex> order_lock(order_lock_);
    addToOrderList(exist_val);

    // Wake up any thread that might be waiting for it to become clean.
    (exist_val->cv_).notify_all();
    return 0;
}

int EntryLockLRU::evictOne() {
    std::unique_lock<std::mutex> order_lock(order_lock_);
    if (order_.empty()) {
        return -1; // Nothing to do
    }
    // Fetch the oldest entry. This will point to our eviction target.
    LCVSharedPtr cval = *(order_.begin());

    // To prevent deadlock, must release the order lock first before
    // acquiring the entry lock.
    order_lock.unlock();

    // Now we have a target to remove. Try to remove it. Need
    // to first acquire a table and an entry lock.
    std::unique_lock<std::mutex> table_lock(table_lock_);
    std::unique_lock<std::mutex> entry_lock(cval->lock_);

    // It is possible that the entry has been replaced. Issue
    // a find on the table to update the reference.
    LKVTable::iterator find_it = table_.find(cval->key_);
    if (find_it == table_.end()) {
        // Already removed by a different eviction thread.
        return -1;
    }

    // Entry has been replaced by an insert. If it has, we should just
    // return an error so a different entry can be evicted.
    if (cval != find_it->second) {
        return -1;
    }

    // Remove entry from the table. We can now release the table lock. 
    table_.erase(cval->key_);
    table_lock.unlock();

    // Since an entry can not transition from clean to dirty, and we know 
    // the entry has not changed, the entry should still be clean.
    assert(!cval->dirty_);

    // Remove from order list
    order_lock.lock();
    order_.erase(cval->pos_);
    return 0;
}

int EntryLockLRU::getOldestClean(hr_time* oldest_time) {
    std::unique_lock<std::mutex> table_lock(table_lock_);
    std::unique_lock<std::mutex> order_lock(order_lock_);
    if (order_.empty()) {
        return -1; // Nothing to do
    }
    // Fetch the oldest entry.
    LCVSharedPtr cval = *(order_.begin());

    // To prevent deadlock, must release the order lock first before
    // acquiring the entry lock.
    order_lock.unlock();
    std::unique_lock<std::mutex> entry_lock(cval->lock_);
    *oldest_time = cval->last_accessed_;
    return 0;
}
