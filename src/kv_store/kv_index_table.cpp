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

#include "include/kv_store/kv_index_table.h"
#include "../../include/kv_store/kv_index_table.h"

IndexTable::IndexTable() {}

IndexTable::~IndexTable() {}

void IndexTable::setIndex(uint32_t i, uint64_t index) {
    table[i] = index;
}

void IndexTable::clearIndex(uint32_t i) {
    table[i] = 0;
}

uint64_t IndexTable::getIndex(uint32_t i) {
    return table[i];
}

uint64_t *IndexTable::getPointer(uint32_t i) {
    return (uint64_t*)&table[i];
}

void IndexTable::readLock(uint32_t i) {
    //mtx[i].lock();
    lockTable.readLock(i);
}

void IndexTable::readUnlock(uint32_t i) {
    //mtx[i].unlock();
    lockTable.readUnlock(i);
}

void IndexTable::writeLock(uint32_t i) {
    //mtx[i].lock();
    lockTable.writeLock(i);
}

void IndexTable::writeUnlock(uint32_t i) {
    //mtx[i].unlock();
    lockTable.writeUnlock(i);
}

void IndexTable::restore(char *data) {
    memcpy(table.data(), data, sizeof(uint64_t) * KV_INDEX_SIZE);
}