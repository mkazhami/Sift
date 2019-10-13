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

#include "common/common.h"

#include <stdint.h>
#include <array>
#include <atomic>


/*
 *  An index of 0 means there is nothing there. Since the index table exists before the data table in memory,
 *  this is a safe assumption to make.
 */
class IndexTable {
public:
    IndexTable();
    ~IndexTable();

    void setIndex(uint32_t, uint64_t);
    void clearIndex(uint32_t);
    uint64_t getIndex(uint32_t);
    uint64_t *getPointer(uint32_t);

    void lock(uint32_t);
    void unlock(uint32_t);

private:
    std::mutex mtx[KV_INDEX_SIZE];
    std::array< std::atomic<uint64_t>, KV_INDEX_SIZE> table;
};