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

#if USE_PERSISTENCE

#include <stdint.h>
#include <chrono>
#include <mutex>

#include "include/memory_replication/request_processor.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/checkpoint.h"

class PersistentStore {
public:
    PersistentStore();
    PersistentStore(std::string db);
    ~PersistentStore();

    static PersistentStore* openSnapshot();
    void closeSnapshot();
    std::vector<std::pair<uint64_t, std::string>> scanTable();
    void createSnapshot();

    void write(uint64_t commitIndex, uint64_t address, char* value, size_t length);
    void write(uint64_t commitIndexStart, const std::vector<uint64_t>& addresses, const std::vector<char*>& values, const std::vector<size_t>& lengths);
    void write(std::vector<std::pair<uint64_t, std::shared_ptr<RequestProcessor>>>);

    void holdWrites();
    void continueWrites();

    static const std::string rmDbPath; // "/ssd1/mkazhami/db/coordinator_rm"
    static const std::string rmBackupDbPath; // "/ssd1/mkazhami/db/coordinator_rm_backup"
    static const std::string logDbPath; // "/ssd1/mkazhami/db/coordinator_log"

private:
    std::string createValue(char *value, size_t length);

    rocksdb::DB* rmDb;
    rocksdb::Checkpoint *checkpoint;
    rocksdb::DB* logDb;
    rocksdb::Options options;
    rocksdb::WriteOptions writeOptions;

    std::mutex checkpointLock;
};

#endif