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

#if USE_PERSISTENCE

#include "include/memory_replication/persistent_store.h"
#include "common/common.h"
#include "common/logging.h"
#include "rocksdb/write_batch.h"

#include <fstream>

const std::string PersistentStore::rmDbPath = "/ssd1/mkazhami/db/coordinator_rm";
const std::string PersistentStore::rmBackupDbPath = "/ssd1/mkazhami/db/coordinator_rm_backup";
const std::string PersistentStore::logDbPath = "/ssd1/mkazhami/db/coordinator_log";


PersistentStore::PersistentStore() {
    if (!USE_PERSISTENCE) return;
    options.create_if_missing = true;
    // Push all writes to disk before completing
    writeOptions.sync = true;
    // Unsure of whether this helps performance
    options.IncreaseParallelism(4);
    rocksdb::Status s = rocksdb::DB::Open(options, rmDbPath, &rmDb);
    LogAssert(s.ok(), "Failed to open RM DB");
    s = rocksdb::Checkpoint::Create(rmDb, &checkpoint);
    LogAssert(s.ok(), "Failed to create checkpoint object");
}

PersistentStore::PersistentStore(std::string db) {
    if (!USE_PERSISTENCE) return;
    logDb = nullptr;
    options.create_if_missing = false;
    rocksdb::Status s = rocksdb::DB::Open(rocksdb::Options(), db, &rmDb);
    if (!s.ok()) {
        LogInfo("Could not open db " << db << " - might not exist");
        rmDb = nullptr;
    }
}

PersistentStore::~PersistentStore() {
    if (!USE_PERSISTENCE) return;
    LogInfo("Cleaning up databases...");
    rocksdb::Status s;
    if (rmDb != nullptr) {
        delete rmDb;
        s = rocksdb::DestroyDB(rmDbPath, options);
        LogAssert(s.ok(), "Failed to destroy RM DB");
    }
    // Might fail if backup db does not exist
    rocksdb::DestroyDB(rmBackupDbPath, options);
}

PersistentStore* PersistentStore::openSnapshot() {
    PersistentStore *ps = new PersistentStore(rmBackupDbPath);
    return ps;
}

void PersistentStore::closeSnapshot() {
    delete rmDb;
}

void PersistentStore::holdWrites() {
    //LogInfo("Holding writes");
    checkpointLock.lock();
}

void PersistentStore::continueWrites() {
    //LogInfo("Continuing writes");
    checkpointLock.unlock();
}

std::string PersistentStore::createValue(char *value, size_t length) {
    std::string logValue(value, length);
    return logValue;
}

void PersistentStore::write(uint64_t commitIndex, uint64_t address, char *value, size_t length) {
    std::string logValue = createValue(value, length);
    rocksdb::Status s = rmDb->Put(writeOptions, std::to_string(address), logValue);
    LogAssert(s.ok(), "Failed to write to RM db");
}

void PersistentStore::write(uint64_t commitIndex,
                            const std::vector<uint64_t> &addresses,
                            const std::vector<char *> &values,
                            const std::vector<size_t> &lengths) {
    rocksdb::WriteBatch rmBatch;
    for (uint32_t i = 0; i < addresses.size(); i++) {
        uint64_t address = addresses[i];
        char *value = values[i];
        size_t length = lengths[i];

        std::string logValue = createValue(value, length);
        // Assume that commit indices were obtained in sequence
        rmBatch.Put(std::to_string(address), logValue);
    }
    rocksdb::Status s = rmDb->Write(writeOptions, &rmBatch);
    LogAssert(s.ok(), "Failed to write batch to rm DB");
}

void PersistentStore::write(std::vector<std::pair<uint64_t, std::shared_ptr<RequestProcessor>>> writes) {
    rocksdb::WriteBatch rmBatch;
    for (auto writeEntry : writes) {
        uint64_t commitIndex = writeEntry.first;
        auto requestProcessor = writeEntry.second;
        std::vector<uint64_t>& addresses = requestProcessor->getAddresses();
        std::vector<char *>& values = requestProcessor->getValues();
        std::vector<size_t>& lengths = requestProcessor->getValueSizes();

        for (uint32_t i = 0; i < addresses.size(); i++) {
            uint64_t address = addresses[i];
            char *value = values[i];
            size_t length = lengths[i];

            std::string logValue = createValue(value, length);
            // Assume that commit indices were obtained in sequence
            rmBatch.Put(std::to_string(address), logValue);
        }
    }
    rocksdb::Status s = rmDb->Write(writeOptions, &rmBatch);
    LogAssert(s.ok(), "Failed to write batch to rm DB");
}

void PersistentStore::createSnapshot() {
    LogDebug("Creating snapshot");
    // Might fail if backup db does not exist
    // TODO: create checkpoint, THEN delete old checkpoint
    rocksdb::DestroyDB(rmBackupDbPath, options);
    rocksdb::Status s = checkpoint->CreateCheckpoint(rmBackupDbPath);
    LogAssert(s.ok(), "Failed to create snapshot");
}

std::vector<std::pair<uint64_t, std::string>> PersistentStore::scanTable() {
    std::vector<std::pair<uint64_t, std::string>> table;

    if (rmDb != nullptr) {
        rocksdb::Iterator *it = rmDb->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            table.push_back(std::make_pair(std::stoull(it->key().ToString()), it->value().ToString()));
        }
        assert(it->status().ok()); // Check for any errors found during the scan
        delete it;
    }

    return table;
};

#endif
