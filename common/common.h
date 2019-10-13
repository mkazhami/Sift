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

#include "common/logging.h"
#include <string>

/* MEMORY SERVER DEFINITIONS*/
// This could be deduced from the config file, but the potential of a local memory node complicates it

// Number of memory servers to use
#define TOTAL_MEMORY_SERVERS 3
// Size of the quorum
#define QUORUM ((TOTAL_MEMORY_SERVERS/2) + 1)
// File containing memory server addresses and ports
#define CONFIG_FILE "/home/mkazhami/Sift/servers.config"

/* VARIOUS SYSTEM FLAGS */

// Number of threads to use for applying key-value store operations
#define KV_APPLY_THREADS 3
// Whether to co-locate a memory node with the coordinator
// This stores a full replica on the coordinator CPU node, requiring only 2F memory servers
// Good potential performance optimization, but has implications on the failure model
#define COORDINATOR_MEMORY_NODE false
// Whether to use an applied index for logging
// This is a legacy flag, should always be off
#define USE_APPLIED_INDEX false
// Whether to cache the key-value block addresses
// Experimental feature
#define CACHE_KEY_ADDRESS false
// Whether to cache replicated memory blocks
// Experimental feature
#define USE_RM_CACHE false
// Whether to use rocksdb as a secondary (persistent) storage
// Will use rocksdb snapshots for memory server recovery, assumes presence of network filesystem
#define USE_PERSISTENCE false

/* REPLICATED MEMORY DEFINITIONS */

// Index of local memory node (if COORDINATOR_MEMORY_NODE is set)
#define LOCAL_MEMORY_NODE 0

// Size of each memory block
#define RM_MEMORY_BLOCK_SIZE            1024ul
// Size of each log entry
#define RM_LOG_BLOCK_SIZE               (RM_MEMORY_BLOCK_SIZE + sizeof(uint64_t) + sizeof(uint32_t))
// Number of entries in the write-ahead log
#define RM_LOG_SIZE                     (2ul << 14)
// Total size of log
#define RM_LOG_MEMORY_SIZE              (RM_LOG_SIZE * RM_LOG_BLOCK_SIZE)

// Memory on the RM service is laid out as follows:
//   server_id (16 bits)     ‾|
//   term_id (16 bits)        | - 64 bit admin region for CAS operations
//   timestamp (32 bits)     _|
//   committed index (32 bits)
//   applied index (32 bits)
//   replicated memory block (RM_REPLICATED_MEMORY_SIZE bytes)
//   write-ahead log (RM_WRITE_AHEAD_LOG_SIZE bytes)
#define RM_SERVER_ID_TERM_ID_OFFSET     0ull
#define RM_TIMESTAMP_OFFSET             (RM_SERVER_ID_TERM_ID_OFFSET + sizeof(uint32_t))
#define RM_APPLIED_INDEX_OFFSET         (RM_TIMESTAMP_OFFSET + sizeof(uint32_t))
#define RM_REPLICATED_MEMORY_OFFSET     ((RM_APPLIED_INDEX_OFFSET + sizeof(uint32_t) + RM_MEMORY_BLOCK_SIZE) % RM_MEMORY_BLOCK_SIZE)
#define RM_WRITE_AHEAD_LOG_OFFSET       (RM_REPLICATED_MEMORY_OFFSET + RM_REPLICATED_MEMORY_SIZE)

#define RM_APPLIED_INDEX_SIZE           sizeof(uint32_t)


/* KEY VALUE STORE DEFINITIONS */

// Size of each key (bytes)
#define KV_KEY_SIZE                     32ul
// Size of next pointer (bytes)
#define KV_NEXT_PTR_SIZE                sizeof(uint64_t)
// Size of each value (bytes)
#define KV_VALUE_SIZE                   (RM_MEMORY_BLOCK_SIZE - KV_KEY_SIZE - KV_NEXT_PTR_SIZE)
// Size of write-ahead log blocks (bytes)
#define KV_LOG_BLOCK_SIZE               (KV_KEY_SIZE + KV_VALUE_SIZE + sizeof(uint32_t))
// Number of blocks in the write-ahead log
#define KV_LOG_SIZE                     (2ul << 15)

// Number of entries the key value store can hold
#define KV_SIZE                         (1000000)
// Number of entries the cache can hold (1/8 of the total size)
#define KV_CACHE_SIZE                   (KV_SIZE / 2)
// Size of index table
#define KV_INDEX_SIZE                   (KV_SIZE << 3)


// Total size of replicated memory
#define RM_REPLICATED_MEMORY_SIZE       (RM_MEMORY_BLOCK_SIZE + KV_INDEX_SIZE*sizeof(uint64_t) + KV_SIZE*RM_MEMORY_BLOCK_SIZE + KV_SIZE + KV_LOG_SIZE*KV_LOG_BLOCK_SIZE)
// Total memory used
#define RDMA_MEMORY_SIZE                (RM_MEMORY_BLOCK_SIZE + RM_REPLICATED_MEMORY_SIZE + RM_LOG_MEMORY_SIZE)


inline void printConfig() {
    LogInfo("System configuration:" << std::endl
            << "MEMORY_BLOCK_SIZE = " << RM_MEMORY_BLOCK_SIZE << " bytes" << std::endl
            << "RM_LOG_SIZE = " << RM_LOG_SIZE << " entries" << std::endl
            << "RM_REPLICATED_MEMORY_SIZE = " << (RM_REPLICATED_MEMORY_SIZE >> 20) << " MB" << std::endl
            << "KV_LOG_SIZE = " << KV_LOG_SIZE << " entries" << std::endl
            << "KV_SIZE = " << KV_SIZE << " entries" << std::endl
            << "KV_CACHE_SIZE = " << KV_CACHE_SIZE << " entries" << std::endl);
}