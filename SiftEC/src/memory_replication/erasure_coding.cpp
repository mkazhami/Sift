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

#include "include/memory_replication/erasure_coding.h"

#include <vector>

CM256 ErasureCoding::cm256;
CM256::cm256_encoder_params ErasureCoding::params;

void ErasureCoding::erasure_init() {
    params.BlockBytes = CHUNK_SIZE;
    params.OriginalCount = NUM_ORIGINAL_CHUNKS;
    params.RecoveryCount = NUM_REDUNDANT_CHUNKS;
}

char *ErasureCoding::rebuild_block(CM256::cm256_block *chunks, bool need_rebuild) {
    int err;
    if (need_rebuild) {
        if ((err = cm256.cm256_decode(params, chunks))) {
            DIE("Failed to decode, err = " << err);
        }
    }

    char *block = new char[RM_MEMORY_BLOCK_SIZE];
    for (int i = 0; i < NUM_ORIGINAL_CHUNKS; i++) {
        size_t copy_length = std::min(CHUNK_SIZE, RM_MEMORY_BLOCK_SIZE - i * CHUNK_SIZE);
        memcpy(block + i * CHUNK_SIZE, chunks[i].Block, copy_length);
    }

    return block;
}

CM256::cm256_block *ErasureCoding::generate_chunks(const char *block) {
    CM256::cm256_block *chunks = new CM256::cm256_block[NUM_CHUNKS];
    uint8_t *data_chunks = new uint8_t[NUM_CHUNKS * CHUNK_SIZE];

    memcpy(data_chunks, block, RM_MEMORY_BLOCK_SIZE);
    for (unsigned char i = 0; i < NUM_ORIGINAL_CHUNKS; i++) {
        //size_t copy_length = std::min(CHUNK_SIZE, RM_MEMORY_BLOCK_SIZE - i*CHUNK_SIZE);
        //memcpy(data_chunks + i*CHUNK_SIZE, block + i*CHUNK_SIZE, copy_length);
        chunks[i].Block = data_chunks + i * CHUNK_SIZE;
        chunks[i].Index = i;
    }

    int err;
    if ((err = cm256.cm256_encode(params, chunks, data_chunks + CHUNK_SIZE * NUM_ORIGINAL_CHUNKS))) {
        DIE("Failed to encode - err: " << err);
    }

    for (unsigned char i = NUM_ORIGINAL_CHUNKS; i < NUM_CHUNKS; i++) {
        chunks[i].Block = data_chunks + i * CHUNK_SIZE;
        chunks[i].Index = i;
    }

    return chunks;
}

uint64_t ErasureCoding::erasure_translate_address(uint64_t address) {
    return (address / NUM_ORIGINAL_CHUNKS);
}
