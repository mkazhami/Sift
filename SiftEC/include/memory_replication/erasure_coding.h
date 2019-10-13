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

#include <vector>
#include "cm256cc/cm256.h"
#include "common/common.h"

#define NUM_CHUNKS (TOTAL_MEMORY_SERVERS)
#define NUM_ORIGINAL_CHUNKS ((TOTAL_MEMORY_SERVERS / 2) + 1)
#define NUM_REDUNDANT_CHUNKS (TOTAL_MEMORY_SERVERS - NUM_ORIGINAL_CHUNKS)
#define CHUNK_SIZE ((RM_MEMORY_BLOCK_SIZE + NUM_ORIGINAL_CHUNKS - 1) / NUM_ORIGINAL_CHUNKS)


class ErasureCoding {
public:
    static void erasure_init();
    static char *rebuild_block(CM256::cm256_block *chunks, bool need_rebuild);
    static CM256::cm256_block *generate_chunks(const char *block);
    static uint64_t erasure_translate_address(uint64_t address);
private:
    static CM256 cm256;
    static CM256::cm256_encoder_params params;
};

