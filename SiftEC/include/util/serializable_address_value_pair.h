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

#include <stdint.h>

#include "common/common.h"

class SerializableAVPair {
public:
    SerializableAVPair();
    ~SerializableAVPair();
    uint64_t getAddress();
    void setAddress(uint64_t address);
    char *getValue();
    void setValue(const char* value, size_t len);
    uint32_t getLogIndex();
    void setLogIndex(uint32_t log_index);
    char* serialize();
    void deserialize(char *data);
    struct AVPair {
        uint32_t log_index;
        uint64_t address;
        char value[RM_MEMORY_BLOCK_SIZE];
    };
    AVPair value;
    bool equals(SerializableAVPair &kv);
private:
};