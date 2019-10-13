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

#include "include/util/serializable_address_value_pair.h"
#include <cstring>

SerializableAVPair::SerializableAVPair() {}

SerializableAVPair::~SerializableAVPair() {}

char* SerializableAVPair::serialize() {
    char* frame = new char[sizeof(AVPair)];
    memcpy(frame, &value, sizeof(AVPair)); // copy memory and not string
    return frame;
}

void SerializableAVPair::deserialize(char *data) {
    memcpy(&value, data, sizeof(AVPair));
}

uint64_t SerializableAVPair::getAddress() {
    return value.address;
}

void SerializableAVPair::setAddress(uint64_t address) {
    value.address = address;
}

char *SerializableAVPair::getValue() {
    return value.value;
}

void SerializableAVPair::setValue(const char* new_value, size_t len) {
    memcpy(value.value, new_value, len);
}

uint32_t SerializableAVPair::getLogIndex() {
    return value.log_index;
}

void SerializableAVPair::setLogIndex(uint32_t log_index) {
    value.log_index = log_index;
}

bool SerializableAVPair::equals(SerializableAVPair &sv) {
    return value.address == sv.value.address
           && strcmp(value.value, sv.value.value);
}