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

#include "include/kv_store/data_entry.h"

#include <assert.h>

DataEntry::DataEntry() {
    memset(&entry, 0, sizeof(entry));
}

void DataEntry::setKey(const char *key) {
    memcpy(entry.key, key, strlen(key)+1);
}

void DataEntry::setValue(const char *value) {
    assert(strlen(value)+1 < sizeof(entry.value));
    memcpy(entry.value, value, strlen(value)+1);
}

void DataEntry::setNextPtr(uint64_t next_ptr) {
    entry.next_ptr = next_ptr;
}

char *DataEntry::getKey() {
    return entry.key;
}

char *DataEntry::getValue() {
    return entry.value;
}

uint64_t DataEntry::getNextPtr() {
    return entry.next_ptr;
}

char *DataEntry::serialize() {
    char *serialized = new char[sizeof(entry)];
    memcpy(serialized, &entry, sizeof(entry));
    return serialized;
}
void DataEntry::deserialize(const char *data) {
    memcpy(&entry, data, sizeof(entry));
}