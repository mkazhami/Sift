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

#include "include/kv_store/log_entry.h"
#include <assert.h>

LogEntry::LogEntry() {}

void LogEntry::setLogIndex(uint32_t log_index) {
    entry.log_index = log_index;
}

void LogEntry::setKey(const char *key) {
    size_t slen = strlen(key)+1;
    assert(slen <= KV_KEY_SIZE);
    memcpy(entry.key, key, slen);
}

void LogEntry::setValue(const char *value) {
    size_t slen = strlen(value)+1;
    assert(slen <= KV_VALUE_SIZE);
    memcpy(entry.value, value, slen);
}

uint32_t LogEntry::getLogIndex() {
    return entry.log_index;
}

char *LogEntry::getKey() {
    return entry.key;
}

char *LogEntry::getValue() {
    return entry.value;
}

char *LogEntry::serialize() {
    char *serialized = new char[sizeof(entry)];
    memcpy(serialized, &entry, sizeof(entry));
    return serialized;
}
void LogEntry::deserialize(const char *data) {
    memcpy(&entry, data, sizeof(entry));
}
