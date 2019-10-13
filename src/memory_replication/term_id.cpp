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

#include "include/memory_replication/term_id.h"

TermId::TermId() {
    pthread_mutex_init(&value_mutex, NULL);
}

TermId::~TermId() {
    pthread_mutex_destroy(&value_mutex);
}

uint32_t TermId::getTermId() {
    uint32_t t;
    pthread_mutex_lock(&value_mutex);
    t = termId;
    pthread_mutex_unlock(&value_mutex);
    return t;
}

bool TermId::setTermId(uint32_t termId) {
    bool result;
    pthread_mutex_lock(&value_mutex);
    if (termId >= TermId::termId) {
        result = true;
        TermId::termId = termId;
    } else {
        result = false;
    }
    pthread_mutex_unlock(&value_mutex);
    return result;
}

void TermId::increment() {
    pthread_mutex_lock(&value_mutex);
    TermId::termId++;
    pthread_mutex_unlock(&value_mutex);
}