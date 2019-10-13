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

#include "include/memory_replication/coordinator_state.h"

CoordinatorState::CoordinatorState() : raft_state(INIT) {
    pthread_mutex_init(&wait_on_state, NULL);
    pthread_cond_init(&state_change, NULL);
}

CoordinatorState::~CoordinatorState() {
    pthread_mutex_destroy(&wait_on_state);
    pthread_cond_destroy(&state_change);
}

void CoordinatorState::change_state(const State &state) {
    pthread_mutex_lock(&wait_on_state);
    raft_state = state;
    pthread_cond_broadcast(&state_change);
    pthread_mutex_unlock(&wait_on_state);
}

void CoordinatorState::wait_ne(State state) {
    pthread_mutex_lock(&wait_on_state);
    while (raft_state == state) {
        pthread_cond_wait(&state_change, &wait_on_state);
    }
    pthread_mutex_unlock(&wait_on_state);
}

void CoordinatorState::wait_e(State state) {
    pthread_mutex_lock(&wait_on_state);
    while (raft_state != state) {
        pthread_cond_wait(&state_change, &wait_on_state);
    }
    pthread_mutex_unlock(&wait_on_state);
}

void CoordinatorState::wait_ge(State state) {
    pthread_mutex_lock(&wait_on_state);
    while (raft_state < state) {
        pthread_cond_wait(&state_change, &wait_on_state);
    }
    pthread_mutex_unlock(&wait_on_state);
}

CoordinatorState::State CoordinatorState::read_state() {
    State current_state;
    pthread_mutex_lock(&wait_on_state);
    current_state = raft_state;
    pthread_mutex_unlock(&wait_on_state);
    return current_state;
}