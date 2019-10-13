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

#include <pthread.h>

class CoordinatorState {
public:
    // Add more states as needed.
    enum State {
        INIT, FOLLOWER, CANDIDATE, LEADER_PREP, LEADER
    };
    CoordinatorState();
    ~CoordinatorState();
    // Change the internal state of CoordinatorState.
    void change_state(const State &state);
    // Wait until state equals the parameter.
    void wait_e(State state);
    void wait_ne(State state);
    // Wait until state is greater than or equal to the parameter.
    void wait_ge(State state);
    State read_state();
private:
    State raft_state;
    pthread_cond_t state_change;
    pthread_mutex_t wait_on_state;
};