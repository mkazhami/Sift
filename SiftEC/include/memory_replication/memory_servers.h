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

#include <stdio.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <chrono>
#include "common/logging.h"

class IPPortPair2 {
private:
    bool alive = false;
    pthread_mutex_t log_value_mutex;
public:
    std::string ip;
    std::string port;
    uint32_t lastTimeStamp = 0;
    uint32_t lastServerId = 0;
    uint32_t lastTermId = 0;

    bool isAlive() {
        bool r;
        pthread_mutex_lock(&log_value_mutex);
        r = alive;
        pthread_mutex_unlock(&log_value_mutex);
        return r;
    }

    void setAlive() {
        pthread_mutex_lock(&log_value_mutex);
        alive = true;
        pthread_mutex_unlock(&log_value_mutex);
    }

    void setDead() {
        pthread_mutex_lock(&log_value_mutex);
        alive = false;
        pthread_mutex_unlock(&log_value_mutex);
    }
};

typedef std::vector<IPPortPair2> ServerList;

class MemoryServers {
public:
    MemoryServers() {}

    ~MemoryServers() {}

    bool parse(const char *filename);

    const ServerList *get_servers() const {
        return &servers;
    }

    void calculateValues() {
        if (servers.size() == 0) {
            DIE("No memory servers found");
        }
        quorum = (int) (servers.size() / 2) + 1;
        total = (int) servers.size();
    }

    long getTotal() {
        return total;
    }

    ServerList servers;
    int quorum;
    int total;
};