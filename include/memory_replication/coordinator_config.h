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
#include <map>

enum ServerType {
    coordinator, backup, memory
};

struct IPPortPair {
    std::string ip;
    int port;
    ServerType type;
};

typedef std::map<int, struct IPPortPair> ServerMap;

class CoordinatorConfig {
private:
    ServerMap servers;

public:
    CoordinatorConfig() {}

    ~CoordinatorConfig() {}
    bool parse(const std::string& filename);
    const ServerMap* get_servers() const {
        return &servers;
    }
};