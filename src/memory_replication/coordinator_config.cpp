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

#include "include/memory_replication/coordinator_config.h"

bool CoordinatorConfig::parse(const std::string &filename) {
    std::fstream fs;
    fs.open(filename.c_str(), std::fstream::in);
    if (!fs) {
        return false;
    }
    std::string line;
    while (getline(fs, line)) {
        std::stringstream ss(line);
        int node_id;
        struct IPPortPair pair;
        int type;
        ss >> node_id >> pair.ip >> pair.port >> type;
        switch (type) {
            case 1:
                pair.type = coordinator;
                break;
            case 2:
                pair.type = backup;
                break;
            case 3:
                pair.type = memory;
        }
        if (ss) {
            std::cout << "Node ID: " << node_id
                 << " IP: " << pair.ip
                 << " Port: " << pair.port
                 << " Type: " << pair.type << std::endl;
            servers[node_id] = pair;
        } else {
            std::cerr << "Error parsing: " << line << std::endl;
        }
    }
    fs.close();
    return true;
}