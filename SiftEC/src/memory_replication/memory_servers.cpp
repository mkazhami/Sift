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

#include "include/memory_replication/memory_servers.h"

bool MemoryServers::parse(const char *filename) {
    std::fstream fs;
    fs.open(filename, std::fstream::in);
    if (!fs) {
        std::cerr << "Error reading file: " << filename << std::endl;
        exit(1);
    }
    std::string line;
    int node_id = 0;
    while (getline(fs, line)) {
        std::stringstream ss(line);
        IPPortPair2 pair;
        pair.lastTimeStamp = 0;
        ss >> pair.ip >>  pair.port;
        if (ss) {
            std::cout << "Node ID: " << node_id++ << ", IP: " << pair.ip << ", Port: " << pair.port << std::endl;
            servers.push_back(pair);
        } else {
            std::cerr << "Error parsing: " << line << std::endl;
            exit(1);
        }
    }
    fs.close();
    calculateValues();
    return true;
}