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

#include "include/kv_store/remote_server.h"
#include "include/client/remote_client.h"
#include "common/common.h"

#include <random>

const int num_keys = KV_SIZE;

int getOp() {
    static thread_local std::default_random_engine generator;
    std::uniform_int_distribution<int> intDistribution(0,99);
    return intDistribution(generator);
}

int main(int argc, char **argv) {
    if (argc < 5) {
        printf("Usage: %s server_addr server_port numOps readProb\n", argv[0]);
        return -1;
    }

    printConfig();

    std::string server_addr(argv[1]);
    int server_port = std::stoi(argv[2]);
    int num_ops = std::stoi(argv[3]);
    int read_prob = std::stoi(argv[4]);

    LogInfo("Starting test");
    RemoteClient client(server_addr, server_port);

    LogInfo("Populating store with " << num_keys << " values...");
    // Populate the store with values
    for (int i = 0; i < num_keys; i++) {
        std::string key("keykeykey" + std::to_string(i));
        std::string value("this is a test value " + std::to_string(i));
        client.put(key, value);
    }
    LogInfo("Done populating kv store");

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(1,num_keys-1);

    LogInfo("Running workload...");
    uint64_t completed_gets = 0;
    uint64_t completed_puts = 0;

    for (int i = 0; i < num_ops; i++) {
        int op = getOp();
        std::string key("keykeykey" + std::to_string(dist(rng)));

        if (op < read_prob) {
            client.get(key);
            completed_gets++;
        } else {
            std::string value("this is a test string " + std::to_string(i));
            client.put(key, value);
            completed_puts++;
        }
    }

    LogInfo("Result: " << completed_gets << " gets, " << completed_puts << " puts");
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}