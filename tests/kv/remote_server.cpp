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

#include <string>
#include <future>
#include <fstream>
#include "include/kv_store/remote_server.h"

RemoteKVServer *server = NULL;

int main(int argc, char **argv) {
    if (argc < 4) {
        printf("Usage: %s ip port server_id\n", argv[0]);
        return -1;
    }
    std::string funcName("main");
    std::stringstream msg;
    std::string host(argv[1]);
    int port = atoi(argv[2]);
    uint32_t serverId = (uint32_t) std::stoul(argv[3]);

    server = new RemoteKVServer(host, port, serverId);

    server->kvServer->kvCoordinator->status = KVCoordinator::LEADER;

    server->Run();

    std::promise<void>().get_future().wait();

    return 0;
}