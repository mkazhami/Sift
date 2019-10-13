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

#include <unordered_map>
#include "connection.h"
#include "include/kv_store/kv_server.h"

#define READ_REQUEST    1
#define WRITE_REQUEST   2
#define WRITE_OK        1000

class KVConnection : public Connection {
  private:
    KVServer* kv;
    int handleReadRequest(const std::string& key);
    int handleWriteRequest(const std::string& key, const std::string& val);

  public:
    KVConnection(KVServer* kv, int sock) : Connection(sock), kv(kv) {}
    virtual ~KVConnection(){}

    // Hardcoded message format. Should probably use something
    // more structured in the future, such as protobuf.
    // For read requests:
    //   <int (READ_REQUEST), int (key size), string (key)>
    // For write requests:
    //   <int (WRITE_REQUEST), int (key size), string (key),
    //    int (val size), string(val)>
    //
    // TODO: Currently everything is hardcoded. For this implementation
    //       to be more general, an application should instead be able to
    //       register callbacks based on a messag type.
    virtual int handleMsg();
};
