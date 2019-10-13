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

#include <assert.h>
#include <string>
#include <unordered_map>
#include <arpa/inet.h>
#include "kv_connection.h"

int KVConnection::handleReadRequest(const std::string& key) {
    std::string value = kv->get(key);

    /*string_map::iterator find_it = kv->find(key);
    if (find_it != kv->end()) {
       value = find_it->second;
    } else {
       value = "";  // This is not a good error value 
    }*/
    int32_t size = htonl(value.size());
    if (sendMsg((char*)&size, sizeof(int32_t)) == -1) {
        DIE("Failed to send value size");
    }
    if (sendMsg(value.data(), value.size()) == -1) {
        DIE("Failed to send value");
    }
    return 0;
}

int KVConnection::handleWriteRequest(const std::string& key, 
                                     const std::string& val) {
    //(*kv)[key] = val;
    kv->put(key, val);
    int32_t ack = htonl(WRITE_OK);
    int ret = sendMsg((char*)&ack, sizeof(int32_t));
    if (ret == -1) {
        DIE("Failed to send ack");
    }
    return ret;
}

int KVConnection::handleMsg() {
    rdBuf.setReadSavePoint();
    char* buf;
    // Read the request type.
    if (rdBuf.read(&buf, sizeof(int32_t)) != sizeof(int32_t)) {
        rdBuf.rollbackReadIndex();
        return 1;
    }
    int req_type = ntohl(*(int32_t*)buf);
    // Determine if it is a Read or Write request
    int rc = -1;
    if (req_type == READ_REQUEST) {
        std::string* key_str;
        if (extractStr(&rdBuf, &key_str) == -1) {
            // Not enough data available to extract the string
            rdBuf.rollbackReadIndex();
            return 1;
        }
        assert(key_str != NULL); // TODO: Handle more gracefully
        rc = handleReadRequest(*key_str);
        delete key_str;
    } else if (req_type == WRITE_REQUEST) {
        std::string* key_str;
        std::string* val_str;
        if (extractStr(&rdBuf, &key_str) == -1) {
            // Not enough data available to extract the string
            rdBuf.rollbackReadIndex();
            return 1;
        }
        assert(key_str != NULL); // TODO: Handle more gracefully
        if (extractStr(&rdBuf, &val_str) == -1) {
            delete key_str;
            rdBuf.rollbackReadIndex();
            return 1;
        }
        assert(val_str != NULL); // TODO: Handle more gracefully
        rc = handleWriteRequest(*key_str, *val_str);
        delete key_str;
        delete val_str;
    } else {
        assert(false);
    }
    return rc;
}


