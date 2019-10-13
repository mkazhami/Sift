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

#include "common/test.h"
#include "common/common.h"
#include <thread>
#include <string>

#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <assert.h>
#include <string>

class RemoteClient {

public:
    explicit RemoteClient(std::string host, int port) {
        if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            DIE("Unable to create socket");
        }

        int nodelay = 1;
        if (setsockopt(sockfd, SOL_TCP, TCP_NODELAY, (void *)&nodelay, sizeof(nodelay)) < 0 ) {
            DIE("Unable to set nodelay");
        }

        memset(&serv_addr, '0', sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);

        if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <=0) {
            DIE("Unable to get host address");
        }

        int err;
        do {
            err = connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
        } while (err < 0);
//        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
//            DIE("Unable to connect to server");
//        }
    }

    virtual ~RemoteClient() {

    }

    std::string get(const std::string &key) {
        // Send read request type
        int req_type = htonl(READ_REQUEST);
        if (!send_all(sockfd, &req_type, sizeof(int32_t))) {
            return "";
        }

        // Send key length
        int key_len = htonl(key.size());
        if (!send_all(sockfd, &key_len, sizeof(int32_t))) {
            return "";
        }

        // Send key
        if (!send_all(sockfd, key.data(), key.size())) {
            return "";
        }

        // Wait for value length
        int value_len = 0;
        if (!recv_all(sockfd, &value_len, sizeof(int32_t))) {
            return "";
        }
        value_len = ntohl(value_len);

        // Read value
        char *buf = new char[value_len];
        if (!recv_all(sockfd, buf, value_len)) {
            return "";
        }

        return std::string(buf);
    }

    int put(const std::string &key, const std::string &value) {
        // Send write request type
        int req_type = htonl(WRITE_REQUEST);
        if (!send_all(sockfd, &req_type, sizeof(int32_t))) {
            return -1;
        }

        // Send key length
        int key_len = htonl(key.size());
        if (!send_all(sockfd, &key_len, sizeof(int32_t))) {
            return -1;
        }

        // Send key
        if (!send_all(sockfd, key.data(), key.size())) {
            return -1;
        }

        // Send value length
        int value_len = htonl(value.size());
        if (!send_all(sockfd, &value_len, sizeof(int32_t))) {
            return -1;
        }

        // Send value
        if (!send_all(sockfd, value.data(), value.size())) {
            return -1;
        }

        // Wait for ack
        int ack;
        if (!recv_all(sockfd, &ack, sizeof(int32_t))) {
            return -1;
        }

        return 0;
    }

private:
    bool send_all(int socket, const void *buffer, size_t length) {
        char *ptr = (char*) buffer;
        while (length > 0) {
            int rc = send(socket, ptr, length, 0);
            if (rc <= 0) return false;
            ptr += rc;
            length -= rc;
        }
        return true;
    }

    bool recv_all(int socket, const void *buffer, size_t length) {
        char *ptr = (char*) buffer;
        while (length > 0) {
            int rc = recv(socket, ptr, length, 0);
            if (rc <= 0) return false;
            ptr += rc;
            length -= rc;
        }
        return true;
    }

    int sockfd = 0, n = 0;
    char buf[1024];
    struct sockaddr_in serv_addr;
};
