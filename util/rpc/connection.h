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

#include <string>
#include "buffer.h"

#define BUFFER_SIZE 1024 * 100

class Connection {
  protected:
    int sock;  // Socket for this connection

    // Read and write buffers for this connection.
    // TODO: Buffers should be allocated from a pool.
    Buffer rdBuf; 
    Buffer wrBuf;

    // Read a string from the buffer. The string uses length-prefixed encoding.
    // <int32_t, string>, where the length is in network order. This method
    // will allocate a new string that must be later deleted by the caller.
    // The method also assumes that save points and rollbacks are handled by
    // the caller. Returns 0 on success, -1 on failure.
    int extractStr(Buffer* in_buf, std::string** out_str);

    // Copies message to the write buffer. Returns 0 on success, -1 on failure.
    int copyToWriteBuf(const char* msg, int size);

  public:
    Connection(int sock) : sock(sock), rdBuf(BUFFER_SIZE), wrBuf(BUFFER_SIZE) {}
    virtual ~Connection() {}

    // Returns the socket number.
    int getSocket() const     { return sock; }

    // Used to send a message on this connection. Data will be copied to a 
    // write buffer if it cannot be sent immediately. Returns 0 on success,
    // -1 on failure.
    int sendMsg(const char* msg, int size);

    // Returns whether there is still any data in the write buffer.
    bool hasBufferedWrites()  { return wrBuf.len() > 0; }

    // Sends data from the write buffer. Returns 0 on success, -1 on failure.
    int flushWriteBuf();

    // Used to receive messages from the socket. Will call handleMsg for each
    // message that it receives. Must be used with a non-blocking socket.
    // Returns 0 on success, -1 on failure.
    int recvMsgs();

    // Handles a single message. Must be implemented by the derived class.
    virtual int handleMsg() = 0;
};
