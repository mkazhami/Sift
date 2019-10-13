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

#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>
#include <string>
#include <arpa/inet.h>
#include <assert.h>
#include "connection.h"

// Assume that save point have been set by the caller.
int Connection::extractStr(Buffer* in_buf, std::string** out_str) {
    char* buf;
    // Fetch the length field
    if (in_buf->read(&buf, sizeof(int32_t)) != sizeof(int32_t)) {
        return -1;
    }
    int msg_len = ntohl(*(int32_t*)buf);

    // Read the actual message
    if (in_buf->read(&buf, msg_len) != msg_len) {
        return -1;
    }
    *out_str = new std::string(buf, msg_len);
    return 0;
}

int Connection::copyToWriteBuf(const char* msg, int size) {
    char* buf;
    if (wrBuf.acquire(&buf, size) == -1) {
        return -1;
    }
    memcpy(buf, msg, size);
    return 0;
}

int Connection::sendMsg(const char* msg, int size) {
    if (flushWriteBuf() == -1) {
        return -1; // Cannot flush old content
    }

    // Write buffer is still not empty. To preserve write ordering,
    // just copy the message data to the write buffer.
    if (hasBufferedWrites()) {
        return copyToWriteBuf(msg, size);
    }

    // Write buffer is empty. Directly send the message.
    int rc = send(sock, msg, size, 0);
    if (rc == -1) {
        return -1;  // Connection is broken.
    }
    if (rc < size) {
        // Add the remaining bytes to the write buffer,
        return copyToWriteBuf(&msg[rc], size - rc);
    }
    return 0;
}

int Connection::flushWriteBuf() {
    if (!hasBufferedWrites()) {
        return 0;
    }
    // Save the read index. Will need to rollback to it if we are unable
    // to flush the entire buffer.
    wrBuf.setReadSavePoint();

    // Get a pointer to the read index of the buffer.
    char* buf;
    int buf_size = wrBuf.len();
    if (wrBuf.read(&buf, buf_size) == -1) {
        assert(false); // This should never fail.
    }
    // Try to send all of it.
    int rc = send(sock, buf, buf_size, 0);
    if (rc == -1) {
        return -1;  // Connection is broken.
    }
    if (rc < buf_size) {
        // Was only able to flush some of it. Change the read index so
        // that it is only incremented by the amount that was sent.
        wrBuf.rollbackReadIndex();
        wrBuf.forwardReadIndex(rc);
    }
    // Move everything to the front. Will reset the buffer to its original 
    // position if it is empty.
    wrBuf.moveDataToFront();
    return 0;
}

int Connection::recvMsgs() {
    // With a non-blocking socket, we should read until we completely drain
    // all of the available data from the socket.
    while (true) {
        // Acquire buffer for reads
        char* buf;
        int buf_size = rdBuf.capacity();
        if (rdBuf.acquire(&buf, buf_size) == -1) {
            assert(false); // Should never fail.
        }
        // Read from the socket
        int rc = recv(sock, buf, buf_size, 0);
        if (rc == -1) {
            if (errno == EAGAIN) {
                // No more data is available. Release unused buffer space.
                rdBuf.releaseUnused(buf_size);
                // Move buffer data to the front of the buffer
                // so we can read more in the next round.
                rdBuf.moveDataToFront();
                break;
            }
            // Just return -1 for other error cases.
            return -1;
        }
        if (rc == 0) {
            // Other side has closed the connection. 
            return -1;
        }

        // Release unused buffers for future reads
        rdBuf.releaseUnused(buf_size - rc);

        // Handle one or more messages
        while (true) {
            rc = handleMsg();
            if (rc == -1) {
                return -1; // Error handling the request
            } else if (rc == 1) {
                break;     // Need to read more data
            }
        }
        // Shift everything back to the front before the next iteration.
        rdBuf.moveDataToFront();
    }
    return 0;
}

