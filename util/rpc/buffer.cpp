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
#include <stdlib.h> 
#include <string.h>

#include "buffer.h"

Buffer::Buffer(int max_size) : cap(max_size), rd_idx(0), wr_idx(0), save_pt(0) {
    buf = (char*) malloc(cap);
    assert(buf != NULL); // TODO: Throw exception
}

Buffer::~Buffer() {
    free(buf);
}

// TODO: Should automatically double in size if over capacity.
int Buffer::acquire(char** ptr, int size) {
    if (wr_idx + size > cap) {
        return -1;
    }
    *ptr = &buf[wr_idx];
    wr_idx += size;
    return 0;
}

int Buffer::releaseUnused(int size) {
    if (size > wr_idx - rd_idx) {
        return -1;
    }
    wr_idx -= size;
    return 0;
}

// Returns the number of bytes read 
int Buffer::read(char** ptr, int size) {
    *ptr = &buf[rd_idx];
    if (rd_idx + size > wr_idx) {
        // Adjust size if it is too big
        size = wr_idx - rd_idx;
    }
    rd_idx += size;
    return size;
}

int Buffer::forwardReadIndex(int size) {
    if (rd_idx + size > wr_idx) {
        return -1;  // Cannot move past write index. 
    }
    rd_idx += size;
    return size;
}

void Buffer::moveDataToFront() {
    int prev_size = len();
    if (prev_size == 0) {
        reset();
    } else if (rd_idx > 0) {
        memmove(buf, &buf[rd_idx], prev_size);
        rd_idx = 0;
        save_pt = 0;
        wr_idx = prev_size;
    }
}

void Buffer::reset() {
    rd_idx = 0;
    wr_idx = 0;
    save_pt = 0;
}

