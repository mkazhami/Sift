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

class Buffer {
  private:
    char* buf;      // Pointer to the underlying buffer.
    int cap;        // Maximum size of the buffer
    int rd_idx;     // Read index
    int wr_idx;     // Write index
    int save_pt;    // Save point for read index

  public:
    Buffer(int max_size);
    ~Buffer();

    // Acquire space in the buffer. This space can be used as the
    // buffer for a recv call. Increments the write index by "size".
    // Return 0 on success, -1 on failure.
    int acquire(char** ptr, int size);

    // Did not use all of the acquired buffer. Occurs when recv 
    // receives less bytes than the amount that was requested.
    // Decrements the write index by "size". Return 0 on success, 
    // -1 on failure.
    int releaseUnused(int size);

    // Provides access to data starting from the read index. Returns 
    // the number of bytes that can be read (up to size). Increments 
    // the read index by the return value.
    int read(char** ptr, int size);

    // Moves the read index forward by size. Returns the number of
    // bytes that was incremented, or -1 on error.
    int forwardReadIndex(int size);

    // Returns the amount of readable data in the buffer.
    int len() const          { return wr_idx - rd_idx; }

    // Returns the available space in the buffer.
    int capacity() const     { return cap - wr_idx;    }

    // Saves the read index to a save point, which allows the read
    // index to be rolled back to the saved location later if
    // the buffer doesn't contain enough data to unmarshal the
    // entire structure.
    //
    // NOTE: Only supports a single save point. Save points are not
    //       preserved after certain calls, such as moveDataToFront.
    void setReadSavePoint()  { save_pt = rd_idx;       }
    void rollbackReadIndex() { rd_idx = save_pt;       }

    // Move the data in the buffer to the front of the buffer.
    // The read index will be set to 0 afterwards.
    //
    // NOTE: This will clear the read save point
    void moveDataToFront();

    // Reset the buffer to its initial state.
    // NOTE: This will clear the read save point
    void reset();
};

