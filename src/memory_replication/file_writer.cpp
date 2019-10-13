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

#include <unistd.h>
#include <common/test.h>
#include <fcntl.h>
#include "include/memory_replication/file_writer.h"

FileWriter::FileWriter() {}

FileWriter::~FileWriter() {}

int FileWriter::append(const char *data, u_int32_t size) const {
    long len = write(fd, data, size);
    if (len != size) {
        LogError("Error writing " << data << " of length " << size);
    }
    fsync(fd);
    return (int) len;
}

int FileWriter::open_file(const char *path) {
    fd = open(path, O_WRONLY | O_APPEND | O_CREAT, S_IRWXU);
    if (-1 == fd) {
        LogError("Open() failed with error");
        return -1;
    } else {
        LogError("Open() Successful");
        return 0;
    }
}