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

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <queue>
#include <rdma/rdma_cma.h>

#include "include/network/rdma/rdma.h"
#include "include/network/work_request.h"
#include "util/queue.h"

class Coordinator {
/*
    You should only be concerned with the public section of this class, details on calling and return is in the HPP file
    NOTE:
    All calls are Synchronous for the time being via waiting for op to be complete, only one op per QP is tested and implemented

*/
public:
    /*
        1.  Coordinator(char *address, char *port);

            initializes required client and connection structs

        2.  ~Coordinator();

            called by C++ on disconnection, destroys related structs

        3.  int RdmaRead(int offset, int size, char* buffer);

            offset: in bytes the distance from top of the registered memory region
            size: size of read, please ensure it does not exceed the max value, no checks are in place to do so
            buffer: malloced buffer to which rdma copies read data into

        4. int RdmaWrite(int offset, int size, char* buffer);

            offset: in bytes the distance from top of the registered memory region
            size: size of write, please ensure it does not exceed the max value, no checks are in place to do so
            buffer: malloced buffer from which RDMA reads and writes on remote address specified

        5. int RdmaCompSwap(int offset, uint64_t compare, uint64_t swap);

            offset: bytes from top addr from where the read begins
            compare: uint_64 integer that we are comparing against, if this is equal value will be swapped
            swap: if condition is met by compare, this uint64_t will be replaced atomically
            buffer: value of existing value will always be copied in this buffer, please malloc

            untested cases, trying to overwrite not a valid int, will probably fail


            TO DO:
                add revoke mechanism and implement easier to use API with some caching and soft state code pushed to these API, will need some sort of message passing
                and/or timeouts to ensure revokes

        6. void callJoin();

            needs to be called to prevent connection from disconnecting if no work will be done and all threads will exit // primary use case is for testing

    */

    virtual ~Coordinator() {}
    virtual bool CreateConnection()=0;
    virtual int Read(WorkRequest* workReq, bool force=false)=0;
    virtual int Write(WorkRequest* workReq, bool force=false)=0;
    virtual int Write(std::vector<WorkRequest*> &workReqs, bool force=false)=0;
    virtual int CompSwap(WorkRequest* workReq, bool force=false)=0;

    std::mutex state_lock;
    std::atomic<node_state> state;
protected:
    int node_id;
};
