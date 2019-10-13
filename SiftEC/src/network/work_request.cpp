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

#include "include/network/work_request.h"
#include <assert.h>

WorkRequest::WorkRequest() {
    reset();
}

WorkRequest::~WorkRequest() {
}

int WorkRequest::done() {
    return done_votes.fetch_add(1);
}

bool WorkRequest::isDone() {
    return done_votes.load() == TOTAL_MEMORY_SERVERS;
}

int WorkRequest::prepare() {
    return total_votes.fetch_add(1);
}

void WorkRequest::vote(int nodeId) {
    votes[nodeId] = 1;
}

void WorkRequest::reset() {
    total_votes.store(0);
    done_votes.store(0);
    size = 0;
    valueReady.store(false);
    service = nullptr;
    write_value = nullptr;
    originalRequest = nullptr;
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        votes[i] = 0;
        rdma_buffer_offset[i] = -1;
        value[i] = nullptr;
    }
    requestType = NONE_REQUEST_TYPE;
    workRequestCommand = NONE_REQUEST_COMMAND;
}

void WorkRequest::resetVote(int nodeId) {
    total_votes.store(0);
    done_votes.store(0);
    votes[nodeId] = 0;
    valueReady.store(false);
}

int WorkRequest::getVotes() {
    int v = 0;
    for (int i = 0; i < TOTAL_MEMORY_SERVERS; i++) {
        v += votes[i];
    }
    return v;
}

char **WorkRequest::getValue() {
    return value;
}

char *WorkRequest::getWriteValue() {
    return write_value;
}

void WorkRequest::setWriteValue(char *write_value) {
    this->write_value = write_value;
}

void WorkRequest::copyValue(int nodeId, char *value) {
    char *temp = new char[size];
    std::memcpy(temp, value, size);
    this->value[nodeId] = temp;
}

bool WorkRequest::ready() {
    int count = getVotes();
    return count >= QUORUM;
}

void WorkRequest::copyValueACS(char *value) {
    std::memcpy(&buf_ACS, value, sizeof(uint64_t));
}

void WorkRequest::setOffset(uint64_t offset) {
    this->offset = offset;
}

uint64_t WorkRequest::getOffset() {
    return offset;
}

void WorkRequest::setRDMABufferOffset(int nodeId, uint64_t offset) {
    rdma_buffer_offset[nodeId] = offset;
}

uint64_t WorkRequest::getRDMABufferOffset(int nodeId) {
    return rdma_buffer_offset[nodeId];
}

std::shared_ptr<RequestProcessor> WorkRequest::getOriginalRequest() {
    return originalRequest;
}

void WorkRequest::setOriginalRequest(std::shared_ptr<RequestProcessor> originalRequest) {
    this->originalRequest = originalRequest;
}

WorkRequestType WorkRequest::getRequestType() {
    return requestType;
}

void WorkRequest::setRequestType(WorkRequestType requestType) {
    this->requestType = requestType;
}

void WorkRequest::setSize(size_t size) {
    this->size = size;
}

size_t WorkRequest::getSize() {
    return size;
}

void WorkRequest::setWriteSize(size_t size) {
    this->write_size = size;
}

size_t WorkRequest::getWriteSize() {
    return write_size;
}

void WorkRequest::setValue(int nodeId, char *value) {
    this->value[nodeId] = value;
}

uint64_t WorkRequest::getValueACS() {
    return buf_ACS;
}

int WorkRequest::getVote(int nodeId, WorkRequestType stage) {
    if (stage == stages[nodeId] && votes[nodeId] == 1) {
        return 1;
    }
    return 0;
}

void WorkRequest::setStage(int nodeId, WorkRequestType stage) {
    stages[nodeId] = stage;
}

WorkRequestCommand WorkRequest::getWorkRequestCommand() {
    return workRequestCommand;
}

void WorkRequest::setWorkRequestCommand(WorkRequestCommand wrc) {
    workRequestCommand = wrc;
}

uint64_t WorkRequest::getCompare() {
    return compare;
}

uint64_t WorkRequest::getSwap() {
    return swap;
}

void WorkRequest::setCompare(uint64_t comp) {
    compare = comp;
}

void WorkRequest::setSwap(uint64_t swap_) {
    swap = swap_;
}

void (*WorkRequest::getCallback())(WorkRequest*, int) {
    return callback;
}

void WorkRequest::setCallback(void (*f)(WorkRequest*, int)) {
    callback = f;
}

void *WorkRequest::getConnection(int node_id) {
    return conn[node_id];
}

void WorkRequest::setConnection(int node_id, void *conn) {
    this->conn[node_id] = conn;
}

uint32_t WorkRequest::getLogIndex() {
    return log_index;
}

void WorkRequest::setLogIndex(uint32_t log_index) {
    this->log_index = log_index;
}

bool WorkRequest::getManageConflictFlag() {
    return manage_conflict;
}

void WorkRequest::setManageConflictFlag(bool manage_conflict) {
    this->manage_conflict = manage_conflict;
}