#include "include/util/serializable_address_value_pair.h"
#include <cstring>
#include <assert.h>

SerializableAVPair::SerializableAVPair() {}

SerializableAVPair::~SerializableAVPair() {}

char* SerializableAVPair::serialize() {
    char* frame = new char[sizeof(AVPair)];
    memcpy(frame, &value, sizeof(AVPair)); // copy memory and not string
    return frame;
}

void SerializableAVPair::deserialize(char *data) {
    memcpy(&value, data, sizeof(AVPair));
}

uint64_t SerializableAVPair::getAddress() {
    return value.address;
}

void SerializableAVPair::setAddress(uint64_t address) {
    value.address = address;
}

char *SerializableAVPair::getValue() {
    return value.value;
}

void SerializableAVPair::setValue(const char* new_value, size_t len) {
    memcpy(value.value, new_value, RM_MEMORY_BLOCK_SIZE);
}

uint32_t SerializableAVPair::getLogIndex() {
    return value.log_index;
}

void SerializableAVPair::setLogIndex(uint32_t log_index) {
    value.log_index = log_index;
}

bool SerializableAVPair::equals(SerializableAVPair &sv) {
    return value.address == sv.value.address
           && strcmp(value.value, sv.value.value);
}