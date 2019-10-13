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

#include "common/logging.h"
#include <string.h>
#include <stdint.h>

template<int I>
class Bitmap {
public:
    void lock() {
        mtx.lock();
    }

    void unlock() {
        mtx.unlock();
    }

    int getBit(uint32_t i) {
        uint32_t block = i / 8;
        uint32_t remainder = i % 8;
        return (bitmap[block] >> remainder) & 1;
    }

    void setBit(uint32_t i) {
        uint32_t block = i / 8;
        uint32_t remainder = i % 8;
        bitmap[block] |= (1UL << remainder);
    }

    void clearBit(uint32_t i) {
        uint32_t block = i / 8;
        uint32_t remainder = i % 8;
        bitmap[block] &= ~(1UL << remainder);
    }

    uint32_t setNextAvailableBit() {
        int startSearchIndex = nextSearchIndex - 1;
        while (nextSearchIndex != startSearchIndex) {
            if (getBit(nextSearchIndex) == 0) {
                setBit(nextSearchIndex);
                return nextSearchIndex;
            }

            nextSearchIndex = (nextSearchIndex == I-1) ? 0 : nextSearchIndex+1;
        }
        return -1;
    }

    uint8_t *toByteArray(size_t startBit, size_t numBytes) {
        // Assume startBit is 8 byte aligned
        LogAssert(startBit % 8 == 0, "startBit (" << startBit << ") is not a multiple of 8");
        LogAssert(startBit + 8*numBytes <= I, "startBit (" << startBit << ") and numBytes (" << numBytes << ") goes outside range of bitmap");
        uint8_t *byteArray = new uint8_t[numBytes];
        size_t block = startBit / 8;
        memcpy(byteArray, &bitmap[block], numBytes);
        return byteArray;
    }

    void fromByteArray(uint8_t *arr) {
        // TODO: implement this for the recovery/new coordinator process
    }

private:
    std::mutex mtx;

    // Use a char array to avoid annoying bit manipulation and memory management with std::bitset
    uint8_t bitmap[(I+7)/8];
    uint32_t nextSearchIndex = 0;
};
