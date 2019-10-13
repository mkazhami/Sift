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

#include <mutex>
#include <thread>
#include <iostream>
#include <string.h>
#include <chrono>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define THREAD_SYNC_START { std::lock_guard< std::mutex > lock( sync_mutex() );
#define THREAD_SYNC_STOP }

inline std::mutex& sync_mutex() {
    static std::mutex m;
    return m;
}

inline std::string timestamp() {
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
    std::time_t tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
    gmtime_r(&tt, &gmt);
    std::chrono::duration<double> fractional_seconds =
            (tp - std::chrono::system_clock::from_time_t(tt)) + std::chrono::seconds(gmt.tm_sec);
    std::string buffer("hr:mn:sc.xxxxxx");
    sprintf(&buffer.front(), "%02d:%02d:%08.6f", gmt.tm_hour, gmt.tm_min, fractional_seconds.count());
    return buffer;
}

inline void stack_trace() {
    void *array[10];
    int size = backtrace(array, 10);
    backtrace_symbols_fd(array, size, STDERR_FILENO);
}

#define DEBUG 1
#define LEVEL 3
#define LogMsg(Level, SEVERITY, x) {\
            do {\
                if (DEBUG && Level >= LEVEL) {\
                    THREAD_SYNC_START\
                    std::string ts = timestamp();\
                    std::cerr << ts << " [" << SEVERITY << "] " << "[thread: "\
                              << (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 100000) << "] "\
                              << __FILENAME__ << ":" << __LINE__ << " (" << __FUNCTION__ << "):  ";\
                    std::cerr << x << std::endl;\
                    THREAD_SYNC_STOP\
                }\
            } while(false);\
        }

#define LogError(x) LogMsg(4, "ERROR", x)
#define LogDebug(x) LogMsg(0, "DEBUG", x)
#define LogInfo(x) LogMsg(3, "INFO", x)
#define LogWarning(x) LogMsg(2, "WARNING", x)
#define LogAssertionError(x) { LogMsg(4, "ASSERT ERROR", x); }
#define LogAssert(COND, x) if (!(COND)) { LogAssertionError(x); stack_trace(); }

#define DIE(x) { LogMsg(5, "DIE", x); stack_trace(); exit(EXIT_FAILURE); }

