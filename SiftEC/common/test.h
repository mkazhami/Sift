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

#define TEST_NZ(x) {\
            do {\
                if ((x)) {\
                    LogError(strerror(errno));\
                    DIE("error: " #x " failed (returned non-zero)." );\
                }\
            } while (0);\
        }

#define TEST_Z(x) {\
            do {\
                if (!(x)) {\
                    LogError(strerror(errno));\
                    DIE("error: " #x " failed (returned zero/null).");\
                }\
            } while (0);\
        }
