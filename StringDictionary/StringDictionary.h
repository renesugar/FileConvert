/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef STRINGDICTIONARY_STRINGDICTIONARY_H
#define STRINGDICTIONARY_STRINGDICTIONARY_H

// TODO(renesugar): Stub implementation of StringDictionary (discard when reintegrating)

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <map>
#include <string>
#include <tuple>
#include <vector>

class StringDictionary {
 public:
  StringDictionary() noexcept;
  ~StringDictionary() noexcept;

  void getOrAddBulk(const std::vector<std::string>& string_vec, uint8_t* encoded_vec);
  void getOrAddBulk(const std::vector<std::string>& string_vec, uint16_t* encoded_vec);
  void getOrAddBulk(const std::vector<std::string>& string_vec, int32_t* encoded_vec);

  bool checkpoint() noexcept;

  static const int32_t INVALID_STR_ID = -1;
  static const size_t  MAX_STRLEN     = (1 << 15) - 1;
};

#endif  // STRINGDICTIONARY_STRINGDICTIONARY_H
