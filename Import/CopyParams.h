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

/*
 * @file CopyParams.h
 * @author Wei Hong <wei@mapd.com>
 * @brief Copy parameters for table import from file
 */
#ifndef _COPYPARAMS_H_
#define _COPYPARAMS_H_

#include <string>
#include <cstdio>
#include <cstdlib>

namespace Importer_NS {

enum class TableType { DELIMITED, POLYGON, ARROW };

struct CopyParams {
  char delimiter;
  std::string null_str;
  bool has_header;
  bool quoted;  // does the input have any quoted fields, default to false
  char quote;
  char escape;
  char line_delim;
  char array_delim;
  char array_begin;
  char array_end;
  int threads;
  size_t max_reject;  // maximum number of records that can be rejected
                      // before copy is failed
  TableType table_type;

  CopyParams()
  : delimiter(','),
  null_str(" "),
  has_header(true),
  quoted(true),
  quote('"'),
  escape('"'),
  line_delim('\n'),
  array_delim(','),
  array_begin('{'),
  array_end('}'),
  threads(0),
  max_reject(100000),
  table_type(TableType::DELIMITED) {}
};

};  // namespace Importer_NS
#endif  // _COPYPARAMS_H_
