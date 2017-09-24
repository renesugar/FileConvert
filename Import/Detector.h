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
 * @file Detector.h
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for table import from file
 */
#ifndef _DETECTOR_H_
#define _DETECTOR_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>

#include <arrow/api.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <list>
#include <map>
#include <memory>

#include "../Shared/sqltypes.h"
#include "../Catalog/ColumnDescriptor.h"
#include "CopyParams.h"

namespace Importer_NS {

class Detector {
public:
  Detector(const boost::filesystem::path& fp, CopyParams& cp) : file_path(fp),
  copy_params(cp) {
    read_file();
    init();
  }
  static SQLTypes detect_sqltype(const std::string& str);
  std::vector<std::string> get_headers();
  const CopyParams& get_copy_params() const { return copy_params; }
  std::vector<std::vector<std::string>> raw_rows;
  std::vector<std::vector<std::string>> get_sample_rows(size_t n);
  std::vector<SQLTypes> best_sqltypes;
  std::vector<EncodingType> best_encodings;
  bool has_headers = false;

private:
  void init();
  void read_file();
  void detect_row_delimiter();
  void split_raw_data();
  std::vector<SQLTypes> detect_column_types(const std::vector<std::string>& row);
  static bool more_restrictive_sqltype(const SQLTypes a, const SQLTypes b);
  void find_best_sqltypes();
  std::vector<SQLTypes> find_best_sqltypes(
                        const std::vector<std::vector<std::string>>& raw_rows,
                        const CopyParams& copy_params);
  std::vector<SQLTypes> find_best_sqltypes(
        const std::vector<std::vector<std::string>>::const_iterator& row_begin,
        const std::vector<std::vector<std::string>>::const_iterator& row_end,
        const CopyParams& copy_params);

  std::vector<EncodingType> find_best_encodings(
        const std::vector<std::vector<std::string>>::const_iterator& row_begin,
        const std::vector<std::vector<std::string>>::const_iterator& row_end,
        const std::vector<SQLTypes>& best_types);

  void detect_headers();
  bool detect_headers(const std::vector<std::vector<std::string>>& raw_rows);
  bool detect_headers(const std::vector<SQLTypes>& first_types,
                      const std::vector<SQLTypes>& rest_types);
  void find_best_sqltypes_and_headers();
  std::string raw_data;
  boost::filesystem::path file_path;
  std::chrono::duration<double> timeout{1};
  CopyParams copy_params;
};

};  // namespace Importer_NS
#endif  // _DETECTOR_H_
