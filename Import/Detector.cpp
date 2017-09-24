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
 * @file Detector.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for Importer class
 */

#include "Detector.h"

#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"

#include "DelimitedSupport.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

void Detector::init() {
  detect_row_delimiter();
  split_raw_data();
  find_best_sqltypes_and_headers();
}

void Detector::read_file() {
  if (!boost::filesystem::exists(file_path)) {
    LOG(ERROR) << "File does not exist: " << file_path;
    return;
  }
  std::ifstream infile(file_path.string());
  std::string line;
  auto end_time = std::chrono::steady_clock::now() + timeout;
  try {
    while (std::getline(infile, line, copy_params.line_delim)) {
      raw_data += line;
      raw_data += copy_params.line_delim;
      if (std::chrono::steady_clock::now() > end_time) {
        break;
      }
    }
  } catch (std::exception& e) {
  }
  infile.close();
}

void Detector::detect_row_delimiter() {
  if (copy_params.delimiter == '\0') {
    copy_params.delimiter = ',';
    if (boost::filesystem::extension(file_path) == ".tsv") {
      copy_params.delimiter = '\t';
    }
  }
}

void Detector::split_raw_data() {
  const char* buf = raw_data.c_str();
  const char* buf_end = buf + raw_data.size();
  bool try_single_thread = false;
  for (const char* p = buf; p < buf_end; p++) {
    std::vector<std::string> row;
    p = get_row(p, buf_end, buf_end, copy_params, true, nullptr, row, try_single_thread);
    raw_rows.push_back(row);
    if (try_single_thread) {
      break;
    }
  }
  if (try_single_thread) {
    copy_params.threads = 1;
    raw_rows.clear();
    for (const char* p = buf; p < buf_end; p++) {
      std::vector<std::string> row;
      p = get_row(p, buf_end, buf_end, copy_params, true, nullptr, row, try_single_thread);
      raw_rows.push_back(row);
    }
  }
}

template <class T>
bool try_cast(const std::string& str) {
  try {
    boost::lexical_cast<T>(str);
  } catch (const boost::bad_lexical_cast& e) {
    return false;
  }
  return true;
}

inline char* try_strptimes(const char* str, const std::vector<std::string>& formats) {
  std::tm tm_struct;
  char* buf;
  for (auto format : formats) {
    buf = strptime(str, format.c_str(), &tm_struct);
    if (buf) {
      return buf;
    }
  }
  return nullptr;
}

// TODO(renesugar): Add a specialization for bool to use boost::lexical_cast?
//                  try_cast<bool> is commented out below.
/*
 Example: (would need to check for "t", "f", "true", "false")

 namespace boost {
  template<>
  bool lexical_cast<bool, std::string>(const std::string& arg) {
    std::istringstream ss(arg);
    bool b;
    ss >> std::boolalpha >> b;
    return b;
  }

  template<>
  std::string lexical_cast<std::string, bool>(const bool& b) {
    std::ostringstream ss;
    ss << std::boolalpha << b;
    return ss.str();
  }
 }
 */

SQLTypes Detector::detect_sqltype(const std::string& str) {
  SQLTypes type = kTEXT;
  if (try_cast<double>(str)) {
    type = kDOUBLE;
    // if (try_cast<bool>(str)) {
    // type = kBOOLEAN;
    // } else
    if (try_cast<int16_t>(str)) {
      type = kSMALLINT;
    } else if (try_cast<int32_t>(str)) {
      type = kINT;
    } else if (try_cast<int64_t>(str)) {
      type = kBIGINT;
    } else if (try_cast<float>(str)) {
      type = kFLOAT;
    }
  }

  // see StringToDatum in Shared/Datum.cpp
  if (type == kTEXT) {
    char* buf;
    buf = try_strptimes(str.c_str(), {"%Y-%m-%d", "%m/%d/%Y", "%d-%b-%y", "%d/%b/%Y"});
    if (buf) {
      type = kDATE;
      if (*buf == 'T' || *buf == ' ' || *buf == ':') {
        buf++;
      }
    }
    buf = try_strptimes(buf == nullptr ? str.c_str() : buf, {"%T %z", "%T", "%H%M%S", "%R"});
    if (buf) {
      if (type == kDATE) {
        type = kTIMESTAMP;
      } else {
        type = kTIME;
      }
    }
  }
  return type;
}

std::vector<SQLTypes> Detector::detect_column_types(const std::vector<std::string>& row) {
  std::vector<SQLTypes> types(row.size());
  for (size_t i = 0; i < row.size(); i++) {
    types[i] = detect_sqltype(row[i]);
  }
  return types;
}

bool Detector::more_restrictive_sqltype(const SQLTypes a, const SQLTypes b) {
  static std::array<int, kSQLTYPE_LAST> typeorder;
  typeorder[kCHAR] = 0;
  typeorder[kBOOLEAN] = 2;
  typeorder[kSMALLINT] = 3;
  typeorder[kINT] = 4;
  typeorder[kBIGINT] = 5;
  typeorder[kFLOAT] = 6;
  typeorder[kDOUBLE] = 7;
  typeorder[kTIMESTAMP] = 8;
  typeorder[kTIME] = 9;
  typeorder[kDATE] = 10;
  typeorder[kTEXT] = 11;

  // note: b < a instead of a < b because the map is ordered most to least restrictive
  return typeorder[b] < typeorder[a];
}

void Detector::find_best_sqltypes_and_headers() {
  best_sqltypes = find_best_sqltypes(raw_rows.begin() + 1, raw_rows.end(), copy_params);
  best_encodings = find_best_encodings(raw_rows.begin() + 1, raw_rows.end(), best_sqltypes);
  std::vector<SQLTypes> head_types = detect_column_types(raw_rows.at(0));
  has_headers = detect_headers(head_types, best_sqltypes);
  copy_params.has_header = has_headers;
}

void Detector::find_best_sqltypes() {
  best_sqltypes = find_best_sqltypes(raw_rows.begin(), raw_rows.end(), copy_params);
}

std::vector<SQLTypes> Detector::find_best_sqltypes(
                        const std::vector<std::vector<std::string>>& raw_rows,
                        const CopyParams& copy_params) {
  return find_best_sqltypes(raw_rows.begin(), raw_rows.end(), copy_params);
}

std::vector<SQLTypes> Detector::find_best_sqltypes(
        const std::vector<std::vector<std::string>>::const_iterator& row_begin,
        const std::vector<std::vector<std::string>>::const_iterator& row_end,
        const CopyParams& copy_params) {
  if (raw_rows.size() < 1) {
    throw std::runtime_error("No rows found in: " + boost::filesystem::basename(file_path));
  }
  auto end_time = std::chrono::steady_clock::now() + timeout;
  size_t num_cols = raw_rows.front().size();
  std::vector<SQLTypes> best_types(num_cols, kCHAR);
  std::vector<size_t> non_null_col_counts(num_cols, 0);
  for (auto row = row_begin; row != row_end; row++) {
    while (best_types.size() < row->size() || non_null_col_counts.size() < row->size()) {
      best_types.push_back(kCHAR);
      non_null_col_counts.push_back(0);
    }
    for (size_t col_idx = 0; col_idx < row->size(); col_idx++) {
      // do not count nulls
      if (row->at(col_idx) == "" || !row->at(col_idx).compare(copy_params.null_str))
        continue;
      SQLTypes t = detect_sqltype(row->at(col_idx));
      non_null_col_counts[col_idx]++;
      if (!more_restrictive_sqltype(best_types[col_idx], t)) {
        best_types[col_idx] = t;
      }
    }
    if (std::chrono::steady_clock::now() > end_time) {
      break;
    }
  }
  for (size_t col_idx = 0; col_idx < num_cols; col_idx++) {
    // if we don't have any non-null values for this column make it text to be
    // safe b/c that is least restrictive type
    if (non_null_col_counts[col_idx] == 0)
      best_types[col_idx] = kTEXT;
  }

  return best_types;
}

std::vector<EncodingType> Detector::find_best_encodings(
        const std::vector<std::vector<std::string>>::const_iterator& row_begin,
        const std::vector<std::vector<std::string>>::const_iterator& row_end,
        const std::vector<SQLTypes>& best_types) {
  if (raw_rows.size() < 1) {
    throw std::runtime_error("No rows found in: " + boost::filesystem::basename(file_path));
  }
  size_t num_cols = best_types.size();
  std::vector<EncodingType> best_encodes(num_cols, kENCODING_NONE);
  std::vector<size_t> num_rows_per_col(num_cols, 1);
  std::vector<std::unordered_set<std::string>> count_set(num_cols);
  for (auto row = row_begin; row != row_end; row++) {
    for (size_t col_idx = 0; col_idx < row->size(); col_idx++) {
      if (IS_STRING(best_types[col_idx])) {
        count_set[col_idx].insert(row->at(col_idx));
        num_rows_per_col[col_idx]++;
      }
    }
  }
  for (size_t col_idx = 0; col_idx < num_cols; col_idx++) {
    if (IS_STRING(best_types[col_idx])) {
      float uniqueRatio = static_cast<float>(count_set[col_idx].size()) / num_rows_per_col[col_idx];
      if (uniqueRatio < 0.75) {
        best_encodes[col_idx] = kENCODING_DICT;
      }
    }
  }
  return best_encodes;
}

void Detector::detect_headers() {
  has_headers = detect_headers(raw_rows);
}

bool Detector::detect_headers(const std::vector<std::vector<std::string>>& raw_rows) {
  if (raw_rows.size() < 3) {
    return false;
  }
  std::vector<SQLTypes> head_types = detect_column_types(raw_rows.at(0));
  std::vector<SQLTypes> tail_types = find_best_sqltypes(raw_rows.begin() + 1, raw_rows.end(), copy_params);
  return detect_headers(head_types, tail_types);
}

// detect_headers returns true if:
// - all elements of the first argument are kTEXT
// - there is at least one instance where tail_types is more restrictive than head_types (ie, not kTEXT)
bool Detector::detect_headers(
                              const std::vector<SQLTypes>& head_types,
                              const std::vector<SQLTypes>& tail_types) {
  if (head_types.size() != tail_types.size()) {
    return false;
  }
  bool has_headers = false;
  for (size_t col_idx = 0; col_idx < tail_types.size(); col_idx++) {
    if (head_types[col_idx] != kTEXT) {
      return false;
    }
    has_headers = has_headers || tail_types[col_idx] != kTEXT;
  }
  return has_headers;
}

std::vector<std::vector<std::string>> Detector::get_sample_rows(size_t n) {
  n = std::min(n, raw_rows.size());
  size_t offset = (has_headers && raw_rows.size() > 1) ? 1 : 0;
  std::vector<std::vector<std::string>> sample_rows(raw_rows.begin() + offset, raw_rows.begin() + n);
  return sample_rows;
}

std::vector<std::string> Detector::get_headers() {
  std::vector<std::string> headers(best_sqltypes.size());
  for (size_t i = 0; i < best_sqltypes.size(); i++) {
    headers[i] = has_headers ? raw_rows[0][i] : "column_" + std::to_string(i + 1);
  }
  return headers;
}

}  // namespace Importer_NS
