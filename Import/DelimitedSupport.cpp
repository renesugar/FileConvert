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
 * @file DelimitedSupport.cpp
 * @author Wei Hong <wei@mapd.com>
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for Importer class
 */

#include "DelimitedSupport.h"

#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/crc.hpp>

#include <string>
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
#include <thread>
#include <future>
#include <mutex>

#include "../Shared/SanitizeName.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

std::string make_mutex_name(std::string str) {
  boost::crc_32_type result;
  result.process_bytes(str.data(), str.length());
  return std::to_string(result.checksum()) + "_mapd_mutex";
}

const char* get_row(const char* buf,
                          const char* buf_end,
                          const char* entire_buf_end,
                          const CopyParams& copy_params,
                          bool is_begin,
                          const bool* is_array,
                          std::vector<std::string>& row,
                          bool& try_single_thread) {
  const char* field = buf;
  const char* p;
  bool in_quote = false;
  bool in_array = false;
  bool has_escape = false;
  bool strip_quotes = false;
  try_single_thread = false;
  std::string line_endings({copy_params.line_delim, '\r', '\n'});
  for (p = buf; p < entire_buf_end; p++) {
    if (*p == copy_params.escape && p < entire_buf_end - 1 && *(p + 1) == copy_params.quote) {
      p++;
      has_escape = true;
    } else if (copy_params.quoted && *p == copy_params.quote) {
      in_quote = !in_quote;
      if (in_quote)
        strip_quotes = true;
    } else if (!in_quote && is_array != nullptr && *p == copy_params.array_begin && is_array[row.size()]) {
      in_array = true;
    } else if (!in_quote && is_array != nullptr && *p == copy_params.array_end && is_array[row.size()]) {
      in_array = false;
    } else if (*p == copy_params.delimiter || is_eol(*p, line_endings)) {
      if (!in_quote && !in_array) {
        if (!has_escape && !strip_quotes) {
          std::string s = trim_space(field, p - field);
          row.push_back(s);
        } else {
          auto field_buf = std::unique_ptr<char[]>(new char[p - field + 1]);
          int j = 0, i = 0;
          for (; i < p - field; i++, j++) {
            if (has_escape && field[i] == copy_params.escape && field[i + 1] == copy_params.quote) {
              field_buf[j] = copy_params.quote;
              i++;
            } else {
              field_buf[j] = field[i];
            }
          }
          std::string s = trim_space(field_buf.get(), j);
          if (copy_params.quoted && s.size() > 0 && s.front() == copy_params.quote) {
            s.erase(0, 1);
          }
          if (copy_params.quoted && s.size() > 0 && s.back() == copy_params.quote) {
            s.pop_back();
          }
          row.push_back(s);
        }
        field = p + 1;
        has_escape = false;
        strip_quotes = false;
      }
      if (is_eol(*p, line_endings) && ((!in_quote && !in_array) || copy_params.threads != 1)) {
        while (p + 1 < buf_end && is_eol(*(p + 1), line_endings)) {
          p++;
        }
        break;
      }
    }
  }
  /*
   @TODO(wei) do error handling
   */
  if (in_quote) {
    LOG(ERROR) << "Unmatched quote.";
    try_single_thread = true;
  }
  if (in_array) {
    LOG(ERROR) << "Unmatched array.";
    try_single_thread = true;
  }
  return p;
}

const std::string trim_space(const char* field, const size_t len) {
  size_t i = 0;
  size_t j = len;
  while (i < j && (field[i] == ' ' || field[i] == '\r')) {
    i++;
  }
  while (i < j && (field[j - 1] == ' ' || field[j - 1] == '\r')) {
    j--;
  }
  return std::string(field + i, j - i);
}

const bool is_eol(const char& p, const std::string& line_delims) {
  for (auto i : line_delims) {
    if (p == i) {
      return true;
    }
  }
  return false;
}

int8_t* appendDatum(int8_t* buf, Datum d, const SQLTypeInfo& ti) {
  switch (ti.get_type()) {
    case kBOOLEAN:
      *reinterpret_cast<bool*>(buf) = d.boolval;
      return buf + sizeof(bool);
    case kNUMERIC:
    case kDECIMAL:
    case kBIGINT:
      *reinterpret_cast<int64_t*>(buf) = d.bigintval;
      return buf + sizeof(int64_t);
    case kINT:
      *reinterpret_cast<int32_t*>(buf) = d.intval;
      return buf + sizeof(int32_t);
    case kSMALLINT:
      *reinterpret_cast<int16_t*>(buf) = d.smallintval;
      return buf + sizeof(int16_t);
    case kFLOAT:
      *reinterpret_cast<float*>(buf) = d.floatval;
      return buf + sizeof(float);
    case kDOUBLE:
      *reinterpret_cast<double*>(buf) = d.doubleval;
      return buf + sizeof(double);
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      *reinterpret_cast<time_t*>(buf) = d.timeval;
      return buf + sizeof(time_t);
    default:
      return NULL;
  }
  return NULL;
}

ArrayDatum StringToArray(const std::string& s, const SQLTypeInfo& ti,
                         const CopyParams& copy_params) {
  SQLTypeInfo elem_ti = ti.get_elem_type();
  if (s[0] != copy_params.array_begin || s[s.size() - 1] != copy_params.array_end) {
    LOG(WARNING) << "Malformed array: " << s;
    return ArrayDatum(0, NULL, true);
  }
  std::vector<std::string> elem_strs;
  size_t last = 1;
  for (size_t i = s.find(copy_params.array_delim, 1); i != std::string::npos;
       i = s.find(copy_params.array_delim, last)) {
    elem_strs.push_back(s.substr(last, i - last));
    last = i + 1;
  }
  if (last + 1 < s.size()) {
    elem_strs.push_back(s.substr(last, s.size() - 1 - last));
  }
  if (!elem_ti.is_string()) {
    size_t len = elem_strs.size() * elem_ti.get_size();
    int8_t* buf = reinterpret_cast<int8_t*>(checked_malloc(len));
    int8_t* p = buf;
    for (auto& e : elem_strs) {
      Datum d = StringToDatum(e, elem_ti);
      p = appendDatum(p, d, elem_ti);
    }
    return ArrayDatum(len, buf, len == 0);
  }
  // must not be called for array of strings
  CHECK(false);
  return ArrayDatum(0, NULL, true);
}

void parseStringArray(const std::string& s, const CopyParams& copy_params,
                      std::vector<std::string>& string_vec) {
  if (s == copy_params.null_str || s.size() < 1) {
    return;
  }
  if (s[0] != copy_params.array_begin || s[s.size() - 1] != copy_params.array_end) {
    throw std::runtime_error("Malformed Array :" + s);
  }
  size_t last = 1;
  for (size_t i = s.find(copy_params.array_delim, 1); i != std::string::npos;
       i = s.find(copy_params.array_delim, last)) {
    if (i > last) {  // if not empty string - disallow empty strings for now
      if (s.substr(last, i - last).length() > StringDictionary::MAX_STRLEN)
        throw std::runtime_error("Array String too long : " +
                                 std::to_string(s.substr(last, i - last).length()) +
                                 " max is " + std::to_string(StringDictionary::MAX_STRLEN));

      string_vec.push_back(s.substr(last, i - last));
    }
    last = i + 1;
  }
  if (s.size() - 1 > last) {  // if not empty string - disallow empty strings for now
    if (s.substr(last, s.size() - 1 - last).length() > StringDictionary::MAX_STRLEN)
      throw std::runtime_error("Array String too long : " +
                               std::to_string(s.substr(last, s.size() - 1 - last).length()) +
                               " max is " +
                               std::to_string(StringDictionary::MAX_STRLEN));

    string_vec.push_back(s.substr(last, s.size() - 1 - last));
  }
}

void addBinaryStringArray(const TDatum& datum,
                          std::vector<std::string>& string_vec) {
  const auto& arr = datum.val.arr_val;
  for (const auto& elem_datum : arr) {
    string_vec.push_back(elem_datum.val.str_val);
  }
}

Datum TDatumToDatum(const TDatum& datum, const SQLTypeInfo& ti) {
  Datum d;
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();
  switch (type) {
    case kBOOLEAN:
      d.boolval = datum.is_null ? inline_fixed_encoding_null_val(ti) : datum.val.int_val;
      break;
    case kBIGINT:
      d.bigintval = datum.is_null ? inline_fixed_encoding_null_val(ti) : datum.val.int_val;
      break;
    case kINT:
      d.intval = datum.is_null ? inline_fixed_encoding_null_val(ti) : datum.val.int_val;
      break;
    case kSMALLINT:
      d.smallintval = datum.is_null ? inline_fixed_encoding_null_val(ti) : datum.val.int_val;
      break;
    case kFLOAT:
      d.floatval = datum.is_null ? NULL_FLOAT : datum.val.real_val;
      break;
    case kDOUBLE:
      d.doubleval = datum.is_null ? NULL_DOUBLE : datum.val.real_val;
      break;
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      d.timeval = datum.is_null ? inline_fixed_encoding_null_val(ti) : datum.val.int_val;
      break;
    default:
      throw std::runtime_error("Internal error: invalid type in StringToDatum.");
  }
  return d;
}

ArrayDatum TDatumToArrayDatum(const TDatum& datum, const SQLTypeInfo& ti) {
  SQLTypeInfo elem_ti = ti.get_elem_type();
  CHECK(!elem_ti.is_string());
  size_t len = datum.val.arr_val.size() * elem_ti.get_size();
  int8_t* buf = reinterpret_cast<int8_t*>(checked_malloc(len));
  int8_t* p = buf;
  for (auto& e : datum.val.arr_val) {
    p = appendDatum(p, TDatumToDatum(e, elem_ti), elem_ti);
  }
  return ArrayDatum(len, buf, len == 0);
}

size_t find_beginning(const char* buffer, size_t begin, size_t end,
                      const CopyParams& copy_params) {
  // @TODO(wei) line_delim is in quotes note supported
  if (begin == 0 || (begin > 0 && buffer[begin - 1] == copy_params.line_delim))
    return 0;
  size_t i;
  const char* buf = buffer + begin;
  for (i = 0; i < end - begin; i++)
    if (buf[i] == copy_params.line_delim)
      return i + 1;
  return i;
}

size_t find_end(const char* buffer, size_t size,
                const CopyParams& copy_params) {
  int i;
  // @TODO(wei) line_delim is in quotes note supported
  for (i = size - 1; i >= 0 && buffer[i] != copy_params.line_delim; i--)
    ;

  if (i < 0)
    LOG(ERROR) << "No line delimiter in block.";
  return i + 1;
}

ColumnDescriptor create_array_column(const SQLTypes& type,
                                     const std::string& name) {
  ColumnDescriptor cd;
  cd.columnName = name;
  SQLTypeInfo ti;
  ti.set_type(kARRAY);
  ti.set_subtype(type);
  ti.set_fixed_size();
  cd.columnType = ti;
  return cd;
}

std::list<const ColumnDescriptor*> getAllColumnMetadataForDelimited(
                       const std::string file_name, CopyParams&  copy_params) {
  std::list<const ColumnDescriptor*> columnDescriptors;

  Importer_NS::Detector detector(file_name, copy_params);
  std::vector<SQLTypes> best_types = detector.best_sqltypes;
  std::vector<EncodingType> best_encodings = detector.best_encodings;
  std::vector<std::string> headers = detector.get_headers();
  copy_params = detector.get_copy_params();

  for (size_t col_idx = 0; col_idx < best_types.size(); col_idx++) {
    SQLTypes t = best_types[col_idx];
    EncodingType encodingType = best_encodings[col_idx];
    SQLTypeInfo ti(t, false, encodingType);

    ColumnDescriptor* cd = new ColumnDescriptor();

    // initialize ColumnDescriptor

    cd->tableId = 0;
    cd->columnId = 0;
    cd->columnName = sanitize_name(headers[col_idx]);
    cd->sourceName = sanitize_name(headers[col_idx]);

    cd->columnType = ti;

    // cd->chunks;
    cd->isSystemCol  = false;
    cd->isVirtualCol = false;
    // cd->virtualExpr;

    columnDescriptors.push_back(cd);
  }

  return columnDescriptors;
}

}  // namespace Importer_NS
