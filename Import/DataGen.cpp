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

/**
 * @file    DataGen.cpp
 * @author  MapD
 * @author  Rene Sugar <rene.sugar@gmail.com>
 * @brief   MapD Client code for generating random data that can be
 * inserted into a given MapD table.
 *
 * Copyright (c) 2017 MapD Technologies, Inc.  All rights reserved.
 **/

#include "DataGen.h"

#define __STDC_WANT_LIB_EXT1__ 1
#include <time.h>

#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <cstdint>
#include <cfloat>
#include <random>
#include <ctime>
#include <limits>

// TODO(renesugar): 64-bit time conversion functions
//                  (Arrow, etc. use 64-bit time and date functions)
// https://stackoverflow.com/questions/7960318/math-to-convert-seconds-since-1970-into-date-and-vice-versa
// http://howardhinnant.github.io/date_algorithms.html
// https://howardhinnant.github.io/date/date.html
// https://github.com/HowardHinnant/date

namespace Importer_NS {

namespace {
// anonymous namespace for private functions
std::default_random_engine random_gen(std::random_device{}());

// returns a random integer as string

std::string gen_int8() {
  std::uniform_int_distribution<int8_t> dist(INT8_MIN, INT8_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_int16() {
  std::uniform_int_distribution<int16_t> dist(INT16_MIN, INT16_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_int32() {
  std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_int64() {
  std::uniform_int_distribution<int64_t> dist(INT64_MIN, INT64_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_uint8() {
  std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_uint16() {
  std::uniform_int_distribution<uint16_t> dist(0, UINT16_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_uint32() {
  std::uniform_int_distribution<uint32_t> dist(0, UINT32_MAX);
  return std::to_string(dist(random_gen));
}

std::string gen_uint64() {
  std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
  return std::to_string(dist(random_gen));
}

// returns a random float as string
std::string gen_float() {
  std::uniform_real_distribution<float> dist(std::numeric_limits<float>::lowest(),
                                             std::numeric_limits<float>::max());
  return std::to_string(dist(random_gen));
}

// returns a random double as string
std::string gen_double() {
  std::uniform_real_distribution<double> dist(std::numeric_limits<double>::lowest(),
                                             std::numeric_limits<double>::max());
  return std::to_string(dist(random_gen));
}

const int max_str_len   = 100;
const int max_array_len = 100;

// TODO(renesugar): For binary data, generate a random string with characters
//                  from 0 to 255.

// returns a random string of length up to max_str_len
std::string gen_string() {
  std::string chars("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
  std::uniform_int_distribution<> char_dist(0, chars.size() - 1);
  std::uniform_int_distribution<> len_dist(0, max_str_len);
  int len = len_dist(random_gen);
  std::string s(len, ' ');
  for (int i = 0; i < len; i++)
    s[i] = chars[char_dist(random_gen)];
  return s;
}

// returns a random array length up to max_array_len
int gen_array_len() {
  std::uniform_int_distribution<> len_dist(0, max_array_len);
  int len = len_dist(random_gen);
  return len;
}

// returns a random boolean as string
std::string gen_bool() {
  std::uniform_int_distribution<int> dist(0, 1);
  if (dist(random_gen) == 1)
    return "t";
  return "f";
}

// returns a random time as string
std::string gen_time() {
  std::uniform_int_distribution<int> dist(0, INT32_MAX);
  time_t t = dist(random_gen);
#ifdef __STDC_LIB_EXT1__
  struct tm tm_buf;
  struct tm* tm_ptr = gmtime_s(&t, &tm_buf));
#else
  // TODO(renesugar): Use gmtime_s on Windows and gmtime_r on Linux and Mac OS.
  // https://msdn.microsoft.com/en-us/library/3stkd9be.aspx
  std::tm* tm_ptr = gmtime(&t);
#endif
  char buf[9];
  if (tm_ptr == nullptr) {
    strncpy(buf, "12:12:12", sizeof(buf));
    buf[sizeof(buf)-1] = '\0';
  } else {
    strftime(buf, 9, "%T", tm_ptr);
  }
  return buf;
}

// returns a random timestamp as string
std::string gen_timestamp() {
  std::uniform_int_distribution<int> dist(0, INT32_MAX);
  time_t t = dist(random_gen);
#ifdef __STDC_LIB_EXT1__
  struct tm tm_buf;
  struct tm* tm_ptr = gmtime_s(&t, &tm_buf));
#else
  // TODO(renesugar): Use gmtime_s on Windows and gmtime_r on Linux and Mac OS.
  // https://msdn.microsoft.com/en-us/library/3stkd9be.aspx
  std::tm* tm_ptr = gmtime(&t);
#endif
  char buf[20];
  if (tm_ptr == nullptr) {
    strncpy(buf, "9999-12-12 12:12:12", sizeof(buf));
    buf[sizeof(buf)-1] = '\0';
  } else {
    strftime(buf, 20, "%F %T", tm_ptr);
  }
  return buf;
}

// returns a random date as string
std::string gen_date() {
  std::uniform_int_distribution<int> dist(0, INT32_MAX);
  time_t t = dist(random_gen);
#ifdef __STDC_LIB_EXT1__
  struct tm tm_buf;
  struct tm* tm_ptr = gmtime_s(&t, &tm_buf));
#else
  // TODO(renesugar): Use gmtime_s on Windows and gmtime_r on Linux and Mac OS.
  // https://msdn.microsoft.com/en-us/library/3stkd9be.aspx
  std::tm* tm_ptr = gmtime(&t);
#endif
  char buf[11];
  if (tm_ptr == nullptr) {
    strncpy(buf, "9999-12-12", sizeof(buf));
    buf[sizeof(buf)-1] = '\0';
  } else {
    strftime(buf, 11, "%F", tm_ptr);
  }
  return buf;
}

}  // namespace

// std::shared_ptr<::arrow::Column>& col
// ::arrow::Type::type type_id = col.type()->id();
// auto list_type = std::static_pointer_cast<::arrow::Type::ListType>(col.type());
// std::shared_ptr<::arrow::DataType> elem_type = list_type->value_type();
// elem_type_id = elem_type->id();
std::string GenerateArrowColumnValue(
                            ::arrow::Type::type type_id,
                            ::arrow::Type::type elem_type_id,
                            const CopyParams& cp) {
  switch (type_id) {
    case ::arrow::Type::NA:
      return cp.null_str;
    case ::arrow::Type::BOOL:
      return gen_bool();
    case ::arrow::Type::UINT8:
      return gen_uint8();
    case ::arrow::Type::INT8:
      return gen_int8();
    case ::arrow::Type::UINT16:
      return gen_uint16();
    case ::arrow::Type::INT16:
      return gen_int16();
    case ::arrow::Type::UINT32:
      return gen_uint32();
    case ::arrow::Type::INT32:
      return gen_int32();
    case ::arrow::Type::UINT64:
      return gen_uint64();
    case ::arrow::Type::INT64:
      return gen_int64();
    case ::arrow::Type::HALF_FLOAT:
    case ::arrow::Type::FLOAT:
      return gen_float();
    case ::arrow::Type::DOUBLE:
      return gen_double();
    case ::arrow::Type::STRING:
      return gen_string();
    case ::arrow::Type::BINARY:
    case ::arrow::Type::FIXED_SIZE_BINARY:
      // TODO(renesugar): Add escaped values outside of the printable range
      return gen_string();
    case ::arrow::Type::DATE32:
    case ::arrow::Type::DATE64:
      return gen_date();
    case ::arrow::Type::TIME32:
    case ::arrow::Type::TIME64:
	  return gen_time();
    case ::arrow::Type::INTERVAL:
	case ::arrow::Type::TIMESTAMP:
      return gen_timestamp();
    case ::arrow::Type::DECIMAL:
      return gen_uint64();
    case ::arrow::Type::LIST: {
      std::string strArray;
      bool is_first = true;
      int array_len = gen_array_len();

      // generate array

      strArray += cp.array_begin;

      for (int j = 0; j < array_len; j++) {
        if (is_first == false) {
          strArray += cp.array_delim;
        } else {
          is_first = false;
        }

        // TODO(renesugar): Generate some null values if column can contain nulls
        strArray += GenerateArrowColumnValue(elem_type_id, ::arrow::Type::NA, cp);
      }

      strArray += cp.array_end;

      return strArray;
    }
    case ::arrow::Type::STRUCT:
    case ::arrow::Type::UNION:
    case ::arrow::Type::DICTIONARY:
    default:
      return cp.null_str;
  }
}

std::vector<std::vector<std::string>> getSampleRowsForArrowSchema(
                                const std::shared_ptr<::arrow::Schema>& schema,
                                const CopyParams& cp,
                                size_t n) {
  std::vector<std::vector<std::string>> sample_rows;

  for (int j = 0; j < n; j++) {
    std::vector<std::string> row;

    for (int i = 0; i < schema->num_fields(); i++) {
      ::arrow::Type::type type_id = schema->field(i)->type()->id();
      ::arrow::Type::type elem_type_id = ::arrow::Type::STRING;

      if (type_id == ::arrow::Type::LIST) {
        auto list_type = static_cast<::arrow::ListType*>(schema->field(i)->type().get());
        std::shared_ptr<::arrow::DataType> elem_type = list_type->value_type();
        // get element data type of list
        elem_type_id = elem_type->id();
      }

      row.push_back(GenerateArrowColumnValue(type_id, elem_type_id, cp));
    }

    sample_rows.push_back(row);
  }

  return sample_rows;
}

}  // namespace Importer_NS

