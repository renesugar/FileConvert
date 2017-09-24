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
 * @file TypedImportBuffer.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief TypedImportBuffer class for Importer class
 */

#include "TypedImportBuffer.h"

#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

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

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Import/DelimitedSupport.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

void TypedImportBuffer::add_value(const ColumnDescriptor* cd,
                                  const std::string& val,
                                  const bool is_null,
                                  const CopyParams& copy_params) {
  const auto type = cd->columnType.get_type();
  switch (type) {
    case kBOOLEAN: {
      if (is_null) {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addBoolean(inline_fixed_encoding_null_val(cd->columnType));
      } else {
        SQLTypeInfo ti = cd->columnType;
        Datum d = StringToDatum(val, ti);
        addBoolean((int8_t)d.boolval);
      }
      break;
    }
    case kSMALLINT: {
      if (!is_null && (isdigit(val[0]) || val[0] == '-')) {
        SQLTypeInfo ti = cd->columnType;
        Datum d = StringToDatum(val, ti);
        addSmallint(d.smallintval);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addSmallint(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    }
    case kINT: {
      if (!is_null && (isdigit(val[0]) || val[0] == '-')) {
        SQLTypeInfo ti = cd->columnType;
        Datum d = StringToDatum(val, ti);
        addInt(d.intval);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addInt(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    }
    case kBIGINT: {
      if (!is_null && (isdigit(val[0]) || val[0] == '-')) {
        SQLTypeInfo ti = cd->columnType;
        Datum d = StringToDatum(val, ti);
        addBigint(d.bigintval);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addBigint(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    }
    case kDECIMAL:
    case kNUMERIC: {
      if (!is_null) {
        SQLTypeInfo ti(kNUMERIC, 0, 0, false);
        Datum d = StringToDatum(val, ti);
        const auto converted_decimal_value = convert_decimal_value_to_scale(d.bigintval, ti, cd->columnType);
        addBigint(converted_decimal_value);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addBigint(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    }
    case kFLOAT:
      if (!is_null && (val[0] == '.' || isdigit(val[0]) || val[0] == '-')) {
        addFloat(static_cast<float>(std::atof(val.c_str())));
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addFloat(NULL_FLOAT);
      }
      break;
    case kDOUBLE:
      if (!is_null && (val[0] == '.' || isdigit(val[0]) || val[0] == '-')) {
        addDouble(std::atof(val.c_str()));
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addDouble(NULL_DOUBLE);
      }
      break;
    case kTEXT:
    case kVARCHAR:
    case kCHAR: {
      // @TODO(wei) for now, use empty string for nulls
      if (is_null) {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addString(std::string());
      } else {
        if (val.length() > StringDictionary::MAX_STRLEN)
          throw std::runtime_error("String too long for column " +
                                  cd->columnName + " was " +
                                  std::to_string(val.length()) + " max is " +
                                  std::to_string(StringDictionary::MAX_STRLEN));
        addString(val);
      }
      break;
    }
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      if (!is_null && (isdigit(val[0]) || val[0] == '-')) {
        SQLTypeInfo ti = cd->columnType;
        Datum d = StringToDatum(val, ti);
        addTime(d.timeval);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addTime(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    case kARRAY:
      if (is_null && cd->columnType.get_notnull()) {
        throw std::runtime_error("NULL for column " + cd->columnName);
      }
      if (IS_STRING(cd->columnType.get_subtype())) {
        std::vector<std::string>& string_vec = addStringArray();
        parseStringArray(val, copy_params, string_vec);
      } else {
        if (!is_null) {
          ArrayDatum d = StringToArray(val, cd->columnType, copy_params);
          addArray(d);
        } else {
          addArray(ArrayDatum(0, NULL, true));
        }
      }
      break;
    default:
      CHECK(false);
  }
}

void TypedImportBuffer::pop_value() {
  const auto type = column_desc_->columnType.is_decimal() ?
      decimal_to_int_type(column_desc_->columnType)
      : column_desc_->columnType.get_type();
  switch (type) {
    case kBOOLEAN:
      bool_buffer_->pop_back();
      break;
    case kSMALLINT:
      smallint_buffer_->pop_back();
      break;
    case kINT:
      int_buffer_->pop_back();
      break;
    case kBIGINT:
      bigint_buffer_->pop_back();
      break;
    case kFLOAT:
      float_buffer_->pop_back();
      break;
    case kDOUBLE:
      double_buffer_->pop_back();
      break;
    case kTEXT:
    case kVARCHAR:
    case kCHAR:
      string_buffer_->pop_back();
      break;
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      time_buffer_->pop_back();
      break;
    case kARRAY:
      if (IS_STRING(column_desc_->columnType.get_subtype())) {
        string_array_buffer_->pop_back();
      } else {
        array_buffer_->pop_back();
      }
      break;
    default:
      CHECK(false);
  }
}

void TypedImportBuffer::add_value(const ColumnDescriptor* cd,
                                  const TDatum& datum, const bool is_null) {
  const auto type = cd->columnType.is_decimal() ?
              decimal_to_int_type(cd->columnType) : cd->columnType.get_type();
  switch (type) {
    case kBOOLEAN: {
      if (is_null) {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addBoolean(inline_fixed_encoding_null_val(cd->columnType));
      } else {
        addBoolean((int8_t)datum.val.int_val);
      }
      break;
    }
    case kSMALLINT:
      if (!is_null) {
        addSmallint((int16_t)datum.val.int_val);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addSmallint(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    case kINT:
      if (!is_null) {
        addInt((int32_t)datum.val.int_val);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addInt(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    case kBIGINT:
      if (!is_null) {
        addBigint(datum.val.int_val);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addBigint(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    case kFLOAT:
      if (!is_null) {
        addFloat(static_cast<float>(datum.val.real_val));
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addFloat(NULL_FLOAT);
      }
      break;
    case kDOUBLE:
      if (!is_null) {
        addDouble(datum.val.real_val);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addDouble(NULL_DOUBLE);
      }
      break;
    case kTEXT:
    case kVARCHAR:
    case kCHAR: {
      // @TODO(wei) for now, use empty string for nulls
      if (is_null) {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addString(std::string());
      } else {
        addString(datum.val.str_val);
      }
      break;
    }
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      if (!is_null) {
        addTime((time_t)datum.val.int_val);
      } else {
        if (cd->columnType.get_notnull())
          throw std::runtime_error("NULL for column " + cd->columnName);
        addTime(inline_fixed_encoding_null_val(cd->columnType));
      }
      break;
    case kARRAY:
      if (is_null && cd->columnType.get_notnull()) {
        throw std::runtime_error("NULL for column " + cd->columnName);
      }
      if (IS_STRING(cd->columnType.get_subtype())) {
        std::vector<std::string>& string_vec = addStringArray();
        addBinaryStringArray(datum, string_vec);
      } else {
        if (!is_null) {
          addArray(TDatumToArrayDatum(datum, cd->columnType));
        } else {
          addArray(ArrayDatum(0, NULL, true));
        }
      }
      break;
    default:
      CHECK(false);
  }
}

}  // namespace Importer_NS
