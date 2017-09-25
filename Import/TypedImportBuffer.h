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
 * @file TypedImportBuffer.h
 * @author Wei Hong <wei@mapd.com>
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief TypedImportBuffer class for table import from file
 */
#ifndef _TYPEDIMPORTBUFFER_H_
#define _TYPEDIMPORTBUFFER_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

#include <arrow/api.h>
// Defines in arrow/util/logging.h redefine macros in glog/logging.h
//#define ARROW_UTIL_LOGGING_H
#include <arrow/util/decimal.h>

#include <string>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <type_traits>

#include "CopyParams.h"

#include "../Catalog/ColumnDescriptor.h"
#include "../Shared/fixautotools.h"
#include "../Shared/checked_alloc.h"
#include "../Shared/sqltypes.h"
#include "../QueryEngine/SqlTypesLayout.h"

#include "../StringDictionary/StringDictionary.h"

#include "ArrowSupport.h"

class TDatum;

namespace Importer_NS {

// Templates for copy_to_arrow_builder()

// Copy null to the Arrow builder
template <class TYPE>
::arrow::Status copy_nulls_to_arrow_builder(int start_index, int end_index,
                              std::unique_ptr<::arrow::ArrayBuilder>& builder) {
  ::arrow::Status status = ::arrow::Status::TypeError(std::string("copy_nulls_to_arrow_builder"));
  auto typed_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(builder.get());
  for (int i = start_index; i < end_index; i++) {
    status = typed_builder->AppendNull();
    if (!status.ok()) {
      return status;
    }
  }
  return status;
}

// Copy a slice of the scalar value column to the Arrow builder
template <typename TYPE, typename C_TYPE, typename CAST_TYPE>
  ::arrow::Status copy_scalar_to_arrow_builder(int start_index, int end_index,
                                             std::vector<C_TYPE>* buffer,
                                             std::unique_ptr<::arrow::ArrayBuilder>& builder,
                                             CAST_TYPE (*to_cast_type)(C_TYPE)) {
  ::arrow::Status status = ::arrow::Status::TypeError(std::string("copy_scalar_to_arrow_builder"));
  auto typed_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(builder.get());
  for (int i = start_index; i < end_index; i++) {
    const C_TYPE data = (*buffer)[i];
    if (data == none_encoded_null_value<C_TYPE>()) {
      status = typed_builder->AppendNull();
    } else {
      status = typed_builder->Append((*to_cast_type)(data));
    }
    if (!status.ok()) {
      return status;
    }
  }
  return status;
}

// Copy a slice of the array value column to the Arrow builder
template <typename TYPE, typename C_TYPE, typename CAST_TYPE>
  ::arrow::Status copy_array_to_arrow_builder(int start_index, int end_index,
                                            std::vector<ArrayDatum>* buffer,
                                            std::unique_ptr<::arrow::ArrayBuilder>& builder,
                                            CAST_TYPE (*to_cast_type)(C_TYPE)) {
  ::arrow::Status status = ::arrow::Status::TypeError("copy_array_to_arrow_builder");
  
  auto typed_builder = static_cast<::arrow::ListBuilder*>(builder.get());
  auto typed_value_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(typed_builder->value_builder());
  for (int i = start_index; i < end_index; i++) {
    auto& p = (*buffer)[i];

    if (true == p.is_null) {
      status = typed_builder->AppendNull();
    } else if ((false == p.is_null) && (0 < p.length) && (nullptr != p.pointer)) {
      //  array to vector
      std::vector<C_TYPE> array_data;

      array_data.assign(reinterpret_cast<C_TYPE*>(p.pointer),
                        reinterpret_cast<C_TYPE*>(p.pointer+p.length));

      status = typed_builder->Append();
      if (!status.ok()) {
        return status;
      }

      // copy array
      for (int j = 0; j < array_data.size(); j++) {
        const C_TYPE data = array_data[j];
        if (data == none_encoded_null_value<C_TYPE>()) {
          status = typed_value_builder->AppendNull();
        } else {
          status = typed_value_builder->Append((*to_cast_type)(data));
        }
        if (!status.ok()) {
          return status;
        }
      }
    }
  }
  return status;
}

// Templates for toString()

// Convert ArrayDatum to string for a given C type
template <typename C_TYPE>
std::string _ArrayDatumtoString(int row_idx, const CopyParams& copy_params,
                     std::vector<ArrayDatum>* buffer,
                     int param1,
                     int param2,
                     typename std::string (*to_string)(C_TYPE, int, int)) {
  auto& p = (*buffer)[row_idx];

  if (!p.is_null) {
    std::vector<C_TYPE> array_data;

    if ((0 < p.length) && (nullptr != p.pointer)) {
      std::string strArray;
      bool is_first = true;

      //  array to vector
      array_data.assign(reinterpret_cast<C_TYPE*>(p.pointer),
                        reinterpret_cast<C_TYPE*>(p.pointer+p.length));

      // copy array

      strArray += copy_params.array_begin;

      for (int j = 0; j < array_data.size(); j++) {
        if (is_first == false) {
          strArray += copy_params.array_delim;
        } else {
          is_first = false;
        }

        C_TYPE data = array_data[j];
        if (data == none_encoded_null_value<C_TYPE>()) {
          strArray += copy_params.null_str;
        } else {
          strArray += (*to_string)(data, param1, param2);
        }
      }

      strArray += copy_params.array_end;

      return strArray;
    }
  }

  return copy_params.null_str;
}

// NOTE: Uses "data type"ToString functions in Shared/sqltypes.h and Shared/Datum.cpp
//
//       When QueryEngine/ResultSetConversion.cpp supports more types, these
//       functions could be used there too.
//

// TODO(renesugar): Support for kINTERVAL_DAY_TIME and kINTERVAL_YEAR_MONTH
//                  is missing from TypedImportBuffer
//
//                  MapD checks the size of time_t (e.g. if (sizeof(time_t) == 4));
//                  Arrow uses int64_t for intervals.
//
class TypedImportBuffer : boost::noncopyable {
public:
  explicit TypedImportBuffer(const ColumnDescriptor* col_desc,
                             StringDictionary* string_dict = nullptr)
  : column_desc_(col_desc), string_dict_(string_dict) {
    switch (col_desc->columnType.get_type()) {
      case kBOOLEAN:
        bool_buffer_ = new std::vector<int8_t>();
        break;
      case kSMALLINT:
        smallint_buffer_ = new std::vector<int16_t>();
        break;
      case kINT:
        int_buffer_ = new std::vector<int32_t>();
        break;
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL:
        bigint_buffer_ = new std::vector<int64_t>();
        break;
      case kFLOAT:
        float_buffer_ = new std::vector<float>();
        break;
      case kDOUBLE:
        double_buffer_ = new std::vector<double>();
        break;
      case kTEXT:
      case kVARCHAR:
      case kCHAR:
        string_buffer_ = new std::vector<std::string>();
        if (col_desc->columnType.get_compression() == kENCODING_DICT) {
          switch (col_desc->columnType.get_size()) {
            case 1:
              string_dict_i8_buffer_ = new std::vector<uint8_t>();
              break;
            case 2:
              string_dict_i16_buffer_ = new std::vector<uint16_t>();
              break;
            case 4:
              string_dict_i32_buffer_ = new std::vector<int32_t>();
              break;
            default:
              CHECK(false);
          }
        }
        break;
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
        time_buffer_ = new std::vector<time_t>();
        break;
      case kARRAY:
        if (IS_STRING(col_desc->columnType.get_subtype())) {
          CHECK(col_desc->columnType.get_compression() == kENCODING_DICT);
          string_array_buffer_ = new std::vector<std::vector<std::string>>();
          string_array_dict_buffer_ = new std::vector<ArrayDatum>();
        } else {
          array_buffer_ = new std::vector<ArrayDatum>();
        }
        break;
      default:
        CHECK(false);
    }
  }

  ~TypedImportBuffer() {
    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN:
        delete bool_buffer_;
        break;
      case kSMALLINT:
        delete smallint_buffer_;
        break;
      case kINT:
        delete int_buffer_;
        break;
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL:
        delete bigint_buffer_;
        break;
      case kFLOAT:
        delete float_buffer_;
        break;
      case kDOUBLE:
        delete double_buffer_;
        break;
      case kTEXT:
      case kVARCHAR:
      case kCHAR:
        delete string_buffer_;
        if (column_desc_->columnType.get_compression() == kENCODING_DICT) {
          switch (column_desc_->columnType.get_size()) {
            case 1:
              delete string_dict_i8_buffer_;
              break;
            case 2:
              delete string_dict_i16_buffer_;
              break;
            case 4:
              delete string_dict_i32_buffer_;
              break;
          }
        }
        break;
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
        delete time_buffer_;
        break;
      case kARRAY:
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          delete string_array_buffer_;
          delete string_array_dict_buffer_;
        } else {
          delete array_buffer_;
        }
        break;
      default:
        CHECK(false);
    }
  }

  void addBoolean(const int8_t v) { bool_buffer_->push_back(v); }

  void addSmallint(const int16_t v) { smallint_buffer_->push_back(v); }

  void addInt(const int32_t v) { int_buffer_->push_back(v); }

  void addBigint(const int64_t v) { bigint_buffer_->push_back(v); }

  void addFloat(const float v) { float_buffer_->push_back(v); }

  void addDouble(const double v) { double_buffer_->push_back(v); }

  void addString(const std::string& v) { string_buffer_->push_back(v); }

  void addArray(const ArrayDatum& v) { array_buffer_->push_back(v); }

  std::vector<std::string>& addStringArray() {
    string_array_buffer_->push_back(std::vector<std::string>());
    return string_array_buffer_->back();
  }

  void addStringArray(const std::vector<std::string>& arr) { string_array_buffer_->push_back(arr); }

  void addTime(const time_t v) { time_buffer_->push_back(v); }

  void addDictEncodedString(const std::vector<std::string>& string_vec) {
    // TODO(renesugar): String dictionary is not used when using import/export
    //                  code in standalone programs.
    if (string_dict_ == nullptr) {
      return;
    }
    //CHECK(string_dict_);
    for (const auto& str : string_vec) {
      if (str.size() > StringDictionary::MAX_STRLEN) {
        throw std::runtime_error("String too long for dictionary encoding.");
      }
    }
    switch (column_desc_->columnType.get_size()) {
      case 1:
        string_dict_i8_buffer_->resize(string_vec.size());
        if (string_dict_ != nullptr)
          string_dict_->getOrAddBulk(string_vec, string_dict_i8_buffer_->data());
        break;
      case 2:
        string_dict_i16_buffer_->resize(string_vec.size());
        if (string_dict_ != nullptr)
          string_dict_->getOrAddBulk(string_vec, string_dict_i16_buffer_->data());
        break;
      case 4:
        string_dict_i32_buffer_->resize(string_vec.size());
        if (string_dict_ != nullptr)
          string_dict_->getOrAddBulk(string_vec, string_dict_i32_buffer_->data());
        break;
      default:
        CHECK(false);
    }
  }

  void addDictEncodedStringArray(const std::vector<std::vector<std::string>>& string_array_vec) {
    // TODO(renesugar): String dictionary is not used when using import/export
    //                  code in standalone programs.
    if (string_dict_ == nullptr) {
      return;
    }
    // CHECK(string_dict_);
    for (auto& p : string_array_vec) {
      size_t len = p.size() * sizeof(int32_t);
      auto a = static_cast<int32_t*>(checked_malloc(len));
      for (const auto& str : p) {
        if (str.size() > StringDictionary::MAX_STRLEN) {
          throw std::runtime_error("String too long for dictionary encoding.");
        }
      }
      if (string_dict_ != nullptr)
        string_dict_->getOrAddBulk(p, a);
      string_array_dict_buffer_->push_back(ArrayDatum(len, reinterpret_cast<int8_t*>(a), len == 0));
    }
  }

  const SQLTypeInfo& getTypeInfo() const { return column_desc_->columnType; }

  const ColumnDescriptor* getColumnDesc() const { return column_desc_; }

  const std::string& getColumnName() const { return column_desc_->columnName; }

  StringDictionary* getStringDictionary() const { return string_dict_; }

  int8_t* getAsBytes() const {
    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN:
        return reinterpret_cast<int8_t*>(&((*bool_buffer_)[0]));
      case kSMALLINT:
        return reinterpret_cast<int8_t*>(&((*smallint_buffer_)[0]));
      case kINT:
        return reinterpret_cast<int8_t*>(&((*int_buffer_)[0]));
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL:
        return reinterpret_cast<int8_t*>(&((*bigint_buffer_)[0]));
      case kFLOAT:
        return reinterpret_cast<int8_t*>(&((*float_buffer_)[0]));
      case kDOUBLE:
        return reinterpret_cast<int8_t*>(&((*double_buffer_)[0]));
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
        return reinterpret_cast<int8_t*>(&((*time_buffer_)[0]));
      default:
        abort();
    }
  }

  size_t getElementSize() const {
    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN:
        return sizeof((*bool_buffer_)[0]);
      case kSMALLINT:
        return sizeof((*smallint_buffer_)[0]);
      case kINT:
        return sizeof((*int_buffer_)[0]);
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL:
        return sizeof((*bigint_buffer_)[0]);
      case kFLOAT:
        return sizeof((*float_buffer_)[0]);
      case kDOUBLE:
        return sizeof((*double_buffer_)[0]);
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
        return sizeof((*time_buffer_)[0]);
      default:
        abort();
    }
  }

  std::vector<std::string>* getStringBuffer() const { return string_buffer_; }

  std::vector<ArrayDatum>* getArrayBuffer() const { return array_buffer_; }

  std::vector<std::vector<std::string>>* getStringArrayBuffer() const { return string_array_buffer_; }

  std::vector<ArrayDatum>* getStringArrayDictBuffer() const { return string_array_dict_buffer_; }

  int8_t* getStringDictBuffer() const {
    switch (column_desc_->columnType.get_size()) {
      case 1:
        return reinterpret_cast<int8_t*>(&((*string_dict_i8_buffer_)[0]));
      case 2:
        return reinterpret_cast<int8_t*>(&((*string_dict_i16_buffer_)[0]));
      case 4:
        return reinterpret_cast<int8_t*>(&((*string_dict_i32_buffer_)[0]));
      default:
        abort();
    }
  }

  bool stringDictCheckpoint() {
    if (string_dict_ == nullptr)
      return true;
    return string_dict_->checkpoint();
  }

  size_t size() {
    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN: {
        return bool_buffer_->size();
        break;
      }
      case kSMALLINT: {
        return smallint_buffer_->size();
        break;
      }
      case kINT: {
        return int_buffer_->size();
        break;
      }
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL: {
        return bigint_buffer_->size();
        break;
      }
      case kFLOAT: {
        return float_buffer_->size();
        break;
      }
      case kDOUBLE: {
        return double_buffer_->size();
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        return string_buffer_->size();
        break;
      }
      case kTIME:
      case kTIMESTAMP:
      case kDATE: {
        return time_buffer_->size();
        break;
      }
      case kARRAY: {
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          return string_array_buffer_->size();
        } else {
          return array_buffer_->size();
        }
        break;
      }
      default:
        CHECK(false);
    }
  }

  void clear() {
    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN: {
        bool_buffer_->clear();
        break;
      }
      case kSMALLINT: {
        smallint_buffer_->clear();
        break;
      }
      case kINT: {
        int_buffer_->clear();
        break;
      }
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL: {
        bigint_buffer_->clear();
        break;
      }
      case kFLOAT: {
        float_buffer_->clear();
        break;
      }
      case kDOUBLE: {
        double_buffer_->clear();
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        string_buffer_->clear();
        if (column_desc_->columnType.get_compression() == kENCODING_DICT) {
          switch (column_desc_->columnType.get_size()) {
            case 1:
              string_dict_i8_buffer_->clear();
              break;
            case 2:
              string_dict_i16_buffer_->clear();
              break;
            case 4:
              string_dict_i32_buffer_->clear();
              break;
            default:
              CHECK(false);
          }
        }
        break;
      }
      case kTIME:
      case kTIMESTAMP:
      case kDATE: {
        time_buffer_->clear();
        break;
      }
      case kARRAY: {
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          string_array_buffer_->clear();
          string_array_dict_buffer_->clear();
        } else {
          array_buffer_->clear();
        }
        break;
      }
      default:
        CHECK(false);
    }
  }

  std::unique_ptr<TypedImportBuffer> clone() {
    std::unique_ptr<TypedImportBuffer> buffer(new TypedImportBuffer(column_desc_, string_dict_));

    // copy data to the new TypedImportBuffer

    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN: {
        for (int i = 0; i < bool_buffer_->size(); i++) {
          buffer->bool_buffer_->push_back((*bool_buffer_)[i]);
        }
        break;
      }
      case kSMALLINT: {
        for (int i = 0; i < smallint_buffer_->size(); i++) {
          buffer->smallint_buffer_->push_back((*smallint_buffer_)[i]);
        }
        break;
      }
      case kINT: {
        for (int i = 0; i < int_buffer_->size(); i++) {
          buffer->int_buffer_->push_back((*int_buffer_)[i]);
        }
        break;
      }
      case kBIGINT:
      case kNUMERIC:
      case kDECIMAL: {
        for (int i = 0; i < bigint_buffer_->size(); i++) {
          buffer->bigint_buffer_->push_back((*bigint_buffer_)[i]);
        }
        break;
      }
      case kFLOAT: {
        for (int i = 0; i < float_buffer_->size(); i++) {
          buffer->float_buffer_->push_back((*float_buffer_)[i]);
        }
        break;
      }
      case kDOUBLE: {
        for (int i = 0; i < double_buffer_->size(); i++) {
          buffer->double_buffer_->push_back((*double_buffer_)[i]);
        }
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        for (int i = 0; i < string_buffer_->size(); i++) {
          buffer->string_buffer_->push_back((*string_buffer_)[i]);
        }

        if (column_desc_->columnType.get_compression() == kENCODING_DICT) {
          switch (column_desc_->columnType.get_size()) {
            case 1:
              for (int i = 0; i < string_dict_i8_buffer_->size(); i++) {
                buffer->string_dict_i8_buffer_->push_back((*string_dict_i8_buffer_)[i]);
              }
              break;
            case 2:
              for (int i = 0; i < string_dict_i16_buffer_->size(); i++) {
                buffer->string_dict_i16_buffer_->push_back((*string_dict_i16_buffer_)[i]);
              }
              break;
            case 4:
              for (int i = 0; i < string_dict_i32_buffer_->size(); i++) {
                buffer->string_dict_i32_buffer_->push_back((*string_dict_i32_buffer_)[i]);
              }
              break;
            default:
              CHECK(false);
          }
        }
        break;
      }
      case kTIME:
      case kTIMESTAMP:
      case kDATE: {
        for (int i = 0; i < time_buffer_->size(); i++) {
          buffer->time_buffer_->push_back((*time_buffer_)[i]);
        }
        break;
      }
      case kARRAY: {
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          // copy the std::vector<std::vector<std::string>>
          for (auto& p : (*string_array_buffer_)) {
            // copy the std::vector<std::string>
            std::vector<std::string> string_vec;
            for (const auto& str : p) {
              string_vec.push_back(str);
            }
            buffer->string_array_buffer_->push_back(string_vec);
          }

          // copy string_array_dict_buffer_

          int8_t* buf = nullptr;;
          size_t  len = 0;

          for (auto& p : (*string_array_dict_buffer_)) {
            buf = nullptr;
            len = 0;
            if ((false == p.is_null) && (0 < p.length)) {
              buf = reinterpret_cast<int8_t*>(checked_malloc(p.length));
              memcpy(buf, p.pointer, p.length);
            }
            buffer->string_array_dict_buffer_->push_back(ArrayDatum(len, buf, len == 0));
          }
        } else {
          // copy array_buffer_

          int8_t* buf = nullptr;;
          size_t  len = 0;

          for (auto& p : (*array_buffer_)) {
            buf = nullptr;
            len = 0;
            if ((false == p.is_null) && (0 < p.length) && (nullptr != p.pointer)) {
              buf = reinterpret_cast<int8_t*>(checked_malloc(p.length));
              memcpy(buf, p.pointer, p.length);
            }
            buffer->array_buffer_->push_back(ArrayDatum(len, buf, len == 0));
          }
        }
        break;
      }
      default:
        CHECK(false);
    }

    return std::unique_ptr<TypedImportBuffer>(buffer.release());
  }

  std::unique_ptr<::arrow::ArrayBuilder> make_arrow_builder() {
    // When done, call builder.Finish(&table) to get Arrow table

    return SQLTypeToArrowBuilder(column_desc_->columnType);
  }

  // TODO(renesugar): For export, Parquet files could be output from result sets.
  //
  //       See ExportQueryStmt::execute in Parser/ParserNode.cpp.
  //
  //       However, also adding Arrow table support to TypedImportBuffer
  //       allows data to be converted to Parquet or other formats by
  //       implementing new Writer classes.
  //
  //       For example, a SQLite writer class could write one or more MapD
  //       tables to an SQLite file. A corresponding SQLite importer class could
  //       import SQLite databases into MapD.
  //
  //       You can choose the destination to which the data is written.
  //       (e.g. MapD (import), Parquet (import/export), Arrow (export),
  //             CSV/TSV (import/export), etc.)
  //
  //       Use result set or Arrow table as an import buffer?
  //

  ::arrow::Status copy_nulls_to_arrow_builder(int start_index, int end_index,
                                        std::unique_ptr<::arrow::ArrayBuilder>& builder) {
    size_t max_index = this->size();

    return CopyNullsToArrowBuilder(column_desc_->columnType,
                                   start_index,
                                   end_index,
                                   max_index,
                                   builder);
  }

  // Arrow arrays are immutable. Copy the right number of rows for a row group to the builder.
  ::arrow::Status copy_to_arrow_builder(int start_index, int end_index,
                             std::unique_ptr<::arrow::ArrayBuilder>& builder) {
    size_t max_index = this->size();

    if (start_index < 0) {
      start_index = 0;
    }

    if (end_index < 0) {
      end_index = max_index;
    }

    // check if indices are in range
    if ((start_index > max_index) || (end_index > max_index)) {
      CHECK(false);
    }

    // copy data to the Arrow builder

    switch (column_desc_->columnType.get_type()) {
      // Cog code generation tool: https://nedbatchelder.com/code/cog/
      //
      // cog.py-2.7 -r TypedImportBuffer.h
      //

      // TODO(renesugar): MapD and Arrow support for the intervals is incomplete.
      //
      //       MapD Interval Types    Arrow IntervalType
      //       -------------------    ------------------
      //       kINTERVAL_DAY_TIME     IntervalType::Unit::DAY_TIME
      //       kINTERVAL_YEAR_MONTH   IntervalType::Unit::YEAR_MONTH

      // TODO(renesugar): IntervalBuilder is missing from Arrow library

      // TODO(renesugar): MapD uses time_t for intervals; Arrow uses int64_t

      // TODO(renesugar): Intervals not handled by StringToDatum in
      //                  Shared/Datum.cpp (used by import)

      /*[[[cog
       def capitalize(name):
         return ''.join(s[0].upper() + s[1:] for s in name.split('_'))
       
       types = [
       # sql_type     arrow_type        arrow_builder         type_array       c_type        cast_type        cast_function
       # --------     -------------     -------------------   ----------       ------        ---------        -------------
       ('kBOOLEAN',   'BooleanType',   'BooleanBuilder',     'bool',         'int8_t',          'bool',               '*'),
       ('kSMALLINT',  'Int16Type',     'Int16Builder',       'smallint',    'int16_t',       'int16_t',       'identity'),
       ('kINT',       'Int32Type',     'Int32Builder',       'int',         'int32_t',       'int32_t',       'identity'),
       ('kBIGINT',    'Int64Type',     'Int64Builder',       'bigint',      'int64_t',       'int64_t',       'identity'),
       ('kNUMERIC',   'Int64Type',     'Int64Builder',       'bigint',      'int64_t',       'int64_t',       'identity'),
       ('kFLOAT',     'FloatType',     'FloatBuilder',       'float',         'float',         'float',       'identity'),
       ('kDOUBLE',    'DoubleType',    'DoubleBuilder',      'double',       'double',        'double',       'identity'),
       ('kTIME',      'Time64Type',    'Time64Builder',      'time',         'time_t',       'int64_t',       'identity'),
       ('kTIMESTAMP', 'TimestampType', 'TimestampBuilder',   'time',         'time_t',       'int64_t',       'identity'),
       ('kDATE',      'Date64Type',    'Date64Builder',      'time',         'time_t',        'time_t',       'identity'),
       ('kDECIMAL',   'DecimalType',   'DecimalBuilder',     'bigint',      'int64_t',        '::arrow::Decimal128',       '*'),
       ]
       
       for sql_type, arrow_type, arrow_builder, type_array, c_type, cast_type, cast_function in types:
         cog.outl('case %s: {' % (sql_type))
         if sql_type == 'kBOOLEAN':
           cog.outl('  ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, %s_buffer_, builder, [](%s v) -> %s { return (v != 0); });' % (arrow_type, c_type, cast_type, type_array, c_type, cast_type))
         elif sql_type == 'kTIME':
           cog.outl('  ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, %s_buffer_, builder, [](%s v) -> %s { return (UINT64_C(1000000) * v); });' % (arrow_type, c_type, cast_type, type_array, c_type, cast_type))
         elif sql_type == 'kTIMESTAMP':
           cog.outl('  ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, %s_buffer_, builder, [](%s v) -> %s { return (UINT64_C(1000000) * v); });' % (arrow_type, c_type, cast_type, type_array, c_type, cast_type))
         elif sql_type == 'kDECIMAL':
           cog.outl('  ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, %s_buffer_, builder, [](%s v) -> %s { return ::arrow::Decimal128(v); });' % (arrow_type, c_type, cast_type, type_array, c_type, cast_type))
         else:
           cog.outl('  ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, %s_buffer_, builder, [](%s v) -> %s { return (v); });' % (arrow_type, c_type, cast_type, type_array, c_type, cast_type))
         cog.outl('  ARROW_RETURN_NOT_OK(s);')
         cog.outl('  break;')
         cog.outl('}')
       ]]]*/
      case kBOOLEAN: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::BooleanType, int8_t, bool>(start_index, end_index, bool_buffer_, builder, [](int8_t v) -> bool { return (v != 0); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kSMALLINT: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Int16Type, int16_t, int16_t>(start_index, end_index, smallint_buffer_, builder, [](int16_t v) -> int16_t { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kINT: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Int32Type, int32_t, int32_t>(start_index, end_index, int_buffer_, builder, [](int32_t v) -> int32_t { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kBIGINT: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Int64Type, int64_t, int64_t>(start_index, end_index, bigint_buffer_, builder, [](int64_t v) -> int64_t { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kNUMERIC: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Int64Type, int64_t, int64_t>(start_index, end_index, bigint_buffer_, builder, [](int64_t v) -> int64_t { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kFLOAT: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::FloatType, float, float>(start_index, end_index, float_buffer_, builder, [](float v) -> float { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kDOUBLE: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::DoubleType, double, double>(start_index, end_index, double_buffer_, builder, [](double v) -> double { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kTIME: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Time64Type, time_t, int64_t>(start_index, end_index, time_buffer_, builder, [](time_t v) -> int64_t { return (UINT64_C(1000000) * v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kTIMESTAMP: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::TimestampType, time_t, int64_t>(start_index, end_index, time_buffer_, builder, [](time_t v) -> int64_t { return (UINT64_C(1000000) * v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kDATE: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::Date64Type, time_t, time_t>(start_index, end_index, time_buffer_, builder, [](time_t v) -> time_t { return (v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
      case kDECIMAL: {
        ::arrow::Status s = copy_scalar_to_arrow_builder<::arrow::DecimalType, int64_t, ::arrow::Decimal128>(start_index, end_index, bigint_buffer_, builder, [](int64_t v) -> ::arrow::Decimal128 { return ::arrow::Decimal128(v); });
        ARROW_RETURN_NOT_OK(s);
        break;
      }
        //[[[end]]]
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        // TODO(renesugar): Add support for using Arrow dictionary columns for
        //                  for kENCODING_DICT encoded text columns.
        ::arrow::Status status = ::arrow::Status::TypeError(std::string("case kTEXT kVARCHAR kCHAR in copy_to_arrow_builder"));
        auto typed_builder = static_cast<::arrow::StringBuilder*>(builder.get());
        for (int i = start_index; i < end_index; i++) {
          if ((*string_buffer_)[i].empty()) {
            status = typed_builder->AppendNull();
          } else {
            status = typed_builder->Append((*string_buffer_)[i]);
          }
          if (!status.ok()) {
            return status;
          }
        }
        break;
      }
      case kARRAY: {
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          ::arrow::Status status = ::arrow::Status::TypeError(std::string("case kARRAY/IS_STRING in copy_to_arrow_builder"));
          auto typed_builder = static_cast<::arrow::ListBuilder*>(builder.get());
          auto typed_value_builder = static_cast<::arrow::StringBuilder*>(typed_builder->value_builder());
          // copy the std::vector<std::vector<std::string>>
          for (int i = start_index; i < end_index; i++) {
            auto& p = (*string_array_buffer_)[i];

            // NOTE: MapD uses empty strings to represent NULL values in strings.
            // Treat an empty string array as a NULL value.

            if (p.empty()) {
              status = typed_builder->AppendNull();
              if (!status.ok()) {
                return status;
              }
            } else {
              status = typed_builder->Append();
              if (!status.ok()) {
                return status;
              }
              // copy the std::vector<std::string>
              for (const auto& str : p) {
                status = typed_value_builder->Append(str);
                if (!status.ok()) {
                  return status;
                }
              }
            }
          }
        } else {
          // copy array_buffer_
          switch (column_desc_->columnType.get_subtype()) {
            // Cog code generation tool: https://nedbatchelder.com/code/cog/
            //
            // cog.py-2.7 -r TypedImportBuffer.h
            //

            /*[[[cog
             def capitalize(name):
               return ''.join(s[0].upper() + s[1:] for s in name.split('_'))

             # TODO(renesugar): Add kINTERVAL_DAY_TIME and kINTERVAL_YEAR_MONTH
             #                  when Arrow support for interval types is complete.

             types = [
             # sql_type     arrow_type        arrow_builder         type_array       c_type        cast_type        cast_function
             # --------     -------------     -------------------   ----------       ------        ---------        -------------
             ('kBOOLEAN',   'BooleanType',   'BooleanBuilder',     'bool',         'int8_t',          'bool',               '*'),
             ('kSMALLINT',  'Int16Type',     'Int16Builder',       'smallint',    'int16_t',       'int16_t',       'identity'),
             ('kINT',       'Int32Type',     'Int32Builder',       'int',         'int32_t',       'int32_t',       'identity'),
             ('kBIGINT',    'Int64Type',     'Int64Builder',       'bigint',      'int64_t',       'int64_t',       'identity'),
             ('kNUMERIC',   'Int64Type',     'Int64Builder',       'bigint',      'int64_t',       'int64_t',       'identity'),
             ('kFLOAT',     'FloatType',     'FloatBuilder',       'float',         'float',         'float',       'identity'),
             ('kDOUBLE',    'DoubleType',    'DoubleBuilder',      'double',       'double',        'double',       'identity'),
             ('kTIME',      'Time64Type',    'Time64Builder',      'time',         'time_t',       'int64_t',       'identity'),
             ('kTIMESTAMP', 'TimestampType', 'TimestampBuilder',   'time',         'time_t',       'int64_t',       'identity'),
             ('kDATE',      'Date64Type',    'Date64Builder',      'time',         'time_t',        'time_t',       'identity'),
             ('kDECIMAL',   'DecimalType',   'DecimalBuilder',     'bigint',      'int64_t',        '::arrow::Decimal128',       '*'),
             ]

             for sql_type, arrow_type, arrow_builder, type_array, c_type, cast_type, cast_function in types:
               cog.outl('case %s: {' % (sql_type))
               if sql_type == 'kBOOLEAN':
                 cog.outl('  ::arrow::Status s = copy_array_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, array_buffer_, builder, [](%s v) -> %s { return (v != 0); });' % (arrow_type, c_type, cast_type, c_type, cast_type))
               elif sql_type == 'kTIME':
                 cog.outl('  ::arrow::Status s = copy_array_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, array_buffer_, builder, [](%s v) -> %s { return (UINT64_C(1000000) * v); });' % (arrow_type, c_type, cast_type, c_type, cast_type))
               elif sql_type == 'kTIMESTAMP':
                 cog.outl('  ::arrow::Status s = copy_array_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, array_buffer_, builder, [](%s v) -> %s { return (UINT64_C(1000000) * v); });' % (arrow_type, c_type, cast_type, c_type, cast_type))
               elif sql_type == 'kDECIMAL':
                 cog.outl('  ::arrow::Status s = copy_array_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, array_buffer_, builder, [](%s v) -> %s { return ::arrow::Decimal128(v); });' % (arrow_type, c_type, cast_type, c_type, cast_type))
               else:
                 cog.outl('  ::arrow::Status s = copy_array_to_arrow_builder<::arrow::%s, %s, %s>(start_index, end_index, array_buffer_, builder, [](%s v) -> %s { return (v); });' % (arrow_type, c_type, cast_type, c_type, cast_type))
               cog.outl('  ARROW_RETURN_NOT_OK(s);')
               cog.outl('  break;')
               cog.outl('}')
             ]]]*/
            case kBOOLEAN: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::BooleanType, int8_t, bool>(start_index, end_index, array_buffer_, builder, [](int8_t v) -> bool { return (v != 0); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kSMALLINT: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Int16Type, int16_t, int16_t>(start_index, end_index, array_buffer_, builder, [](int16_t v) -> int16_t { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kINT: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Int32Type, int32_t, int32_t>(start_index, end_index, array_buffer_, builder, [](int32_t v) -> int32_t { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kBIGINT: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Int64Type, int64_t, int64_t>(start_index, end_index, array_buffer_, builder, [](int64_t v) -> int64_t { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kNUMERIC: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Int64Type, int64_t, int64_t>(start_index, end_index, array_buffer_, builder, [](int64_t v) -> int64_t { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kFLOAT: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::FloatType, float, float>(start_index, end_index, array_buffer_, builder, [](float v) -> float { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kDOUBLE: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::DoubleType, double, double>(start_index, end_index, array_buffer_, builder, [](double v) -> double { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kTIME: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Time64Type, time_t, int64_t>(start_index, end_index, array_buffer_, builder, [](time_t v) -> int64_t { return (UINT64_C(1000000) * v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kTIMESTAMP: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::TimestampType, time_t, int64_t>(start_index, end_index, array_buffer_, builder, [](time_t v) -> int64_t { return (UINT64_C(1000000) * v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kDATE: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::Date64Type, time_t, time_t>(start_index, end_index, array_buffer_, builder, [](time_t v) -> time_t { return (v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
            case kDECIMAL: {
              ::arrow::Status s = copy_array_to_arrow_builder<::arrow::DecimalType, int64_t, ::arrow::Decimal128>(start_index, end_index, array_buffer_, builder, [](int64_t v) -> ::arrow::Decimal128 { return ::arrow::Decimal128(v); });
              ARROW_RETURN_NOT_OK(s);
              break;
            }
              //[[[end]]]
            case kTEXT:
            case kVARCHAR:
            case kCHAR: {
              // String arrays handled in "if" branch above.
              // String arrays are not stored as vectors of ArrayDatum.
              break;
            }
            case kARRAY: {
              // TODO(renesugar): Implement nested arrays?
              CHECK(false);
              break;
            }
            default:
              CHECK(false);
          }
        }
        break;
      }
      default:
        CHECK(false);
    }

    return ::arrow::Status::OK();
  }

  std::string toString(int row_idx, const CopyParams& copy_params) {
    size_t max_index = this->size();

    if (row_idx < 0) {
      row_idx = 0;
    }

    // check if index is in range
    if (row_idx > max_index) {
      CHECK(false);
    }

    // convert data to string

    switch (column_desc_->columnType.get_type()) {
      case kBOOLEAN: {
        if ((*bool_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return BooleanToString((*bool_buffer_)[row_idx]);
        }
        break;
      }
      case kSMALLINT: {
        if ((*smallint_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return std::to_string((*smallint_buffer_)[row_idx]);
        }
        break;
      }
      case kINT: {
        if ((*int_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return std::to_string((*int_buffer_)[row_idx]);
        }
        break;
      }
      case kBIGINT:
      case kNUMERIC: {
        if ((*bigint_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return std::to_string((*bigint_buffer_)[row_idx]);
        }
        break;
      }
      case kDECIMAL: {
        if ((*bigint_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return DecimalToString((*bigint_buffer_)[row_idx], column_desc_->columnType.get_dimension(), column_desc_->columnType.get_scale());
        }
        break;
      }
      case kFLOAT: {
        if ((*float_buffer_)[row_idx] == inline_fp_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return std::to_string((*float_buffer_)[row_idx]);
        }
        break;
      }
      case kDOUBLE: {
        if ((*double_buffer_)[row_idx] == inline_fp_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return std::to_string((*double_buffer_)[row_idx]);
        }
        break;
      }
      case kTEXT:
      case kVARCHAR:
      case kCHAR: {
        if ((*string_buffer_)[row_idx].empty()) {
          return copy_params.null_str;
        } else {
          return (*string_buffer_)[row_idx];
        }
        break;
      }
      case kTIME:  {
        if ((*time_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return TimeToString((*time_buffer_)[row_idx]);
        }
        break;
      }
      case kTIMESTAMP:  {
        if ((*time_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return TimestampToString((*time_buffer_)[row_idx]);
        }
        break;
      }
      case kDATE: {
        if ((*time_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return DateToString((*time_buffer_)[row_idx]);
        }
        break;
      }
      case kINTERVAL_DAY_TIME: {
        if ((*time_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return IntervalDayTimeToString((*time_buffer_)[row_idx]);
        }
        break;
      }
      case kINTERVAL_YEAR_MONTH: {
        if ((*time_buffer_)[row_idx] == inline_fixed_encoding_null_val(column_desc_->columnType)) {
          return copy_params.null_str;
        } else {
          return IntervalYearMonthToString((*time_buffer_)[row_idx]);
        }
        break;
      }
      case kARRAY: {
        if (IS_STRING(column_desc_->columnType.get_subtype())) {
          auto& p = (*string_array_buffer_)[row_idx];

          // NOTE: MapD uses empty strings to represent NULL values in strings.
          // Treat an empty string array as a NULL value.

          if (p.empty()) {
            return copy_params.null_str;
          } else {
            // copy the std::vector<std::string>
            std::string strArray;
            bool is_first = true;

            strArray += copy_params.array_begin;

            for (const auto& str : p) {
              if (is_first == false) {
                strArray += copy_params.array_delim;
                is_first = false;
              }

              strArray += str;
            }

            strArray += copy_params.array_end;

            return strArray;
          }
        } else {
          // copy array_buffer_
          switch (column_desc_->columnType.get_subtype()) {
              // Cog code generation tool: https://nedbatchelder.com/code/cog/
              //
              // cog.py-2.7 -r TypedImportBuffer.h
              //

              /*[[[cog
               def capitalize(name):
                 return ''.join(s[0].upper() + s[1:] for s in name.split('_'))
               
               types = [
               # sql_type                   type_array       c_type   str_function                 str_param1        str_param2
               # --------                   ----------       ------   -----------------            ----------        ----------
               ('kBOOLEAN',                 'bool',         'int8_t', 'BooleanToString',                 '0',               '0'),
               ('kSMALLINT',                'smallint',    'int16_t', 'std::to_string',                  '0',               '0'),
               ('kINT',                     'int',         'int32_t', 'std::to_string',                  '0',               '0'),
               ('kBIGINT',                  'bigint',      'int64_t', 'std::to_string',                  '0',               '0'),
               ('kNUMERIC',                 'bigint',      'int64_t', 'std::to_string',                  '0',               '0'),
               ('kFLOAT',                   'float',         'float', 'std::to_string',                  '0',               '0'),
               ('kDOUBLE',                  'double',       'double', 'std::to_string',                  '0',               '0'),
               ('kTIME',                    'time',         'time_t', 'TimeToString',                    '0',               '0'),
               ('kTIMESTAMP',               'time',         'time_t', 'TimestampToString',               '0',               '0'),
               ('kDATE',                    'time',         'time_t', 'DateToString',                    '0',               '0'),
               ('kINTERVAL_DAY_TIME',       'time',         'time_t', 'IntervalDayTimeToString',         '0',               '0'),
               ('kINTERVAL_YEAR_MONTH',     'time',         'time_t', 'IntervalYearMonthToString',       '0',               '0'),
               ('kDECIMAL',                 'bigint',      'int64_t', 'DecimalToString',                 'column_desc_->columnType.get_dimension()',               'column_desc_->columnType.get_scale()'),
               ]
               
               for sql_type, type_array, c_type, str_function, str_param1, str_param2 in types:
                 cog.outl('case %s: {' % (sql_type))
                 if sql_type == 'kBOOLEAN':
                   cog.outl('  return _ArrayDatumtoString<%s>(row_idx, copy_params, array_buffer_,' % (c_type))
                   cog.outl('                 %s, %s, [](%s v, int p1, int p2) -> std::string { return BooleanToString(v != 0); });' % (str_param1, str_param2, c_type))
                 elif str_function == 'std::to_string':
                   cog.outl('  return _ArrayDatumtoString<%s>(row_idx, copy_params, array_buffer_,' % (c_type))
                   cog.outl('                 %s, %s, [](%s v, int p1, int p2) -> std::string { return std::to_string(v); });' % (str_param1, str_param2, c_type))
                 elif sql_type == 'kDECIMAL':
                   cog.outl('  return _ArrayDatumtoString<%s>(row_idx, copy_params, array_buffer_,' % (c_type))
                   cog.outl('                 %s, %s, [](%s v, int p1, int p2) -> std::string { return DecimalToString(v, p1, p2); });' % (str_param1, str_param2, c_type))
                 else:
                   cog.outl('  return _ArrayDatumtoString<%s>(row_idx, copy_params, array_buffer_,' % (c_type))
                   cog.outl('                 %s, %s, [](%s v, int p1, int p2) -> std::string { return %s(v); });' % (str_param1, str_param2, c_type, str_function))
                 cog.outl('  break;')
                 cog.outl('}')
               ]]]*/
              case kBOOLEAN: {
                return _ArrayDatumtoString<int8_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](int8_t v, int p1, int p2) -> std::string { return BooleanToString(v != 0); });
                break;
              }
              case kSMALLINT: {
                return _ArrayDatumtoString<int16_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](int16_t v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kINT: {
                return _ArrayDatumtoString<int32_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](int32_t v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kBIGINT: {
                return _ArrayDatumtoString<int64_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](int64_t v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kNUMERIC: {
                return _ArrayDatumtoString<int64_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](int64_t v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kFLOAT: {
                return _ArrayDatumtoString<float>(row_idx, copy_params, array_buffer_,
                               0, 0, [](float v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kDOUBLE: {
                return _ArrayDatumtoString<double>(row_idx, copy_params, array_buffer_,
                               0, 0, [](double v, int p1, int p2) -> std::string { return std::to_string(v); });
                break;
              }
              case kTIME: {
                return _ArrayDatumtoString<time_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](time_t v, int p1, int p2) -> std::string { return TimeToString(v); });
                break;
              }
              case kTIMESTAMP: {
                return _ArrayDatumtoString<time_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](time_t v, int p1, int p2) -> std::string { return TimestampToString(v); });
                break;
              }
              case kDATE: {
                return _ArrayDatumtoString<time_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](time_t v, int p1, int p2) -> std::string { return DateToString(v); });
                break;
              }
              case kINTERVAL_DAY_TIME: {
                return _ArrayDatumtoString<time_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](time_t v, int p1, int p2) -> std::string { return IntervalDayTimeToString(v); });
                break;
              }
              case kINTERVAL_YEAR_MONTH: {
                return _ArrayDatumtoString<time_t>(row_idx, copy_params, array_buffer_,
                               0, 0, [](time_t v, int p1, int p2) -> std::string { return IntervalYearMonthToString(v); });
                break;
              }
              case kDECIMAL: {
                return _ArrayDatumtoString<int64_t>(row_idx, copy_params, array_buffer_,
                               column_desc_->columnType.get_dimension(), column_desc_->columnType.get_scale(), [](int64_t v, int p1, int p2) -> std::string { return DecimalToString(v, p1, p2); });
                break;
              }
              //[[[end]]]
            case kTEXT:
            case kVARCHAR:
            case kCHAR: {
              // String arrays handled in "if" branch above.
              // String arrays are not stored as vectors of ArrayDatum.
              break;
            }
            case kARRAY: {
              // TODO(renesugar): Implement nested arrays?
              CHECK(false);
              break;
            }
            default:
              CHECK(false);
          }
        }
        break;
      }
      default:
        CHECK(false);
    }

    return std::string();
  }

  void add_value(const ColumnDescriptor* cd, const std::string& val, const bool is_null, const CopyParams& copy_params);
  void add_value(const ColumnDescriptor* cd, const TDatum& val, const bool is_null);
  void pop_value();

private:
  ::arrow::Status status_;
  union {
    std::vector<int8_t>* bool_buffer_;
    std::vector<int16_t>* smallint_buffer_;
    std::vector<int32_t>* int_buffer_;
    std::vector<int64_t>* bigint_buffer_;
    std::vector<float>* float_buffer_;
    std::vector<double>* double_buffer_;
    std::vector<time_t>* time_buffer_;
    std::vector<std::string>* string_buffer_;
    std::vector<ArrayDatum>* array_buffer_;
    std::vector<std::vector<std::string>>* string_array_buffer_;
  };
  union {
    std::vector<uint8_t>* string_dict_i8_buffer_;
    std::vector<uint16_t>* string_dict_i16_buffer_;
    std::vector<int32_t>* string_dict_i32_buffer_;
    std::vector<ArrayDatum>* string_array_dict_buffer_;
  };
  const ColumnDescriptor* column_desc_;
  StringDictionary* string_dict_;
};

}; // namespace Importer_NS
#endif  // _TYPEDIMPORTBUFFER_H_
