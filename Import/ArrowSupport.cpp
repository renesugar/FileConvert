/*
 * Copyright 2017 Rene Sugar
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
 * @file ArrowSupport.cpp
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for Arrow tables
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 */

#include "ArrowSupport.h"

#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

#include <memory>
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

#include "../Shared/SanitizeName.h"
#include "../Shared/HalfFloat.h"
#include "../QueryEngine/SqlTypesLayout.h"
#include "TypedImportBuffer.h"
#include "ArrowTemplates.h"
#include "DataGen.h"

#define STR(x) #x
#define STR_(x) STR(x)
#define STR__LINE__ STR_(__LINE__)

namespace Importer_NS {

std::shared_ptr<::arrow::DataType> SQLTypeToArrowType(const SQLTypeInfo& columnType) {
  switch (columnType.get_type()) {
    case kNULLT:
      return arrow::null();
    case kBOOLEAN:
      return arrow::boolean();
    case kCHAR:
    case kVARCHAR:
    case kTEXT:
      return arrow::utf8();
    case kSMALLINT:
      return arrow::int16();
    case kINT:
      return arrow::int32();
    case kBIGINT:
    case kNUMERIC:
      return arrow::int64();
    case kINTERVAL_DAY_TIME:
      return std::make_shared<::arrow::IntervalType>(arrow::IntervalType::Unit::DAY_TIME);
    case kINTERVAL_YEAR_MONTH:
      return std::make_shared<::arrow::IntervalType>(arrow::IntervalType::Unit::YEAR_MONTH);
      // TODO(renesugar): Integer types supported by Spark, etc..
      //                  Add support for these types to MapD?
      //
      // return arrow::int8();
      // return arrow::uint8();
      // return arrow::uint16();
      // return arrow::uint32();
      // return arrow::uint64();
      // return arrow::float16();
    case kFLOAT:
      return arrow::float32();
    case kDOUBLE:
      return arrow::float64();
    case kTIME:
      // Arrow uses int64_t for timestamp; use arrow:time64 and arrow::date64 so sizes match.

      // NOTE: In Arrow, for Time32Type, the TimeUnit::type must be
      //       TimeUnit::SECOND or TimeUnit::MILLI and for Time64Type, the
      //       TimeUnit::type must be TimeUnit::MICRO or TimeUnit::NANO
      //
      //       MapD stores times in seconds so, when exporting to Arrow, the
      //       value needs to be scaled by the time unit.
      //
      return arrow::time64(arrow::TimeUnit::MICRO);
    case kTIMESTAMP:
      // Arrow uses int64_t for timestamp; use arrow:time64 and arrow::date64 so sizes match.
      return arrow::timestamp(arrow::TimeUnit::MICRO);
      // return arrow::timestamp(TimeUnit::SECONDS);
      // return arrow::timestamp(TimeUnit::MILLI);
      // return arrow::timestamp(TimeUnit::NANO);
      // return arrow::binary();
    case kDATE:
      // return arrow::date32();
      // Arrow uses int64_t for timestamp; use arrow:time64 and arrow::date64 so sizes match.
      return arrow::date64();
    case kDECIMAL:
      return arrow::decimal(columnType.get_precision(), columnType.get_scale());
      // return arrow::fixed_size_binary(byte_width);
      // return arrow::timestamp(unit);
      // return arrow::timestamp(unit, timezone);
      // return arrow::time32(unit);
      // return arrow::time64(unit);
      // return arrow::list(value_type);
      // return arrow::list(value_field);
      // return arrow::struct_(fields);
      // return arrow::union_(child_fields,type_codes,mode);
      // return arrow::dictionary(index_type,dict_values);
    case kARRAY:
      // Store kARRAY as an Arrow ListType.
      return arrow::list(SQLTypeToArrowType(columnType.get_elem_type()));
    default:
      return std::make_shared<::arrow::StringType>();
  }
}

::arrow::Status ArrowTypeToSQLType(const std::shared_ptr<::arrow::Field>& field,
                                   ColumnDescriptor* cd) {
  ::arrow::Status status = ::arrow::Status::OK();

  if (cd == nullptr) {
    return ::arrow::Status::Invalid("Null column descriptor in " __FILE__ " at " STR__LINE__);
  }

  // initialize ColumnDescriptor

  cd->tableId = 0;
  cd->columnId = 0;
  cd->columnName = sanitize_name(field->name());
  cd->sourceName = sanitize_name(field->name());
  cd->columnType.set_type(kSQLTYPE_LAST);
  cd->columnType.set_subtype(kSQLTYPE_LAST);
  cd->columnType.set_dimension(0);
  cd->columnType.set_precision(0);
  cd->columnType.set_scale(0);
  cd->columnType.set_notnull(!field->nullable());
  cd->columnType.set_size(-1);
  // cd->columnType.set_fixed_size();
  cd->columnType.set_compression(kENCODING_NONE);
  cd->columnType.set_comp_param(0);
  // cd->chunks;
  cd->isSystemCol  = false;
  cd->isVirtualCol = false;
  // cd->virtualExpr;

  // set SQLTypeInfo if present in metadata for the field

  std::shared_ptr<const ::arrow::KeyValueMetadata> metadata = field->metadata();
  int metadata_size  = ((metadata != nullptr) ? metadata->size() : 0);
  int metadata_set   = 0;

  for (int i = 0; i < metadata_size; i++) {
    if (ArrowKeyValueMetadataToSQLTypeInfo(cd->columnType, metadata->key(i),
                                          metadata->value(i))) {
      metadata_set++;
    }
  }

  switch (field->type()->id()) {
    // A degenerate NULL type represented as 0 bytes/bits
    case ::arrow::Type::NA: {
      cd->columnType.set_type(kNULLT);
      break;
    }
    case ::arrow::Type::BOOL: {
      cd->columnType.set_type(kBOOLEAN);
      break;
    }
    // Little-endian integer types
    case ::arrow::Type::UINT8: {
      cd->columnType.set_type(kINT);
      break;
    }
    case ::arrow::Type::INT8: {
      cd->columnType.set_type(kINT);
      break;
    }
    case ::arrow::Type::UINT16: {
      cd->columnType.set_type(kSMALLINT);
      break;
    }
    case ::arrow::Type::INT16: {
      cd->columnType.set_type(kSMALLINT);
      break;
    }
    case ::arrow::Type::UINT32: {
      cd->columnType.set_type(kINT);
      break;
    }
    case ::arrow::Type::INT32: {
      cd->columnType.set_type(kINT);
      break;
    }
    case ::arrow::Type::UINT64: {
      cd->columnType.set_type(kBIGINT);
      break;
    }
    case ::arrow::Type::INT64: {
      cd->columnType.set_type(kBIGINT);
      break;
    }
    // 2-byte floating point value
    case ::arrow::Type::HALF_FLOAT: {
      cd->columnType.set_type(kFLOAT);
      break;
    }
    // 4-byte floating point value
    case ::arrow::Type::FLOAT: {
      cd->columnType.set_type(kFLOAT);
      break;
    }
    // 8-byte floating point value
    case ::arrow::Type::DOUBLE: {
      cd->columnType.set_type(kDOUBLE);
      break;
    }
    // UTF8 variable-length string as List<Char>
    case ::arrow::Type::STRING: {
      if ((cd->columnType.get_type() != kSQLTYPE_LAST) &&
          (cd->columnType.is_string())) {
        // keep the type set in the metadata
        //
        // MapD has kCHAR, kVARCHAR, kTEXT string types; Arrow has STRING.
      } else {
        cd->columnType.set_type(kVARCHAR);
        cd->columnType.set_size(-1);
      }
      break;
    }
    // Variable-length bytes (no guarantee of UTF8-ness)
    case ::arrow::Type::BINARY: {
      // TODO(renesugar): List<Int8> and List<UInt8> are stored as kVARCHAR.
      //                  Are there character set conversions that interfere with
      //                  storing binary data in strings?
      cd->columnType.set_type(kVARCHAR);
      cd->columnType.set_size(-1);
      break;
    }
    // Fixed-size binary. Each value occupies the same number of bytes
    case ::arrow::Type::FIXED_SIZE_BINARY: {
      auto typed_datatype = static_cast<arrow::FixedSizeBinaryType*>(field->type().get());
      // TODO(renesugar): Fixed size binary stored a kCHAR. Are there character
      //                  set conversions that interfere with storing binary
      //                  data in strings?
      cd->columnType.set_type(kCHAR);
      cd->columnType.set_size(typed_datatype->byte_width());
      break;
    }
    // int32_t days since the UNIX epoch
    case ::arrow::Type::DATE32: {
      // NOTE: When importing from Parquet, ::arrow::Type::DATE32: values will
      //       have to be scaled to seconds from DateUnit:
      //
      //       ::arrow::DateUnit::DAY, ::arrow::DateUnit::MILLI
      //
      //       auto typed_datatype = static_cast<arrow::Date32Type*>(field->type().get());
      //       typed_datatype->unit();
      //       typed_datatype->bit_width();
      cd->columnType.set_type(kDATE);
      break;
    }
    // int64_t milliseconds since the UNIX epoch
    case ::arrow::Type::DATE64: {
      // NOTE: When importing from Parquet, ::arrow::Type::DATE64: values will
      //       have to be scaled to seconds from DateUnit:
      //
      //       ::arrow::DateUnit::DAY, ::arrow::DateUnit::MILLI
      //
      //       auto typed_datatype = static_cast<arrow::Date64Type*>(field->type().get());
      //       typed_datatype->unit();
      //       typed_datatype->bit_width();
      cd->columnType.set_type(kDATE);
      break;
    }
    // Exact timestamp encoded with int64 since UNIX epoch
    // Default unit millisecond
    case ::arrow::Type::TIMESTAMP: {
      auto typed_datatype = static_cast<arrow::TimestampType*>(field->type().get());

      // TODO(renesugar): Add units to SQLTypeInfo for kTIMESTAMP type to MapD?

      // TODO(renesugar): When importing from Parquet, ::arrow::Type::TIMESTAMP:
      //                  values will have to be scaled to
      //                  TimestampType::Unit::SECOND.

      // TODO(renesugar): Add timezone to SQLTypeInfo for kTIMESTAMP type to MapD?
      //                  timestamp_type->timezone();
      switch (typed_datatype->unit()) {
        case ::arrow::TimestampType::Unit::SECOND:
          cd->columnType.set_type(kTIMESTAMP);
          break;
        case ::arrow::TimestampType::Unit::MILLI:
          cd->columnType.set_type(kTIMESTAMP);
          break;
        case ::arrow::TimestampType::Unit::MICRO:
          cd->columnType.set_type(kTIMESTAMP);
          break;
        case ::arrow::TimestampType::Unit::NANO:
          cd->columnType.set_type(kTIMESTAMP);
          break;
      }
      break;
    }
    // Time as signed 32-bit integer, representing either seconds or
    // milliseconds since midnight
    case ::arrow::Type::TIME32: {
      // NOTE: When importing from Parquet, ::arrow::Type::TIME32: values will
      //       have to be scaled to TimestampType::Unit::SECOND.
      //       auto typed_datatype = static_cast<arrow::Time32Type*>(field->type().get());
      cd->columnType.set_type(kTIME);
      break;
    }
    // Time as signed 64-bit integer, representing either microseconds or
    // nanoseconds since midnight
    case ::arrow::Type::TIME64: {
      // NOTE: When importing from Parquet, ::arrow::Type::TIME64Í: values will
      //       have to be scaled to TimestampType::Unit::SECOND.
      //       auto typed_datatype = static_cast<arrow::Time64Type*>(field->type().get());
      cd->columnType.set_type(kTIME);
      break;
    }
    // YEAR_MONTH or DAY_TIME interval in SQL style
    case ::arrow::Type::INTERVAL: {
      auto typed_datatype = static_cast<arrow::IntervalType*>(field->type().get());
      switch (typed_datatype->unit()) {
        case ::arrow::IntervalType::Unit::YEAR_MONTH:
          cd->columnType.set_type(kINTERVAL_YEAR_MONTH);
          break;
        case ::arrow::IntervalType::Unit::DAY_TIME:
          cd->columnType.set_type(kINTERVAL_DAY_TIME);
          break;
      }
      break;
    }
    // Precision- and scale-based decimal type. Storage type depends on the
    // parameters.
    case ::arrow::Type::DECIMAL: {
      auto typed_datatype = static_cast<arrow::DecimalType*>(field->type().get());
      cd->columnType.set_type(kDECIMAL);
      cd->columnType.set_precision(typed_datatype->precision());
      cd->columnType.set_scale(typed_datatype->scale());
      break;
    }
    // A list of some logical data type
    case ::arrow::Type::LIST: {
      auto typed_datatype = static_cast<arrow::ListType*>(field->type().get());
      ColumnDescriptor value_cd;

      // List<UInt8> or List<Int8>
      if ((typed_datatype->value_field()->type()->id() == ::arrow::Type::UINT8) ||
          (typed_datatype->value_field()->type()->id() == ::arrow::Type::INT8)) {
        // Store List<UInt8> or List<Int8> as binary array (stored as a kVARCHAR)
        //
        // TODO(renesugar): Add support for TINYINT to MapD?
        //                  is_null() in sqltypes.h uses NULL_TINYINT
        //
        // References:
        // (1) https://docs.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql
        // (2) https://www.postgresql.org/docs/current/static/datatype.html
        // (3) http://docs.oracle.com/database/122/LNPLS/plsql-data-types.htm#LNPLS003
        // (4) https://www.ibm.com/support/knowledgecenter/en/SSEPEK_12.0.0/sqlref/src/tpc/db2z_datatypesintro.html
        // (5) https://dev.mysql.com/doc/refman/5.7/en/data-types.html
        //
        cd->columnType.set_type(kVARCHAR);
        cd->columnType.set_size(-1);
      } else {
        // Store list as an array
        status = ArrowTypeToSQLType(typed_datatype->value_field(), &value_cd);
        if (status.ok()) {
          cd->columnType.set_type(kARRAY);
          cd->columnType.set_subtype(value_cd.columnType.get_type());
          cd->columnType.set_dimension(value_cd.columnType.get_dimension());
          cd->columnType.set_precision(value_cd.columnType.get_precision());
          cd->columnType.set_scale(value_cd.columnType.get_scale());
          cd->columnType.set_notnull(value_cd.columnType.get_notnull());
          cd->columnType.set_size(-1);
          // cd->columnType.set_fixed_size();
          cd->columnType.set_compression(value_cd.columnType.get_compression());
          cd->columnType.set_comp_param(value_cd.columnType.get_comp_param());
        }
      }
      break;
    }
    // Dictionary aka Category type
    case ::arrow::Type::DICTIONARY: {
      // NOTE: https://arrow.apache.org/docs/memory_layout.html#dictionary-encoding
      auto typed_datatype = static_cast<arrow::DictionaryType*>(field->type().get());
      ColumnDescriptor value_cd;
      std::shared_ptr<::arrow::Field> value_field( new ::arrow::Field(
                           field->name(),
                           typed_datatype->dictionary()->type(),
                           field->nullable(),
                           nullptr));

      // Get the type of data stored in the dictionary
      status = ArrowTypeToSQLType(value_field, &value_cd);
      if (status.ok()) {
        cd->columnType.set_type(value_cd.columnType.get_type());
        cd->columnType.set_dimension(value_cd.columnType.get_dimension());
        cd->columnType.set_precision(value_cd.columnType.get_precision());
        cd->columnType.set_scale(value_cd.columnType.get_scale());
        cd->columnType.set_notnull(value_cd.columnType.get_notnull());
        cd->columnType.set_size(-1);
        // cd->columnType.set_fixed_size();
        cd->columnType.set_compression(kENCODING_NONE);
        cd->columnType.set_comp_param(0);

        // TODO(renesugar): Should the decision to dictionary encode a column
        //                  be made at this point or when writing to MapD?
        //
        //                  Indices used by MapD and Arrow are not interchangeable.
        //

        /*
        if (typed_datatype->dictionary()->type()->id() == ::arrow::Type::STRING) {
          // NOTE: Arrow has more dictionary encoded types than MapD which only
          // dictonary encodes strings.
          cd->columnType.set_compression(kENCODING_DICT);
          // NOTE: 32-bit index to dictionary is required for compatibility with
          //       some features of Arrow.
          cd->columnType.set_comp_param(8 * DataTypeCTypeByteLength(
            typed_datatype->indices()->type()->id()));
        }
        */
      }
      break;
    }
    // Struct of logical types
    case ::arrow::Type::STRUCT: {
      status = ::arrow::Status::NotImplemented("Struct not implemented in " __FILE__ " at " STR__LINE__);
      break;
    }
    // Unions of logical types
    case ::arrow::Type::UNION: {
      status = ::arrow::Status::NotImplemented("Union not implemented in " __FILE__ " at " STR__LINE__);
      break;
    }
    // unknown type
    default:
      status = ::arrow::Status::TypeError("Unknown type in " __FILE__ " at " STR__LINE__);
  }

  return status;
}

int64_t DataTypeCTypeByteLength(::arrow::Type::type id) {
  // TODO(renesugar): FIXED_SIZE_BINARY, TIMESTAMP, TIME32, TIME64, INTERVAL, LIST,
  //                  STRUCT, UNION and DICTIONARY don't have "(parameter-free)
  //                  Factory functions" found in type_fwd.h

  // Returns the size of the C type or -1 if the data type does not fit in a C type.

  switch (id) {
    case ::arrow::Type::NA:
      return -1;
    case ::arrow::Type::BOOL:
      return sizeof(bool);
    case ::arrow::Type::UINT8:
    case ::arrow::Type::INT8:
      return sizeof(uint8_t);
    case ::arrow::Type::UINT16:
    case ::arrow::Type::INT16:
      return sizeof(uint16_t);
    case ::arrow::Type::UINT32:
    case ::arrow::Type::INT32:
      return sizeof(uint32_t);
    case ::arrow::Type::UINT64:
    case ::arrow::Type::INT64:
      return sizeof(uint64_t);
    case ::arrow::Type::HALF_FLOAT:
      // store as a float in MapD
    case ::arrow::Type::FLOAT:
      return sizeof(float);
    case ::arrow::Type::DOUBLE:
      return sizeof(double);
    case ::arrow::Type::STRING:
    case ::arrow::Type::BINARY:
    case ::arrow::Type::FIXED_SIZE_BINARY:
      return -1;
    case ::arrow::Type::DATE32:
      return sizeof(int32_t);
    case ::arrow::Type::DATE64:
      return sizeof(int64_t);
    case ::arrow::Type::TIMESTAMP:
      return sizeof(int64_t);
    case ::arrow::Type::TIME32:
      return sizeof(int32_t);
    case ::arrow::Type::TIME64:
      return sizeof(int64_t);
    case ::arrow::Type::INTERVAL:
      return sizeof(int64_t);
    case ::arrow::Type::DECIMAL:
      // Size will get narrowed using specific type information before allocation takes place.
      // Return size that represents the maximum size used by MapD to store a decimal.
      // Parquet will store larger decimal values in a fixed size binary column.
      return sizeof(int64_t);
    case ::arrow::Type::LIST:
      // List<UInt8> or List<Int8> converted to a binary type
      // (stored as kVARCHAR) (see ArrowTypeToSQLType)
    case ::arrow::Type::STRUCT:
    case ::arrow::Type::UNION:
    case ::arrow::Type::DICTIONARY:
      // For DICTIONARY type, underlying type is used and compression set to kENCODING_DICT
    default:
      return -1;
  }
}

bool IsArrowScalarColumn(const std::shared_ptr<::arrow::Column>& col) {
  ::arrow::Type::type type_id = col->type()->id();
  switch (type_id) {
    case ::arrow::Type::NA:
    case ::arrow::Type::BOOL:
    case ::arrow::Type::UINT8:
    case ::arrow::Type::INT8:
    case ::arrow::Type::UINT16:
    case ::arrow::Type::INT16:
    case ::arrow::Type::UINT32:
    case ::arrow::Type::INT32:
    case ::arrow::Type::UINT64:
    case ::arrow::Type::INT64:
    case ::arrow::Type::HALF_FLOAT:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DOUBLE:
    case ::arrow::Type::STRING:
    case ::arrow::Type::BINARY:
    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::DATE32:
    case ::arrow::Type::DATE64:
    case ::arrow::Type::TIMESTAMP:
    case ::arrow::Type::TIME32:
    case ::arrow::Type::TIME64:
    case ::arrow::Type::INTERVAL:
    case ::arrow::Type::DECIMAL:
    case ::arrow::Type::DICTIONARY:
      // For DICTIONARY type, underlying type is used and compression set to kENCODING_DICT
      return true;
    case ::arrow::Type::LIST:
    case ::arrow::Type::STRUCT:
    case ::arrow::Type::UNION:
    default:
      return false;
  }
}

bool IsArrowListColumn(const std::shared_ptr<::arrow::Column>& col) {
  ::arrow::Type::type type_id = col->type()->id();

  if (type_id == ::arrow::Type::LIST) {
    return true;
  }

  return false;
}

/*
 //       TODO(renesugar): Indices could be implemented on in-memory columns for queries.
 //
 //       Comparisions on strings in dictionary columns could be done by
 //       comparing two integers with an order-preserving perfect hash function.
 //
 //       If the table is updated infrequently, an order-preserving
 //       perfect hash function can be used as the string dictionary. The data
 //       returned by the hash function is the string's index in sorted order.
 //
 //       http://iswsa.acm.org/mphf/index.html
 //       Practical Perfect Hashing for very large Key-Value Databases
 //
 //       https://github.com/julian-klode/triehash
 //       https://goo.gl/6ChdxJ
 //       TrieHash, a order-preserving minimal perfect hash function generator for C(++)
 //
 //       https://goo.gl/e6fHT1
 //       Practical perfect hashing
 //       GV Cormack, RNS Horspool, M Kaiserswerth - The Computer Journal, 1985
 //       https://github.com/renesugar/pph-cpp
 //
 //       http://burtleburtle.net/bob/hash/perfect.html
 //       External Perfect Hashing for Very Large Key Sets
 //       FC Botelho, N Ziviani - Lisbon, Portugal (CIKM '07)
 //
 //       https://sdm.lbl.gov/fastbit/index.html
 //       FastBit implements a set of alternative indexes called compressed bitmap indexes.
 //
 //       https://openproceedings.org/2014/conf/edbt/0002RF14.pdf
 //       Adaptive String Dictionary Compression in In-Memory Column-Store Database Systems
 //
 */

#define DICTIONARY_NUMERIC_ARRAY_CASE(ENUM, TYPE, C_TYPE)                   \
  case ::arrow::Type::ENUM: {                                               \
    ARROW_RETURN_NOT_OK((CopyNumericArrayDictionarySlice<::arrow::TYPE, C_TYPE>( \
      0,                                                                    \
      num_values,                                                           \
      indices,                                                              \
      dictionary,                                                           \
      builder)));                                                           \
    ARROW_RETURN_NOT_OK(builder->Finish(out));                              \
    return ::arrow::Status::OK();                                           \
  }

::arrow::Status DecodeDictionaryToArray(const ::arrow::Array& input,
                                        ::arrow::MemoryPool* pool,
                                        std::shared_ptr<::arrow::Array>* out) {
  if (input.type()->id() != ::arrow::Type::DICTIONARY) {
    // return an error if the array type is not DictionaryType
    return ::arrow::Status::TypeError("Invalid parameter in " __FILE__ " at " STR__LINE__);
  }

  const auto& dict_array = static_cast<const ::arrow::DictionaryArray&>(input);

  ::arrow::Type::type index_type_id = dict_array.indices()->type()->id();
  if (!::arrow::is_integer(index_type_id)) {
    return ::arrow::Status::Invalid("Dictionary indices must be an integer type");
  }

  std::unique_ptr<::arrow::ArrayBuilder> builder;
  std::shared_ptr<::arrow::Array> indices = dict_array.indices();
  std::shared_ptr<::arrow::Array> dictionary = dict_array.dictionary();
  std::shared_ptr<::arrow::DataType> value_type = dictionary->type();
  int64_t num_values = indices->length();

  ARROW_RETURN_NOT_OK(::arrow::MakeBuilder(pool, value_type, &builder));

  switch (value_type->id()) {
    DICTIONARY_NUMERIC_ARRAY_CASE(UINT8,  UInt8Type,  uint8_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(INT8,   Int8Type,   int8_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(UINT16, UInt16Type, uint16_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(INT16,  Int16Type,  int16_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(UINT32, UInt32Type, uint32_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(INT32,  Int32Type,  int32_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(UINT64, UInt64Type, uint64_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(INT64,  Int64Type,  int64_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(HALF_FLOAT,  HalfFloatType,  uint16_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(FLOAT,       FloatType,      float);
    DICTIONARY_NUMERIC_ARRAY_CASE(DOUBLE,      DoubleType,     double);
    DICTIONARY_NUMERIC_ARRAY_CASE(DATE32,      Date32Type,     int32_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(DATE64,      Date64Type,     int64_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(TIMESTAMP,   TimestampType,  int64_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(TIME32,      Time32Type,     int32_t);
    DICTIONARY_NUMERIC_ARRAY_CASE(TIME64,      Time64Type,     int64_t);

    case ::arrow::Type::STRING: {
      ARROW_RETURN_NOT_OK((CopyStringArrayDictionarySlice<::arrow::StringType, std::string>(
                                                                    0,
                                                                    num_values,
                                                                    indices,
                                                                    dictionary,
                                                                    builder)));
      ARROW_RETURN_NOT_OK(builder->Finish(out));
      return ::arrow::Status::OK();
    }

    case ::arrow::Type::BINARY: {
      ARROW_RETURN_NOT_OK((CopyBinaryArrayDictionarySlice<::arrow::BinaryType, uint8_t>(
                                                                    0,
                                                                    num_values,
                                                                    indices,
                                                                    dictionary,
                                                                    builder)));
      ARROW_RETURN_NOT_OK(builder->Finish(out));
      return ::arrow::Status::OK();
    }

    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::DECIMAL:
    case ::arrow::Type::NA:
    case ::arrow::Type::BOOL:
    case ::arrow::Type::INTERVAL:
    case ::arrow::Type::LIST:
    case ::arrow::Type::STRUCT:
    case ::arrow::Type::UNION:
    case ::arrow::Type::DICTIONARY:
    default:
      return ::arrow::Status::NotImplemented("Not implemented in " __FILE__ " at " STR__LINE__);
  }

  return ::arrow::Status::OK();
}

#define DICTIONARY_NUMERIC_COLUMN_CASE(ENUM, TYPE, C_TYPE)                    \
  case ::arrow::Type::ENUM: {                                                 \
    for (auto chunk : chunks->chunks()) {                                     \
      const auto& dict_array = static_cast<const ::arrow::DictionaryArray&>(*chunk); \
      std::shared_ptr<::arrow::Array> indices = dict_array.indices();               \
      std::shared_ptr<::arrow::Array> dictionary = dict_array.dictionary();         \
      int64_t num_values = indices->length();                                 \
      ARROW_RETURN_NOT_OK((CopyNumericArrayDictionarySlice<::arrow::TYPE, C_TYPE>( \
            0,                                                                \
            num_values,                                                       \
            indices,                                                          \
            dictionary,                                                       \
            builder)));                                                       \
    }                                                                         \
    ARROW_RETURN_NOT_OK(builder->Finish(&arr));                               \
    *out = std::make_shared<::arrow::Column>(input.name(), arr);              \
    return ::arrow::Status::OK();                                             \
  }

::arrow::Status DecodeDictionaryToColumn(const ::arrow::Column& input,
                                         ::arrow::MemoryPool* pool,
                                         std::shared_ptr<::arrow::Column>* out) {
  std::unique_ptr<::arrow::ArrayBuilder> builder;
  std::shared_ptr<::arrow::Array> arr;
  std::shared_ptr<::arrow::ChunkedArray> chunks = input.data();

  if (input.type()->id() != ::arrow::Type::DICTIONARY) {
    // return an error if the array type is not DictionaryType
    return ::arrow::Status::TypeError("Invalid parameter in " __FILE__ " at " STR__LINE__);
  }

  const auto& dict_array = static_cast<const ::arrow::DictionaryArray&>(*(chunks->chunk(0)));

  ::arrow::Type::type index_type_id = dict_array.indices()->type()->id();
  if (!::arrow::is_integer(index_type_id)) {
    return ::arrow::Status::Invalid("Dictionary indices must be an integer type");
  }

  std::shared_ptr<::arrow::DataType> value_type = dict_array.dictionary()->type();

  ARROW_RETURN_NOT_OK(::arrow::MakeBuilder(pool, value_type, &builder));

  switch (value_type->id()) {
    DICTIONARY_NUMERIC_COLUMN_CASE(UINT8,  UInt8Type,  uint8_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(INT8,   Int8Type,   int8_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(UINT16, UInt16Type, uint16_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(INT16,  Int16Type,  int16_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(UINT32, UInt32Type, uint32_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(INT32,  Int32Type,  int32_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(UINT64, UInt64Type, uint64_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(INT64,  Int64Type,  int64_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(HALF_FLOAT,  HalfFloatType,  uint16_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(FLOAT,       FloatType,      float);
    DICTIONARY_NUMERIC_COLUMN_CASE(DOUBLE,      DoubleType,     double);
    DICTIONARY_NUMERIC_COLUMN_CASE(DATE32,      Date32Type,     int32_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(DATE64,      Date64Type,     int64_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(TIMESTAMP,   TimestampType,  int64_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(TIME32,      Time32Type,     int32_t);
    DICTIONARY_NUMERIC_COLUMN_CASE(TIME64,      Time64Type,     int64_t);

    case ::arrow::Type::STRING: {
      for (auto chunk : chunks->chunks()) {
        const auto& dict_array = static_cast<const ::arrow::DictionaryArray&>(*chunk);
        std::shared_ptr<::arrow::Array> indices = dict_array.indices();
        std::shared_ptr<::arrow::Array> dictionary = dict_array.dictionary();
        int64_t num_values = indices->length();
        ARROW_RETURN_NOT_OK((CopyStringArrayDictionarySlice<::arrow::StringType, std::string>(
                                                                                   0,
                                                                                   num_values,
                                                                                   indices,
                                                                                   dictionary,
                                                                                   builder)));
      }
      ARROW_RETURN_NOT_OK(builder->Finish(&arr));
      *out = std::make_shared<::arrow::Column>(input.name(), arr);
      return ::arrow::Status::OK();
    }

    case ::arrow::Type::BINARY: {
      for (auto chunk : chunks->chunks()) {
        const auto& dict_array = static_cast<const ::arrow::DictionaryArray&>(*chunk);
        std::shared_ptr<::arrow::Array> indices = dict_array.indices();
        std::shared_ptr<::arrow::Array> dictionary = dict_array.dictionary();
        int64_t num_values = indices->length();
        ARROW_RETURN_NOT_OK((CopyBinaryArrayDictionarySlice<::arrow::BinaryType, uint8_t>(
                                                                                         0,
                                                                                         num_values,
                                                                                         indices,
                                                                                         dictionary,
                                                                                         builder)));
      }
      ARROW_RETURN_NOT_OK(builder->Finish(&arr));
      *out = std::make_shared<::arrow::Column>(input.name(), arr);
      return ::arrow::Status::OK();
    }

    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::DECIMAL:
    case ::arrow::Type::NA:
    case ::arrow::Type::BOOL:
    case ::arrow::Type::INTERVAL:
    case ::arrow::Type::LIST:
    case ::arrow::Type::STRUCT:
    case ::arrow::Type::UNION:
    case ::arrow::Type::DICTIONARY:
    default:
      return ::arrow::Status::NotImplemented("Not implemented in " __FILE__ " at " STR__LINE__);
  }
  
  return ::arrow::Status::OK();
}

::arrow::Status CopyArrowListColumn(const std::shared_ptr<::arrow::Column>& col,
                                    TypedImportBuffer* tib) {
  const ::arrow::ChunkedArray& data = *col->data().get();

  if (col->type()->id() != ::arrow::Type::LIST) {
    // return an error if the column type is not ListType
    return ::arrow::Status::TypeError("Invalid parameter in " __FILE__ " at " STR__LINE__);
  }

  auto list_type = std::static_pointer_cast<::arrow::ListType>(col->type());

  // Assumption: TypedImportBuffer type information was created from Arrow column type information.

  std::shared_ptr<::arrow::DataType> elem_type = list_type->value_type();

  bool is_null = false;

  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = std::static_pointer_cast<::arrow::ListArray>(data.chunk(c));

    const bool has_nulls = data.null_count() > 0;

    for (int64_t i = 0; i < arr->length(); ++i) {
      is_null = (has_nulls && arr->IsNull(i));

      if (is_null) {
        // add null value to TypedImportBuffer
        if ((elem_type->id() == ::arrow::Type::UINT8) ||
            (elem_type->id() == ::arrow::Type::UINT8)) {
          tib->addString(std::string());
        } else if (IS_STRING(tib->getColumnDesc()->columnType.get_subtype())) {
          tib->addStringArray();
        } else {
          tib->addArray(ArrayDatum(0, NULL, true));
        }

        continue;
      } else {
        auto value_arr = std::static_pointer_cast<::arrow::Array>(arr->Slice(arr->value_offset(i),
                                                                    arr->value_length(i)));

        std::vector<std::string> string_vec;
        std::unique_ptr<int8_t, CheckedAllocDeleter> buf_ptr(nullptr);

        int     c_type_size = DataTypeCTypeByteLength(elem_type->id());
        size_t  len = 0;
        int8_t* buf = nullptr;
        int8_t* p   = nullptr;

        if (c_type_size > 0) {
          len = value_arr->length() * c_type_size;
          buf_ptr.reset(static_cast<int8_t*>(checked_malloc(len)));
          buf = buf_ptr.get();
          p   = buf;
        }

        if ((elem_type->id() == ::arrow::Type::UINT8) ||
            (elem_type->id() == ::arrow::Type::UINT8)) {
          // Store list of UINT8 or INT8 as a binary type (stored as a string in MapD)
          auto typed_array = static_cast<::arrow::Int8Array*>(value_arr.get());
          for (int64_t j = 0; j < typed_array->length(); ++i) {
            is_null = (has_nulls && typed_array->IsNull(i));
            int8_t value = typed_array->Value(i);
            if (is_null) {
              *reinterpret_cast<int8_t*>(p) = 0;
            } else {
              *reinterpret_cast<int8_t*>(p) = value;
            }
            p = p + sizeof(int8_t);
          }

          // NOTE: Specify length in std::string constructor so binary string
          //       can contain null characters.
          tib->addString(std::string(reinterpret_cast<char const*>(buf), len));

          // List<INT8> or List<UINT8> was stored as a string instead of an array
          continue;
        }

        // Arrow methods to get the value from an array vary by type
        //
        // value_type  NumericArray<type>::Value(i)
        // NumericArray<Int8Type>;
        // NumericArray<UInt8Type>;
        // NumericArray<Int16Type>;
        // NumericArray<UInt16Type>;
        // NumericArray<Int32Type>;
        // NumericArray<UInt32Type>;
        // NumericArray<Int64Type>;
        // NumericArray<UInt64Type>;
        // NumericArray<HalfFloatType>;
        // NumericArray<FloatType>;
        // NumericArray<DoubleType>;
        // NumericArray<Date32Type>;
        // NumericArray<Date64Type>;
        // NumericArray<Time32Type>;
        // NumericArray<Time64Type>;
        // NumericArray<TimestampType>;
        // value_type  BooleanArray::Value(i)
        // uint8_t *   BinaryArray::GetValue(i, &length)
        // std::string StringArray::GetString(i)
        // uint8_t *   FixedSizeBinary::GetValue(i)  int32_t byte_width()  (DecimalType)
        //
        //
        for (int64_t j = 0; j < value_arr->length(); ++j) {
          is_null = value_arr->IsNull(j);

          switch (elem_type->id()) {
            case ::arrow::Type::NA:
              // MapD does not store kNULL columns
              break;
            case ::arrow::Type::BOOL: {
              // kBOOLEAN
              if (is_null) {
                *reinterpret_cast<bool*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::BooleanArray*>(value_arr.get());
                bool value = typed_array->Value(j);

                *reinterpret_cast<bool*>(p) = value;
              }
              p = p + sizeof(bool);
              break;
            }
            case ::arrow::Type::UINT8:
            case ::arrow::Type::INT8: {
              // A list of 8-bit characters stored as a string containing binary data.
              break;
            }
            // TODO(renesugar): Arrow has unsigned integer types while MapD
            //                  stores signed integer types.
            case ::arrow::Type::UINT16: {
              // kSMALLINT
              if (is_null) {
                *reinterpret_cast<uint16_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::UInt16Array*>(value_arr.get());
                uint16_t value = typed_array->Value(j);

                *reinterpret_cast<uint16_t*>(p) = value;
              }
              p = p + sizeof(uint16_t);
              break;
            }
            case ::arrow::Type::INT16: {
              // kSMALLINT
              if (is_null) {
                *reinterpret_cast<int16_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Int16Array*>(value_arr.get());
                int16_t value = typed_array->Value(j);

                *reinterpret_cast<int16_t*>(p) = value;
              }
              p = p + sizeof(int16_t);
              break;
            }
            case ::arrow::Type::UINT32: {
              // kINT
              if (is_null) {
                *reinterpret_cast<uint32_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::UInt32Array*>(value_arr.get());
                uint32_t value = typed_array->Value(j);

                *reinterpret_cast<uint32_t*>(p) = value;
              }
              p = p + sizeof(uint32_t);
              break;
            }
            case ::arrow::Type::INT32: {
              // kINT
              if (is_null) {
                *reinterpret_cast<int32_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Int32Array*>(value_arr.get());
                int32_t value = typed_array->Value(j);

                *reinterpret_cast<int32_t*>(p) = value;
              }
              p = p + sizeof(int32_t);
              break;
            }
            case ::arrow::Type::UINT64: {
              // kBIGINT
              // kNUMERIC
              if (is_null) {
                *reinterpret_cast<uint64_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::UInt64Array*>(value_arr.get());
                uint64_t value = typed_array->Value(j);

                *reinterpret_cast<uint64_t*>(p) = value;
              }
              p = p + sizeof(uint64_t);
              break;
            }
            case ::arrow::Type::INT64: {
              // kBIGINT
              // kNUMERIC
              if (is_null) {
                *reinterpret_cast<int64_t*>(p) =
                inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Int64Array*>(value_arr.get());
                int64_t value = typed_array->Value(j);

                *reinterpret_cast<int64_t*>(p) = value;
              }
              p = p + sizeof(int64_t);
              break;
            }
            case ::arrow::Type::HALF_FLOAT: {
              // kFLOAT
              if (is_null) {
                *reinterpret_cast<float*>(p) = NULL_FLOAT;
              } else {
                auto typed_array = static_cast<::arrow::HalfFloatArray*>(value_arr.get());

                // Convert from half-float to float
                //
                // References:
                //
                // https://stackoverflow.com/questions/1659440/32-bit-to-16-bit-floating-point-conversion
                // https://github.com/numpy/numpy/blob/master/numpy/core/src/npymath/halffloat.c#L466

                float value = static_cast<float>(halfbits_to_floatbits(typed_array->Value(j)));

                *reinterpret_cast<float*>(p) = value;
              }
              p = p + sizeof(float);
              break;
            }
            case ::arrow::Type::FLOAT: {
              // kFLOAT
              if (is_null) {
                *reinterpret_cast<float*>(p) = NULL_FLOAT;
              } else {
                auto typed_array = static_cast<::arrow::FloatArray*>(value_arr.get());
                float value = typed_array->Value(j);

                *reinterpret_cast<float*>(p) = value;
              }
              p = p + sizeof(float);
              break;
            }
            case ::arrow::Type::DOUBLE: {
              // kDOUBLE
              if (is_null) {
                *reinterpret_cast<double*>(p) = NULL_DOUBLE;
              } else {
                auto typed_array = static_cast<::arrow::DoubleArray*>(value_arr.get());
                double value = typed_array->Value(j);

                *reinterpret_cast<double*>(p) = value;
              }
              p = p + sizeof(double);
              break;
            }
            case ::arrow::Type::STRING: {
              // kTEXT
              // kVARCHAR
              // kCHAR
              // @TODO(wei) for now, use empty string for nulls
              if (is_null) {
                string_vec.push_back(std::string());
              } else {
                auto typed_array = static_cast<::arrow::StringArray*>(value_arr.get());
                std::string value = typed_array->GetString(j);

                string_vec.push_back(value);
              }
              break;
            }
            case ::arrow::Type::BINARY: {
              // kVARCHAR
              if (is_null) {
                string_vec.push_back(std::string());
              } else {
                auto typed_array = static_cast<::arrow::BinaryArray*>(value_arr.get());
                int32_t length = 0;
                const uint8_t* value = typed_array->GetValue(i, &length);

                string_vec.push_back(std::string(reinterpret_cast<char const*>(value), length));
              }
              break;
            }
            case ::arrow::Type::FIXED_SIZE_BINARY: {
              // kCHAR
              if (is_null) {
                string_vec.push_back(std::string());
              } else {
                auto typed_array = static_cast<::arrow::FixedSizeBinaryArray*>(value_arr.get());
                int32_t length = typed_array->byte_width();
                const uint8_t* value = typed_array->GetValue(j);

                string_vec.push_back(std::string(reinterpret_cast<char const*>(value), length));
              }
              break;
            }
            // TODO(renesugar): Arrow has not implemented IntervalArray
            /*case ::arrow::Type::INTERVAL: {
               // kINTERVAL_DAY_TIME
               // kINTERVAL_YEAR_MONTH
               if (is_null) {
                 *reinterpret_cast<int64_t*>(p) =
                 inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
               } else {
                 auto typed_array = static_cast<::arrow::IntervalArray*>(value_arr.get());
                 auto typed_datatype = static_cast<::arrow::IntervalType*>(typed_array->type().get());
                 int64_t value = typed_array->Value(j);

                 switch (typed_datatype->unit()) {
                 case ::arrow::IntervalType::Unit::YEAR_MONTH:
                 // no unit conversion necessary
                 break;
                 case ::arrow::IntervalType::Unit::DAY_TIME:
                 // no unit conversion necessary
                 break;
                 default:
                 break;
                 }

                 *reinterpret_cast<int64_t*>(p) = value;
               }
               p = p + sizeof(int64_t);
               break;
             }*/
            case ::arrow::Type::DECIMAL: {
              const uint8_t* value = nullptr;
              int16_t  value_int16 = 0;
              int32_t  value_int32 = 0;
              int64_t  value_int64 = 0;
              /*int128_t  value_int128 = 0;*/
              auto typed_array = static_cast<::arrow::FixedSizeBinaryArray*>(value_arr.get());
              int32_t length = typed_array->byte_width();

              switch (length) {
                case 2: {
                  if (is_null) {
                    *reinterpret_cast<int16_t*>(p) =
                    inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
                  } else {
                    value = typed_array->GetValue(j);

                    value_int16 = *reinterpret_cast<int16_t*>(const_cast<uint8_t*>(value));

                    *reinterpret_cast<int16_t*>(p) = value_int16;
                  }
                  p = p + sizeof(int16_t);
                  break;
                }
                case 4: {
                  if (is_null) {
                    *reinterpret_cast<int32_t*>(p) =
                    inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
                  } else {
                    value = typed_array->GetValue(j);

                    value_int32 = *reinterpret_cast<int32_t*>(const_cast<uint8_t*>(value));

                    *reinterpret_cast<int32_t*>(p) = value_int32;
                  }
                  p = p + sizeof(int32_t);
                  break;
                }
                case 8: {
                  if (is_null) {
                    *reinterpret_cast<int64_t*>(p) =
                   inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
                  } else {
                    value = typed_array->GetValue(j);

                    value_int64 = *reinterpret_cast<int64_t*>(const_cast<uint8_t*>(value));

                    *reinterpret_cast<int64_t*>(p) = value_int64;
                  }
                  p = p + sizeof(int64_t);
                  break;
                }
                // TODO(renesugar): MapD currently does not support 128-bit decimal values.
                //                  inline_fixed_encoding_null_val() will have to be updated.
                /*
                 case 16: {
                 if (is_null) {
                   *reinterpret_cast<int128_t*>(p) =
                   inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
                 } else {
                   value = typed_array->GetValue(j);

                   value_int128 = *reinterpret_cast<int128_t*>(const_cast<uint8_t*>(value));

                   *reinterpret_cast<int128_t*>(p) = value_int128;
                 }
                 p = p + sizeof(int128_t);
                 break;
                 }
                 */
                default:
                  break;
              }
              break;
            }
            case ::arrow::Type::TIME32: {
              // kTIME:
              if (is_null) {
                *reinterpret_cast<time_t*>(p) =
               inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Time32Array*>(value_arr.get());
                auto typed_datatype = static_cast<::arrow::Time32Type*>(typed_array->type().get());
                time_t value = typed_array->Value(j);

                switch (typed_datatype->unit()) {
                  case ::arrow::TimeUnit::SECOND:
                    // no unit conversion necessary
                    break;
                  case ::arrow::TimeUnit::MILLI:
                    // convert milliseconds to seconds
                    value = value / 1000;
                    break;
                    /*
                     case ::arrow::TimeUnit::MICRO:
                     // convert microseconds to seconds
                     value = value / 1000000;
                     break;
                     case ::arrow::TimeUnit::NANO:
                     // convert nanoseconds to seconds
                     value = value / 1000000000;
                     break;
                     */
                  default:
                    break;
                }

                *reinterpret_cast<time_t*>(p) = value;
              }
              p = p + sizeof(time_t);
              break;
            }
            case ::arrow::Type::TIME64: {
              // kTIME:
              if (is_null) {
                *reinterpret_cast<time_t*>(p) =
               inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Time64Array*>(value_arr.get());
                auto typed_datatype = static_cast<::arrow::Time64Type*>(typed_array->type().get());
                time_t value = typed_array->Value(j);

                switch (typed_datatype->unit()) {
                  case ::arrow::TimeUnit::SECOND:
                    // no unit conversion necessary
                    break;
                  case ::arrow::TimeUnit::MILLI:
                    // convert milliseconds to seconds
                    value = value / 1000;
                    break;
                  case ::arrow::TimeUnit::MICRO:
                    // convert microseconds to seconds
                    value = value / 1000000;
                    break;
                  case ::arrow::TimeUnit::NANO:
                    // convert nanoseconds to seconds
                    value = value / 1000000000;
                    break;
                  default:
                    break;
                }

                *reinterpret_cast<time_t*>(p) = value;
              }
              p = p + sizeof(time_t);
              break;
            }
            case ::arrow::Type::DATE32: {
              // kDATE
              if (is_null) {
                *reinterpret_cast<time_t*>(p) =
               inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Date32Array*>(value_arr.get());
                time_t value = typed_array->Value(j);

                *reinterpret_cast<time_t*>(p) = value;
              }
              p = p + sizeof(time_t);
              break;
            }
            case ::arrow::Type::DATE64: {
              // kDATE
              if (is_null) {
                *reinterpret_cast<time_t*>(p) =
               inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::Date64Array*>(value_arr.get());
                time_t value = typed_array->Value(j);

                // convert milliseconds to seconds
                value = value / 1000;

                *reinterpret_cast<time_t*>(p) = value;
              }
              p = p + sizeof(time_t);
              break;
            }
            case ::arrow::Type::TIMESTAMP: {
              // kTIMESTAMP:
              // TODO(renesugar): Add units to SQLTypeInfo for kTIMESTAMP type to MapD?

              // TODO(renesugar): Add timezone to SQLTypeInfo for kTIMESTAMP type to MapD?
              //                  timestamp_type->timezone();

              // TODO(renesugar): Arrow uses int64_t for timestamp values; MapD uses time_t.
              //       Use int64_t for timestamps in MapD?
              //       https://www.mapd.com/docs/latest/mapd-core-guide/fixed-encoding/
              //       https://sourceware.org/glibc/wiki/Y2038ProofnessDesign
              //
              if (is_null) {
                *reinterpret_cast<time_t*>(p) =
               inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType);
              } else {
                auto typed_array = static_cast<::arrow::TimestampArray*>(value_arr.get());
                auto typed_datatype = static_cast<::arrow::TimestampType*>(typed_array->type().get());
                time_t value = typed_array->Value(j);

                switch (typed_datatype->unit()) {
                  case ::arrow::TimeUnit::SECOND:
                    // no unit conversion necessary
                    break;
                  case ::arrow::TimeUnit::MILLI:
                    // convert milliseconds to seconds
                    value = value / 1000;
                    break;
                  case ::arrow::TimeUnit::MICRO:
                    // convert microseconds to seconds
                    value = value / 1000000;
                    break;
                  case ::arrow::TimeUnit::NANO:
                    // convert nanoseconds to seconds
                    value = value / 1000000000;
                    break;
                  default:
                    break;
                }

                *reinterpret_cast<time_t*>(p) = value;
              }
              p = p + sizeof(time_t);
              break;
            }
            case ::arrow::Type::LIST:
            case ::arrow::Type::STRUCT:
            case ::arrow::Type::UNION:
            case ::arrow::Type::DICTIONARY: {
              // kARRAY
              // TODO(renesugar): Nested non-scalar types as elements in a list/array?
              throw std::runtime_error("List of list, struct, union or dictionary not supported for column " + tib->getColumnDesc()->columnName);
              break;
            }
            default:
              CHECK(false);
          }
        }

        // Add array to TypedImportBuffer

        // NOTE: Never reach here for List<INT8> or List<UINT8>; added as a string
        //       containing binary data instead of array of INT8 or UINT8.

        if (IS_STRING(tib->getColumnDesc()->columnType.get_subtype())) {
          std::vector<std::string>& string_array = tib->addStringArray();
          if (!is_null) {
            for (int k = 0; k < string_vec.size(); i++) {
              string_array.push_back(string_vec[i]);
            }
          }
        } else {
          if (is_null) {
            tib->addArray(ArrayDatum(0, NULL, true));
          } else {
            tib->addArray(ArrayDatum(len, buf, len == 0));
          }
        }
      }
    }
  }

  return ::arrow::Status::OK();
}

::arrow::Status CopyArrowScalarColumn(const std::shared_ptr<::arrow::Column>& col,
                                      TypedImportBuffer* tib) {
  const ::arrow::ChunkedArray& data = *col->data().get();

  ::arrow::Type::type type_id = col->type()->id();

  if (!IsArrowScalarColumn(col)) {
    // return an error if the column type is not a scalar type
    return ::arrow::Status::TypeError("Not a scalar type in " __FILE__ " at " STR__LINE__ " for column " + col->field()->name());
  }

  bool is_null = false;

  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = std::static_pointer_cast<::arrow::Array>(data.chunk(c));

    const bool has_nulls = data.null_count() > 0;

    for (int64_t i = 0; i < arr->length(); ++i) {
      is_null = (has_nulls && arr->IsNull(i));

      // Arrow methods to get the value from an array vary by type
      //
      // value_type  NumericArray<type>::Value(i)
      // NumericArray<Int8Type>;
      // NumericArray<UInt8Type>;
      // NumericArray<Int16Type>;
      // NumericArray<UInt16Type>;
      // NumericArray<Int32Type>;
      // NumericArray<UInt32Type>;
      // NumericArray<Int64Type>;
      // NumericArray<UInt64Type>;
      // NumericArray<HalfFloatType>;
      // NumericArray<FloatType>;
      // NumericArray<DoubleType>;
      // NumericArray<Date32Type>;
      // NumericArray<Date64Type>;
      // NumericArray<Time32Type>;
      // NumericArray<Time64Type>;
      // NumericArray<TimestampType>;
      // NumericArray<IntervalType>; (not implemented by Arrow)
      // value_type  BooleanArray::Value(i)
      // uint8_t *   BinaryArray::GetValue(i, &length)
      // std::string StringArray::GetString(i)
      // uint8_t *   FixedSizeBinary::GetValue(i)  int32_t byte_width()  (DecimalType)
      //
      //
      if (is_null && !col->field()->nullable())
        throw std::runtime_error("NULL for column " + col->field()->name());

      switch (type_id) {
        case ::arrow::Type::NA:
          // MapD does not store kNULL columns
          break;
        case ::arrow::Type::BOOL: {
          // kBOOLEAN
          if (is_null) {
            tib->addBoolean(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::BooleanArray*>(arr.get());
            bool value = typed_array->Value(i);

            tib->addBoolean(value);
          }
          break;
        }
        case ::arrow::Type::UINT8: {
          // kSMALLINT
          if (is_null) {
            tib->addSmallint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::UInt8Array*>(arr.get());
            uint8_t value = typed_array->Value(i);

            tib->addSmallint((int16_t)value);
          }
          break;
        }
        case ::arrow::Type::INT8: {
          // kSMALLINT
          if (is_null) {
            tib->addSmallint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Int8Array*>(arr.get());
            int8_t value = typed_array->Value(i);

            tib->addSmallint((int16_t)value);
          }
          break;
        }
        case ::arrow::Type::UINT16: {
          // kSMALLINT
          if (is_null) {
            tib->addSmallint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::UInt16Array*>(arr.get());
            uint16_t value = typed_array->Value(i);

            tib->addSmallint(value);
          }
          break;
        }
        case ::arrow::Type::INT16: {
          // kSMALLINT
          if (is_null) {
            tib->addSmallint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Int16Array*>(arr.get());
            int16_t value = typed_array->Value(i);

            tib->addSmallint(value);
          }
          break;
        }
        case ::arrow::Type::UINT32:  {
          // kINT
          if (is_null) {
            tib->addInt(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::UInt32Array*>(arr.get());
            uint32_t value = typed_array->Value(i);

            tib->addInt(value);
          }
          break;
        }
        case ::arrow::Type::INT32: {
          // kINT
          if (is_null) {
            tib->addInt(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Int32Array*>(arr.get());
            int32_t value = typed_array->Value(i);

            tib->addInt(value);
          }
          break;
        }
        case ::arrow::Type::UINT64: {
          // kBIGINT
          // kNUMERIC
          if (is_null) {
            tib->addBigint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::UInt64Array*>(arr.get());
            uint64_t value = typed_array->Value(i);

            tib->addBigint(value);
          }
          break;
        }
        case ::arrow::Type::INT64: {
          // kBIGINT
          // kNUMERIC
          if (is_null) {
            tib->addBigint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Int64Array*>(arr.get());
            int64_t value = typed_array->Value(i);

            tib->addBigint(value);
          }
          break;
        }
        case ::arrow::Type::HALF_FLOAT: {
          // kFLOAT
          if (is_null) {
            tib->addFloat(NULL_FLOAT);
          } else {
            auto typed_array = static_cast<::arrow::HalfFloatArray*>(arr.get());
            // Convert from half-float to float
            float value = static_cast<float>(halfbits_to_floatbits(typed_array->Value(i)));

            tib->addFloat(value);
          }
          break;
        }
        case ::arrow::Type::FLOAT: {
          // kFLOAT
          if (is_null) {
            tib->addFloat(NULL_FLOAT);
          } else {
            auto typed_array = static_cast<::arrow::FloatArray*>(arr.get());
            float value = typed_array->Value(i);

            tib->addFloat(value);
          }
          break;
        }
        case ::arrow::Type::DOUBLE: {
          // kDOUBLE
          if (is_null) {
            tib->addDouble(NULL_DOUBLE);
          } else {
            auto typed_array = static_cast<::arrow::DoubleArray*>(arr.get());
            double value = typed_array->Value(i);

            tib->addDouble(value);
          }
          break;
        }
        case ::arrow::Type::STRING: {
          // kTEXT
          // kVARCHAR
          // kCHAR
          if (is_null) {
            // @TODO(wei) for now, use empty string for nulls
            tib->addString(std::string());
          } else {
            auto typed_array = static_cast<::arrow::StringArray*>(arr.get());
            std::string value = typed_array->GetString(i);

            tib->addString(value);
          }
          break;
        }
        case ::arrow::Type::BINARY: {
          // kVARCHAR
          if (is_null) {
            tib->addString(std::string());
          } else {
            auto typed_array = static_cast<::arrow::BinaryArray*>(arr.get());
            int32_t length = 0;
            const uint8_t* value = typed_array->GetValue(i, &length);

            tib->addString(std::string(reinterpret_cast<char const*>(value), length));
          }
          break;
        }
        case ::arrow::Type::FIXED_SIZE_BINARY: {
          if (is_null) {
            tib->addString(std::string());
          } else {
            auto typed_array = static_cast<::arrow::FixedSizeBinaryArray*>(arr.get());
            int32_t length = typed_array->byte_width();
            const uint8_t* value = typed_array->GetValue(i);

            tib->addString(std::string(reinterpret_cast<char const*>(value), length));
          }
          break;
        }
          // TODO(renesugar): Arrow has not implemented IntervalArray
          /* case ::arrow::Type::INTERVAL: {
           // kINTERVAL_DAY_TIME
           // kINTERVAL_YEAR_MONTH

           if (is_null) {
             tib->addBigint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
           } else {
             auto typed_array = static_cast<::arrow::IntervalArray*>(arr.get());
             auto typed_datatype = static_cast<::arrow::IntervalType*>(typed_array->type().get());
             int64_t value = typed_array->Value(i);

             switch (typed_datatype->unit()) {
             case ::arrow::IntervalType::Unit::YEAR_MONTH:
             // no unit conversion necessary
             break;
             case ::arrow::IntervalType::Unit::DAY_TIME:
             // no unit conversion necessary
             break;
             default:
             break;
             }

             tib->addBigint(value);
           }
           break;
           }*/
        case ::arrow::Type::DECIMAL: {
          const uint8_t* value = nullptr;
          int16_t  value_int16 = 0;
          int32_t  value_int32 = 0;
          int64_t  value_int64 = 0;
          /*int128_t  value_int128 = 0;*/
          auto typed_array = static_cast<::arrow::FixedSizeBinaryArray*>(arr.get());
          int32_t length = typed_array->byte_width();

          switch (length) {
            case 2: {
              if (is_null) {
                tib->addSmallint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
              } else {
                value = typed_array->GetValue(i);

                value_int16 = *reinterpret_cast<int16_t*>(const_cast<uint8_t*>(value));

                tib->addSmallint(value_int16);
              }
              break;
            }
            case 4: {
              if (is_null) {
                tib->addInt(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
              } else {
                value = typed_array->GetValue(i);

                value_int32 = *reinterpret_cast<int32_t*>(const_cast<uint8_t*>(value));

                tib->addInt(value_int32);
              }
              break;
            }
            case 8: {
              if (is_null) {
                tib->addBigint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
              } else {
                value = typed_array->GetValue(i);

                value_int64 = *reinterpret_cast<int64_t*>(const_cast<uint8_t*>(value));

                tib->addBigint(value_int64);
              }
              break;
            }
              // TODO(renesugar): MapD currently does not support 128-bit decimal values.
              //       inline_fixed_encoding_null_val() will have to be updated.
              /*
               case 16: {
               if (is_null) {
               // TODO(renesugar): Need a method like addBigint for 128-bit integers
                 tib->addBigint(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
               } else {
                 value = typed_array->GetValue(i);

                 value_int128 = *reinterpret_cast<int128_t*>(const_cast<uint8_t*>(value));

                tib->addBigint(value_int128);
               }
               break;
               }
               */
            default:
              break;
          }
          break;
        }
        case ::arrow::Type::TIME32: {
          // kTIME:
          if (is_null) {
            tib->addTime(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Time32Array*>(arr.get());
            auto typed_datatype = static_cast<::arrow::Time32Type*>(typed_array->type().get());
            time_t value = typed_array->Value(i);

            switch (typed_datatype->unit()) {
              case ::arrow::TimeUnit::SECOND:
                // no unit conversion necessary
                break;
              case ::arrow::TimeUnit::MILLI:
                // convert milliseconds to seconds
                value = value / 1000;
                break;
                /*
                 case ::arrow::TimeUnit::MICRO:
                 // convert microseconds to seconds
                 value = value / 1000000;
                 break;
                 case ::arrow::TimeUnit::NANO:
                 // convert nanoseconds to seconds
                 value = value / 1000000000;
                 break;
                 */
              default:
                break;
            }

            tib->addTime(value);
          }
          break;
        }
        case ::arrow::Type::TIME64: {
          // kTIME:
          if (is_null) {
            tib->addTime(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Time64Array*>(arr.get());
            auto typed_datatype = static_cast<::arrow::Time64Type*>(typed_array->type().get());
            time_t value = typed_array->Value(i);

            switch (typed_datatype->unit()) {
              case ::arrow::TimeUnit::SECOND:
                // no unit conversion necessary
                break;
              case ::arrow::TimeUnit::MILLI:
                // convert milliseconds to seconds
                value = value / 1000;
                break;
              case ::arrow::TimeUnit::MICRO:
                // convert microseconds to seconds
                value = value / 1000000;
                break;
              case ::arrow::TimeUnit::NANO:
                // convert nanoseconds to seconds
                value = value / 1000000000;
                break;
              default:
                break;
            }

            tib->addTime(value);
          }
          break;
        }
        case ::arrow::Type::DATE32: {
          // kDATE
          if (is_null) {
            tib->addTime(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Date32Array*>(arr.get());
            time_t value = typed_array->Value(i);

            tib->addTime(value);
          }
          break;
        }
        case ::arrow::Type::DATE64: {
          // kDATE
          if (is_null) {
            tib->addTime(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::Date64Array*>(arr.get());
            time_t value = typed_array->Value(i);

            // convert milliseconds to seconds
            value = value / 1000;

            tib->addTime(value);
          }
          break;
        }
        case ::arrow::Type::TIMESTAMP: {
          // kTIMESTAMP:
          // TODO(renesugar): Add units to SQLTypeInfo for kTIMESTAMP type to MapD?

          // TODO(renesugar): Add timezone to SQLTypeInfo for kTIMESTAMP type to MapD?
          //       timestamp_type->timezone();

          // TODO(renesugar): Arrow uses int64_t for timestamp values; MapD uses time_t.
          //       Use int64_t for timestamps in MapD?
          //       https://www.mapd.com/docs/latest/mapd-core-guide/fixed-encoding/
          //       https://sourceware.org/glibc/wiki/Y2038ProofnessDesign
          //
          if (is_null) {
            tib->addTime(inline_fixed_encoding_null_val(tib->getColumnDesc()->columnType));
          } else {
            auto typed_array = static_cast<::arrow::TimestampArray*>(arr.get());
            auto typed_datatype = static_cast<::arrow::TimestampType*>(typed_array->type().get());
            time_t value = typed_array->Value(i);

            switch (typed_datatype->unit()) {
              case ::arrow::TimeUnit::SECOND:
                // no unit conversion necessary
                break;
              case ::arrow::TimeUnit::MILLI:
                // convert milliseconds to seconds
                value = value / 1000;
                break;
              case ::arrow::TimeUnit::MICRO:
                // convert microseconds to seconds
                value = value / 1000000;
                break;
              case ::arrow::TimeUnit::NANO:
                // convert nanoseconds to seconds
                value = value / 1000000000;
                break;
              default:
                break;
            }

            tib->addTime(value);
          }
          break;
        }
        case ::arrow::Type::LIST: {
          // kARRAY is handled in CopyArrowListColumn
          throw std::runtime_error("Wrong function call; List columns implemented in CopyArrowListColumn for column " + tib->getColumnDesc()->columnName);
          break;
        }
        case ::arrow::Type::STRUCT:
        case ::arrow::Type::UNION: {
          // TODO(renesugar): Implement struct and union columns for MapD?
          //       https://arrow.apache.org/docs/memory_layout.html#dictionary-encoding
          throw std::runtime_error("Struct and union types not implemented for column " +
                                   tib->getColumnDesc()->columnName);
          break;
        }
        case ::arrow::Type::DICTIONARY: {
          // Dictionary encode columns should have been decoded and stored as
          // the data type of the dictionary during import.
          //
          // Arrow dictionary encodes many more data types than MapD.
          //
          throw std::runtime_error("Dictionary type should have been decoded for column " +
                                   tib->getColumnDesc()->columnName);
          break;
        }
        default:
          CHECK(false);
      }
    }
  }

  return ::arrow::Status::OK();
}

std::unique_ptr<::arrow::ArrayBuilder> SQLTypeToArrowBuilder(
                                              const SQLTypeInfo& columnType) {
  std::unique_ptr<::arrow::ArrayBuilder> builder;

  std::shared_ptr<::arrow::DataType> type = SQLTypeToArrowType(columnType);

  // TODO(renesugar): IntervalBuilder is missing from Arrow library
  ::arrow::Status status = ::arrow::MakeBuilder(::arrow::default_memory_pool(),
                                                type, &builder);

  if (!status.ok()) {
    builder.reset(nullptr);
  }

  return std::unique_ptr<::arrow::ArrayBuilder>(builder.release());
}

std::string SQLTypeToString(SQLTypes type) {
  switch (type) {
    case kNULLT:
      return "kNULLT";
    case kBOOLEAN:
      return "kBOOLEAN";
    case kCHAR:
      return "kCHAR";
    case kVARCHAR:
      return "kVARCHAR";
    case kNUMERIC:
      return "kNUMERIC";
    case kDECIMAL:
      return "kDECIMAL";
    case kINT:
      return "kINT";
    case kSMALLINT:
      return "kSMALLINT";
    case kFLOAT:
      return "kFLOAT";
    case kDOUBLE:
      return "kDOUBLE";
    case kTIME:
      return "kTIME";
    case kTIMESTAMP:
      return "kTIMESTAMP";
    case kBIGINT:
      return "kBIGINT";
    case kTEXT:
      return "kTEXT";
    case kDATE:
      return "kDATE";
    case kARRAY:
      return "kARRAY";
    case kINTERVAL_DAY_TIME:
      return "kINTERVAL_DAY_TIME";
    case kINTERVAL_YEAR_MONTH:
      return "kINTERVAL_YEAR_MONTH";
    default:
      return "unknown";
  }
}

SQLTypes StringToSQLType(const std::string& type) {
  if (type == "kNULLT")
    return kNULLT;
  else if (type == "kBOOLEAN")
    return kBOOLEAN;
  else if (type == "kCHAR")
    return kCHAR;
  else if (type == "kVARCHAR")
    return kVARCHAR;
  else if (type == "kNUMERIC")
    return kNUMERIC;
  else if (type == "kDECIMAL")
    return kDECIMAL;
  else if (type == "kINT")
    return kINT;
  else if (type == "kSMALLINT")
    return kSMALLINT;
  else if (type == "kFLOAT")
    return kFLOAT;
  else if (type == "kDOUBLE")
    return kDOUBLE;
  else if (type == "kTIME")
    return kTIME;
  else if (type == "kTIMESTAMP")
    return kTIMESTAMP;
  else if (type == "kBIGINT")
    return kBIGINT;
  else if (type == "kTEXT")
    return kTEXT;
  else if (type == "kDATE")
    return kDATE;
  else if (type == "kARRAY")
    return kARRAY;
  else if (type == "kINTERVAL_DAY_TIME")
    return kINTERVAL_DAY_TIME;
  else if (type == "kINTERVAL_YEAR_MONTH")
    return kINTERVAL_YEAR_MONTH;

  // unknown type
  return kSQLTYPE_LAST;
}

std::string EncodingTypeToString(EncodingType type) {
  switch (type) {
    case kENCODING_NONE:
      return "kENCODING_NONE";
    case kENCODING_FIXED:
      return "kENCODING_FIXED";
    case kENCODING_RL:
      return "kENCODING_RL";
    case kENCODING_DIFF:
      return "kENCODING_DIFF";
    case kENCODING_DICT:
      return "kENCODING_DICT";
    case kENCODING_SPARSE:
      return "kENCODING_SPARSE";
    default:
      return "unknown";
  }
}

EncodingType StringToEncodingType(const std::string& type) {
  if (type == "kENCODING_NONE")
    return kENCODING_NONE;
  else if (type == "kENCODING_FIXED")
    return kENCODING_FIXED;
  else if (type == "kENCODING_RL")
    return kENCODING_RL;
  else if (type == "kENCODING_DIFF")
    return kENCODING_DIFF;
  else if (type == "kENCODING_DICT")
    return kENCODING_DICT;
  else if (type == "kENCODING_SPARSE")
    return kENCODING_SPARSE;

  // unknown type
  return kENCODING_LAST;
}

void SQLTypeInfoToArrowKeyValueMetadata(const SQLTypeInfo& ti,
                                       std::vector<std::string>& metadata_keys,
                                       std::vector<std::string>& metadata_values) {
  Datum d;
  SQLTypeInfo dtINT(kINT, false);
  SQLTypeInfo dtBOOLEAN(kBOOLEAN, false);
  std::string key;
  std::string value;

  // type id
  key   = "mapd.SQLTypeInfo.type";
  value = SQLTypeToString(ti.get_type());
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // element type of arrays
  key   = "mapd.SQLTypeInfo.subtype";
  value = SQLTypeToString(ti.get_subtype());
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // VARCHAR/CHAR length or NUMERIC/DECIMAL precision
  key   = "mapd.SQLTypeInfo.dimension";
  d.intval = ti.get_dimension();  // same value as ti.get_precision()
  value = DatumToString(d, dtINT);
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // NUMERIC/DECIMAL scale
  key   = "mapd.SQLTypeInfo.scale";
  d.intval = ti.get_scale();
  value = DatumToString(d, dtINT);
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // nullable?
  key   = "mapd.SQLTypeInfo.notnull";
  d.boolval = ti.get_notnull();
  value = DatumToString(d, dtBOOLEAN);
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // compression scheme
  key   = "mapd.SQLTypeInfo.compression";
  value = EncodingTypeToString(ti.get_compression());
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // compression parameter when applicable for certain schemes
  key   = "mapd.SQLTypeInfo.comp_param";
  d.intval = ti.get_comp_param();
  value = DatumToString(d, dtINT);
  metadata_keys.push_back(key);
  metadata_values.push_back(value);

  // size of the type in bytes.  -1 for variable size
  key   = "mapd.SQLTypeInfo.size";
  d.intval = ti.get_size();
  value = DatumToString(d, dtINT);
  metadata_keys.push_back(key);
  metadata_values.push_back(value);
}

bool ArrowKeyValueMetadataToSQLTypeInfo(SQLTypeInfo& ti,
                                       const std::string& metadata_key,
                                       const std::string& metadata_value) {
  Datum d;
  SQLTypeInfo dtINT(kINT, false);
  SQLTypeInfo dtBOOLEAN(kBOOLEAN, false);

  // type id
  if (metadata_key == "mapd.SQLTypeInfo.type") {
    ti.set_type(StringToSQLType(metadata_value));
    return true;
  }

  // element type of arrays
  if (metadata_key == "mapd.SQLTypeInfo.subtype") {
    ti.set_subtype(StringToSQLType(metadata_value));
    return true;
  }

  // VARCHAR/CHAR length or NUMERIC/DECIMAL precision
  if (metadata_key == "mapd.SQLTypeInfo.dimension") {
    d = StringToDatum(metadata_value, dtINT);
    ti.set_dimension(d.intval);
    return true;
  }

  // NUMERIC/DECIMAL scale
  if (metadata_key == "mapd.SQLTypeInfo.scale") {
    d = StringToDatum(metadata_value, dtINT);
    ti.set_scale(d.intval);
    return true;
  }

  // nullable?  a hint, not used for type checking
  if (metadata_key == "mapd.SQLTypeInfo.notnull") {
    d = StringToDatum(metadata_value, dtBOOLEAN);
    ti.set_notnull(d.boolval);
    return true;
  }

  // TODO(renesugar): Should compression type be preserved?
  /*
  // compression scheme
  if (metadata_key == "mapd.SQLTypeInfo.compression") {
    ti.set_compression(StringToEncodingType(metadata_value));
    return true;
  }

  // compression parameter when applicable for certain schemes
  if (metadata_key == "mapd.SQLTypeInfo.comp_param") {
    d = StringToDatum(metadata_value, dtINT);
    ti.set_comp_param(d.intval);
    return true;
  }
  */
  // size of the type in bytes.  -1 for variable size
  if (metadata_key == "mapd.SQLTypeInfo.size") {
    d = StringToDatum(metadata_value, dtINT);
    ti.set_size(d.intval);
    return true;
  }

  return false;
}

// Call get_column_descs() on Writer class to get parameter for this function
std::shared_ptr<::arrow::Schema> ArrowSchema(const std::list<const ColumnDescriptor*>& column_descs,
                                            const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  std::vector<std::string> metadata_keys;
  std::vector<std::string> metadata_values;

  fields.reserve(column_descs.size());
  for (const auto& column_desc : column_descs) {
    // set SQLTypeInfo as key/value metadata on the column
    metadata_keys.clear();
    metadata_values.clear();

    SQLTypeInfoToArrowKeyValueMetadata(column_desc->columnType, metadata_keys, metadata_values);

    std::shared_ptr<::arrow::KeyValueMetadata> kvm(new ::arrow::KeyValueMetadata(metadata_keys, metadata_values));

    std::shared_ptr<::arrow::DataType> type = SQLTypeToArrowType(column_desc->columnType);

    fields.emplace_back(std::make_shared<::arrow::Field>(column_desc->columnName,
                                                         type,
                                                         !column_desc->columnType.get_notnull(),
                                                         kvm));
  }

  return std::make_shared<::arrow::Schema>(fields, metadata);
}

// Update schema after any arrays have been dictionary encoded
std::shared_ptr<::arrow::Schema> ArrowSchema(std::shared_ptr<::arrow::Schema> schema,
                                             std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  std::vector<std::shared_ptr<::arrow::Field>> fields;

  fields.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); i++) {
    fields.emplace_back(std::make_shared<::arrow::Field>(schema->field(i)->name(),
                                                         arrays[i]->type(),
                                                         schema->field(i)->nullable(),
                                                         schema->field(i)->metadata()));
  }
    
  return std::make_shared<::arrow::Schema>(fields, schema->metadata());
}


const ColumnDescriptor* getMetadataForArrowColumn(
                           const std::shared_ptr<::arrow::Field>& field) {
  ::arrow::Status status;

  ColumnDescriptor* cd = new ColumnDescriptor();

  status = ArrowTypeToSQLType(field, cd);

  if (!status.ok()) {
    delete cd;
    cd = nullptr;
  }

  return cd;
}

const std::shared_ptr<::arrow::Schema> getArrowSchemaFromFile(const std::string& file) {
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
  parquet::ParquetFileReader::OpenFile(file, false);

  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

  const parquet::SchemaDescriptor* parquet_schema = file_metadata->schema();

  std::shared_ptr<const ::arrow::KeyValueMetadata> key_value_metadata = file_metadata->key_value_metadata();

  std::shared_ptr<::arrow::Schema> arrow_schema;

  ::arrow::Status status = ::parquet::arrow::FromParquetSchema(
                                                               parquet_schema,
                                                               key_value_metadata,
                                                               &arrow_schema);
  if (!status.ok()) {
    arrow_schema = nullptr;
  }

  return arrow_schema;
}

std::list<const ColumnDescriptor*> getAllColumnMetadataForArrowSchema(
                                 const std::shared_ptr<::arrow::Schema>& schema,
                                 const bool fetchSystemColumns,
                                 const bool fetchVirtualColumns) {
  std::shared_ptr<::arrow::Field> field;
  std::list<const ColumnDescriptor*> columnDescriptors;

  for (int i = 0; i < schema->num_fields(); i++) {
    const ColumnDescriptor* cd = getMetadataForArrowColumn(schema->field(i));
    assert(cd != nullptr);
    if (!fetchSystemColumns && cd->isSystemCol)
      continue;
    if (!fetchVirtualColumns && cd->isVirtualCol)
      continue;
    columnDescriptors.push_back(cd);
  }

  return columnDescriptors;
}

// Arrow arrays are immutable. Copy the right number of rows for a row group to the builder.
::arrow::Status CopyNullsToArrowBuilder(const SQLTypeInfo& columnType,
                 int start_index, int end_index, int max_index,
                             std::unique_ptr<::arrow::ArrayBuilder>& builder) {
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

  switch (columnType.get_type()) {
      // Cog code generation tool: https://nedbatchelder.com/code/cog/
      //
      // cog.py-2.7 -r ArrowSupport.cpp
      //

      // TODO(renesugar): MapD and Arrow support for the intervals is incomplete.
      //
      //       MapD Interval Types    Arrow IntervalType
      //       -------------------    ------------------
      //       kINTERVAL_DAY_TIME     IntervalType::Unit::DAY_TIME
      //       kINTERVAL_YEAR_MONTH   IntervalType::Unit::YEAR_MONTH

      // TODO(renesugar): IntervalBuilder is missing from Arrow library

      // TODO(renesugar): MapD uses time_t for intervals; Arrow uses int64_t

      // TODO(renesugar): Intervals not handled by StringToDatum in Shared/Datum.cpp (used by import)

      // NOTE: Move comment containing inactive Cog region above the active one or it
      //       will get overwritten when Cog is used to generate code.

      // Inline code

      /*[ [ [ c o g
       def capitalize(name):
         return ''.join(s[0].upper() + s[1:] for s in name.split('_'))
       
       types = [
       # sql_type     arrow_type        arrow_builder         type_array       c_type
       # --------     -------------     -------------------   ----------       ------
       ('kBOOLEAN',   'BooleanType',   'BooleanBuilder',     'bool',         'int8_t'),
       ('kSMALLINT',  'Int16Type',     'Int16Builder',       'smallint',    'int16_t'),
       ('kINT',       'Int32Type',     'Int32Builder',       'int',         'int32_t'),
       ('kBIGINT',    'Int64Type',     'Int64Builder',       'bigint',      'int64_t'),
       ('kNUMERIC',   'Int64Type',     'Int64Builder',       'bigint',      'int64_t'),
       ('kFLOAT',     'FloatType',     'FloatBuilder',       'float',         'float'),
       ('kDOUBLE',    'DoubleType',    'DoubleBuilder',      'double',       'double'),
       ('kTIME',      'Time64Type',    'Time64Builder',      'time',         'time_t'),
       ('kTIMESTAMP', 'TimestampType', 'TimestampBuilder',   'time',         'time_t'),
       ('kDATE',      'Date64Type',    'Date64Builder',      'time',         'time_t'),
       ('kDECIMAL',   'DecimalType',   'DecimalBuilder',     'bigint',      'int64_t'),
       ('kTEXT',      'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kVARCHAR',   'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kCHAR',      'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kARRAY',     'ListType',      'ListBuilder',        'array',    'ArrayDatum'),
       ]
       
       for sql_type, arrow_type, arrow_builder, type_array, c_type in types:
         cog.outl('case %s: {' % (sql_type))
         cog.outl('  ::arrow::Status status = arrow::Status::IsTypeError();')
         cog.outl('  auto typed_builder = static_cast<::arrow::%s*>(builder.get());' % (arrow_builder))
         cog.outl('  for (int i = start_index; i < end_index; i++) {')
         cog.outl('    status = typed_builder->AppendNull();')
         cog.outl('    if (!status.ok()) {')
         cog.outl('      return;')
         cog.outl('    }')
         cog.outl('  }')
         cog.outl('  break;')
         cog.outl('}')
       ] ] ]*/

      // Use templates

      /*[[[cog
       def capitalize(name):
         return ''.join(s[0].upper() + s[1:] for s in name.split('_'))
       
       types = [
       # sql_type     arrow_type        arrow_builder         type_array       c_type
       # --------     -------------     -------------------   ----------       ------
       ('kBOOLEAN',   'BooleanType',   'BooleanBuilder',     'bool',         'int8_t'),
       ('kSMALLINT',  'Int16Type',     'Int16Builder',       'smallint',    'int16_t'),
       ('kINT',       'Int32Type',     'Int32Builder',       'int',         'int32_t'),
       ('kBIGINT',    'Int64Type',     'Int64Builder',       'bigint',      'int64_t'),
       ('kNUMERIC',   'Int64Type',     'Int64Builder',       'bigint',      'int64_t'),
       ('kFLOAT',     'FloatType',     'FloatBuilder',       'float',         'float'),
       ('kDOUBLE',    'DoubleType',    'DoubleBuilder',      'double',       'double'),
       ('kTIME',      'Time64Type',    'Time64Builder',      'time',         'time_t'),
       ('kTIMESTAMP', 'TimestampType', 'TimestampBuilder',   'time',         'time_t'),
       ('kDATE',      'Date64Type',    'Date64Builder',      'time',         'time_t'),
       ('kDECIMAL',   'DecimalType',   'DecimalBuilder',     'bigint',      'int64_t'),
       ('kTEXT',      'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kVARCHAR',   'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kCHAR',      'StringType',    'StringBuilder',      'string',  'std::string'),
       ('kARRAY',     'ListType',      'ListBuilder',        'array',    'ArrayDatum'),
       ]
       
       for sql_type, arrow_type, arrow_builder, type_array, c_type in types:
         cog.outl('case %s: {' % (sql_type))
         cog.outl('  ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::%s>(start_index, end_index, builder));' % (arrow_type))
         cog.outl('  break;')
         cog.outl('}')
       ]]]*/
      case kBOOLEAN: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::BooleanType>(start_index, end_index, builder));
        break;
      }
      case kSMALLINT: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Int16Type>(start_index, end_index, builder));
        break;
      }
      case kINT: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Int32Type>(start_index, end_index, builder));
        break;
      }
      case kBIGINT: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Int64Type>(start_index, end_index, builder));
        break;
      }
      case kNUMERIC: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Int64Type>(start_index, end_index, builder));
        break;
      }
      case kFLOAT: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::FloatType>(start_index, end_index, builder));
        break;
      }
      case kDOUBLE: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::DoubleType>(start_index, end_index, builder));
        break;
      }
      case kTIME: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Time64Type>(start_index, end_index, builder));
        break;
      }
      case kTIMESTAMP: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::TimestampType>(start_index, end_index, builder));
        break;
      }
      case kDATE: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::Date64Type>(start_index, end_index, builder));
        break;
      }
      case kDECIMAL: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::DecimalType>(start_index, end_index, builder));
        break;
      }
      case kTEXT: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::StringType>(start_index, end_index, builder));
        break;
      }
      case kVARCHAR: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::StringType>(start_index, end_index, builder));
        break;
      }
      case kCHAR: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::StringType>(start_index, end_index, builder));
        break;
      }
      case kARRAY: {
        ARROW_RETURN_NOT_OK(copy_nulls_to_arrow_builder<::arrow::ListType>(start_index, end_index, builder));
        break;
      }
    //[[[end]]]
    default:
      CHECK(false);
  }

  return ::arrow::Status::OK();
}

}  // namespace Importer_NS
