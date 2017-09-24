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
 * @file ArrowSupport.h
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for Arrow tables
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 */
#ifndef _ARROWSUPPORT_H_
#define _ARROWSUPPORT_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>

#include <arrow/api.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

#include "../Shared/sqltypes.h"
#include "../Catalog/ColumnDescriptor.h"

#include "ArrowTemplates.h"

// Return the given status if it is not OK.
#define ARROW_RETURN_VOID_NOT_OK(s)                 \
do {                                                \
::arrow::Status _s = (s);                           \
if (ARROW_PREDICT_FALSE(!_s.ok())) { status_ = _s; return; }   \
} while (0);

#define ARROW_RETURN_BOOL_NOT_OK(s)                 \
do {                                                \
::arrow::Status _s = (s);                           \
if (ARROW_PREDICT_FALSE(!_s.ok())) { status_ = _s; return false; }   \
} while (0);

#define ARROW_RETURN_VOID_NOT_OK_ELSE(s, else_) \
do {                           \
::arrow::Status _s = (s);             \
if (!_s.ok()) {              \
else_;                     \
status_ = _s;              \
return;                    \
}                            \
} while (0);

#define ARROW_RETURN_BOOL_NOT_OK_ELSE(s, else_) \
do {                           \
::arrow::Status _s = (s);             \
if (!_s.ok()) {              \
else_;                     \
status_ = _s;              \
return false;              \
}                            \
} while (0);

/*
 // Arrow uses boost::multiprecision to support 128-bit decimals.
 // TODO(renesugar): Add support for 128-bit decimals in MapD?
 //
 #include <boost/multiprecision/cpp_int.hpp>
 
 using boost::multiprecision::int128_t;
 
 // TODO(renesugar): IntervalType is not implemented by Arrow library
 //       See TimestampArray and TimestampBuilder for how to implement IntervalType arrays
 //
 namespace arrow {
 // missing from builder.h and type_fwd.h
 using IntervalBuilder = NumericBuilder<IntervalType>;
 
 // missing from array.h
 ARROW_EXTERN_TEMPLATE NumericArray<IntervalType>;
 
 // missing from array.cc
 template class ARROW_TEMPLATE_EXPORT NumericArray<IntervalType>;

 // missing from builder.cc
 BUILDER_CASE(INTERVAL, IntervalBuilder);

 }
 */

namespace Importer_NS {

class TypedImportBuffer;

// Arrow to/from MapD Conversion Functions

std::shared_ptr<::arrow::DataType> SQLTypeToArrowType(const SQLTypeInfo& columnType);

::arrow::Status ArrowTypeToSQLType(const std::shared_ptr<::arrow::Field>& field, ColumnDescriptor* cd);

int64_t DataTypeCTypeByteLength(::arrow::Type::type id);

bool IsArrowScalarColumn(const std::shared_ptr<::arrow::Column>& col);

bool IsArrowListColumn(const std::shared_ptr<::arrow::Column>& col);

::arrow::Status DecodeDictionaryToArray(const ::arrow::Array& input,
                                        ::arrow::MemoryPool* pool,
                                        std::shared_ptr<::arrow::Array>* out);

::arrow::Status DecodeDictionaryToColumn(const ::arrow::Column& input,
                                         ::arrow::MemoryPool* pool,
                                         std::shared_ptr<::arrow::Column>* out);

::arrow::Status CopyArrowListColumn(const std::shared_ptr<::arrow::Column>& col,
                                    TypedImportBuffer* tib);

::arrow::Status CopyArrowScalarColumn(const std::shared_ptr<::arrow::Column>& col,
                                      TypedImportBuffer* tib);

// Arrow to/from MapD Schema Functions

std::unique_ptr<::arrow::ArrayBuilder> SQLTypeToArrowBuilder(const SQLTypeInfo& columnType);

std::string SQLTypeToString(SQLTypes type);

SQLTypes StringToSQLType(const std::string& type);

std::string EncodingTypeToString(EncodingType type);

EncodingType StringToEncodingType(const std::string& type);

void SQLTypeInfoToArrowKeyValueMetadata(const SQLTypeInfo& ti,
                                        std::vector<std::string>& metadata_keys,
                                        std::vector<std::string>& metadata_values);

bool ArrowKeyValueMetadataToSQLTypeInfo(SQLTypeInfo& ti,
                                        const std::string& metadata_key,
                                        const std::string& metadata_value);

std::shared_ptr<::arrow::Schema> ArrowSchema(
              const std::list<const ColumnDescriptor*>& column_descs,
              const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata);

std::shared_ptr<::arrow::Schema> ArrowSchema(std::shared_ptr<::arrow::Schema> schema,
                                             std::vector<std::shared_ptr<arrow::Array>>& arrays);

// Arrow schema to MapD schema conversion functions

const ColumnDescriptor* getMetadataForArrowColumn(
                                  const std::shared_ptr<::arrow::Field>& field);

const std::shared_ptr<::arrow::Schema> getArrowSchemaFromFile(
                                                       const std::string& file);

std::list<const ColumnDescriptor*> getAllColumnMetadataForArrowSchema(
                                 const std::shared_ptr<::arrow::Schema>& schema,
                                 const bool fetchSystemColumns,
                                 const bool fetchVirtualColumns);

::arrow::Status CopyNullsToArrowBuilder(const SQLTypeInfo& columnType,
                             int start_index, int end_index, int max_index,
                             std::unique_ptr<::arrow::ArrayBuilder>& builder);
  
};  // namespace Importer_NS
#endif  // _ARROWSUPPORT_H_
