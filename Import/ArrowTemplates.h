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
 * @file ArrowTemplates.h
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Templates for Arrow data structures
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 */
#ifndef _ARROWTEMPLATES_H_
#define _ARROWTEMPLATES_H_

#include <arrow/api.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

namespace Importer_NS {

inline int64_t IntegerArrayValue(std::shared_ptr<::arrow::Array>& indices, int64_t index) {
  int64_t retval = 0;
  ::arrow::Type::type type_id = indices->type()->id();

  if (indices->IsNull(index)) {
    return -1;
  }

  switch (type_id) {
    case ::arrow::Type::UINT8: {
      auto typed_array = static_cast<::arrow::UInt8Array*>(indices.get());
      uint8_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::INT8: {
      auto typed_array = static_cast<::arrow::Int8Array*>(indices.get());
      int8_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::UINT16: {
      auto typed_array = static_cast<::arrow::UInt16Array*>(indices.get());
      uint16_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::INT16: {
      auto typed_array = static_cast<::arrow::Int16Array*>(indices.get());
      int16_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::UINT32: {
      auto typed_array = static_cast<::arrow::UInt32Array*>(indices.get());
      uint32_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::INT32: {
      auto typed_array = static_cast<::arrow::Int32Array*>(indices.get());
      int32_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::UINT64: {
      auto typed_array = static_cast<::arrow::UInt64Array*>(indices.get());
      uint64_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    case ::arrow::Type::INT64: {
      auto typed_array = static_cast<::arrow::Int64Array*>(indices.get());
      int16_t value = typed_array->Value(index);
      retval = static_cast<int64_t>(value);
      break;
    }
    default:
      return -1;
      break;
  }

  return retval;
}

// Copy dictionary array slice templates

template <typename TYPE, typename C_TYPE>
::arrow::Status CopyNumericArrayDictionarySlice(int start_index,
                               int end_index,
                               std::shared_ptr<::arrow::Array>& indices,
                               std::shared_ptr<::arrow::Array>& dictionary,
                               std::unique_ptr<::arrow::ArrayBuilder>& builder) {
  ::arrow::Status status = ::arrow::Status::TypeError("CopyNumericArraySlice");

  ::arrow::Type::type type_id = indices->type()->id();

  if (!::arrow::is_integer(type_id)) {
    return ::arrow::Status::Invalid("Dictionary indices must be an integer type");
  }

  if (start_index >= end_index) {
    return ::arrow::Status::Invalid("Start index should be less than end index");
  }

  if ((start_index < 0) || (start_index >= indices->length())) {
    return ::arrow::Status::Invalid("Start index out of range");
  }

  if ((end_index < 0) || (end_index > indices->length())) {
    return ::arrow::Status::Invalid("End index out of range");
  }

  auto typed_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(builder.get());
  auto typed_array = static_cast<typename ::arrow::TypeTraits<TYPE>::ArrayType*>(dictionary.get());
  for (int i = start_index; i < end_index; i++) {
    if (indices->IsNull(i)) {
      ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
    } else {
      const int64_t index = IntegerArrayValue(indices, i);
      const C_TYPE  value = typed_array->Value(index);

      ARROW_RETURN_NOT_OK(typed_builder->Append(value));
    }
  }

  return ::arrow::Status::OK();
}

template <typename TYPE, typename C_TYPE>
::arrow::Status CopyBinaryArrayDictionarySlice(int start_index,
                                                int end_index,
                                                std::shared_ptr<::arrow::Array>& indices,
                                                std::shared_ptr<::arrow::Array>& dictionary,
                                                std::unique_ptr<::arrow::ArrayBuilder>& builder) {
  ::arrow::Status status = ::arrow::Status::TypeError("CopyBinaryArrayDictionarySlice");

  ::arrow::Type::type type_id = indices->type()->id();

  if (!::arrow::is_integer(type_id)) {
    return ::arrow::Status::Invalid("Dictionary indices must be an integer type");
  }

  if (start_index >= end_index) {
    return ::arrow::Status::Invalid("Start index should be less than end index");
  }

  if ((start_index < 0) || (start_index >= indices->length())) {
    return ::arrow::Status::Invalid("Start index out of range");
  }

  if ((end_index < 0) || (end_index > indices->length())) {
    return ::arrow::Status::Invalid("End index out of range");
  }

  auto typed_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(builder.get());
  auto typed_array = static_cast<typename ::arrow::TypeTraits<TYPE>::ArrayType*>(dictionary.get());
  for (int i = start_index; i < end_index; i++) {
    if (indices->IsNull(i)) {
      ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
    } else {
      const int64_t index = IntegerArrayValue(indices, i);
      int32_t value_length = 0;

      const C_TYPE* value = typed_array->GetValue(index, &value_length);

      ARROW_RETURN_NOT_OK(typed_builder->Append(value, value_length));
    }
  }

  return ::arrow::Status::OK();
}

template <typename TYPE, typename C_TYPE>
::arrow::Status CopyStringArrayDictionarySlice(int start_index,
                                                int end_index,
                                                std::shared_ptr<::arrow::Array>& indices,
                                                std::shared_ptr<::arrow::Array>& dictionary,
                                                std::unique_ptr<::arrow::ArrayBuilder>& builder) {
  ::arrow::Status status = ::arrow::Status::TypeError("CopyStringArrayDictionarySlice");

  ::arrow::Type::type type_id = indices->type()->id();

  if (!::arrow::is_integer(type_id)) {
    return ::arrow::Status::Invalid("Dictionary indices must be an integer type");
  }

  if (start_index >= end_index) {
    return ::arrow::Status::Invalid("Start index should be less than end index");
  }

  if ((start_index < 0) || (start_index >= indices->length())) {
    return ::arrow::Status::Invalid("Start index out of range");
  }

  if ((end_index < 0) || (end_index > indices->length())) {
    return ::arrow::Status::Invalid("End index out of range");
  }

  auto typed_builder = static_cast<typename ::arrow::TypeTraits<TYPE>::BuilderType*>(builder.get());
  auto typed_array = static_cast<typename ::arrow::TypeTraits<TYPE>::ArrayType*>(dictionary.get());
  for (int i = start_index; i < end_index; i++) {
    if (indices->IsNull(i)) {
      ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
    } else {
      const int64_t index = IntegerArrayValue(indices, i);
      const C_TYPE value = typed_array->GetString(index);

      ARROW_RETURN_NOT_OK(typed_builder->Append(value));
    }
  }

  return ::arrow::Status::OK();
}

};  // namespace Importer_NS

#endif  // _ARROWTEMPLATES_H_
