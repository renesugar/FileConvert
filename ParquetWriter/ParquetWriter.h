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

/**
 * @file	ParquetWriter.h
 * @author	Rene Sugar <rene.sugar@gmail.com>
 * @brief	Parquet file writing supported by MapD
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 **/
#ifndef PARQUETWRITER_H
#define PARQUETWRITER_H

#include <arrow/api.h>
#include <arrow/io/file.h>

#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>

#include <parquet/arrow/schema.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <memory>
#include <string>
#include <list>
#include <vector>

#include "../Import/Importer.h"

namespace Importer_NS {

class ParquetWriter : public Writer {
public:
  ParquetWriter(const std::string& f,
                const std::list<const ColumnDescriptor*>& columnDescriptors)
  : Writer(columnDescriptors), init_(true), close_(false), file_path_(f),
    row_group_size_(-1), metadata_(nullptr), properties_(nullptr),
    arrow_properties_(nullptr) {
      init();
  }

  ~ParquetWriter() {
  }

  bool addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count, bool checkpoint);

  bool doCheckpoint();

  void checkpoint1();

  void checkpoint2();

  mapd_shared_mutex& get_mutex();

  TypedImportBuffer* makeTypedImportBuffer(const ColumnDescriptor* cd);

  void close();

  // Configuration

  int row_group_size() {
    return row_group_size_;
  }
  void set_row_group_size(int value) { row_group_size_ = value; }

  const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata() { return metadata_; }
  void set_metadata(const std::shared_ptr<const ::arrow::KeyValueMetadata>& metadata) {
    metadata_ = metadata;
  }

  void set_writer_properties(const std::shared_ptr<::parquet::WriterProperties>& properties) {
    properties_ = properties;
  }

  void set_arrow_properties(const std::shared_ptr<::parquet::arrow::ArrowWriterProperties>& arrow_properties) {
    arrow_properties_ = arrow_properties;
  }

protected:
  bool init_;
  bool close_;
  ::arrow::Status status_;
  std::string file_path_;
  // TODO(renesugar): This is not used for writing to files.
  mapd_shared_mutex mapd_mutex_;

  std::shared_ptr<::arrow::io::FileOutputStream> file_;
  int row_group_size_;

  std::shared_ptr<::arrow::Schema> schema_;
  std::shared_ptr<const ::arrow::KeyValueMetadata> metadata_;

  std::shared_ptr<::parquet::WriterProperties> properties_;
  std::shared_ptr<::parquet::arrow::ArrowWriterProperties> arrow_properties_;
  std::unique_ptr<::parquet::arrow::FileWriter> file_writer_;
  std::vector<std::unique_ptr<::arrow::ArrayBuilder>> arrow_builders_;
  std::vector<bool> dict_columns_;

  void init();
};

}  // namespace Importer_NS

#endif  // PARQUETWRITER_H
