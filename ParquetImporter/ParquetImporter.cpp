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
 * @file ParquetImporter.cpp
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for ParquetImporter class
 */

#include "ParquetImporter.h"

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <stdexcept>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <future>
#include <mutex>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Import/Importer.h"

/*
 
 Read Parquet file one row group at a time into an Arrow table and translate each column
 in the row group into a list of TypedImportBufer.
 
 MapD array columns are represented as Arrow ListType columns (ListArray).
 
 References:
 
 (1) https://github.com/apache/arrow/blob/master/cpp/src/arrow/python/pandas_convert.cc
 (2) https://github.com/apache/arrow/blob/master/python/pyarrow/parquet.py
 (3) https://github.com/apache/parquet-cpp/blob/master/src/parquet/arrow/arrow-reader-writer-test.cc
 (4) https://github.com/apache/parquet-cpp/blob/master/examples/reader-writer.cc
 
 */

namespace Importer_NS {

// NOTE: File path is passed to the Importer constructor by MapDHandler::import_table
ImportStatus ParquetImporter::import(std::map<std::string, std::string>& param) {
  ImportStatus import_status;
  int64_t total_get_row_time_us = 0;
  int64_t total_str_to_val_time_us = 0;
  auto load_ms = measure<>::execution([]() {});

  auto us = measure<std::chrono::microseconds>::execution([&]() {});

  // Create a ParquetReader instance
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
  parquet::ParquetFileReader::OpenFile(Importer::get_file_path(), false);
  
  // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
  
  // Get the number of RowGroups
  int num_row_groups = file_metadata->num_row_groups();

  int num_columns = file_metadata->num_columns();
  
  // Get the schema
  const parquet::SchemaDescriptor* parquet_schema = file_metadata->schema();
  
  // Get the key/value metadata
  std::shared_ptr<const parquet::KeyValueMetadata> key_value_metadata = file_metadata->key_value_metadata();
  
  std::shared_ptr<::arrow::Schema> arrow_schema;
  
  ::arrow::Status status = ::parquet::arrow::FromParquetSchema(
                                                               parquet_schema,
                                                               key_value_metadata,
                                                               &arrow_schema);

  std::shared_ptr<parquet::arrow::FileReader> reader =
    std::make_shared<::parquet::arrow::FileReader>(::arrow::default_memory_pool(),
                                                   std::move(parquet_reader));
  
  const std::list<const ColumnDescriptor*>& col_descs = Importer::get_column_descs();
  
  int64_t field_index = 0;
  std::vector<int> indices;
  
  // For each column descriptor, find field by name in input file schema and build list of indices.

  indices.reserve(num_columns);
  
  for (const auto cd : col_descs) {
    field_index = arrow_schema->GetFieldIndex(cd->columnName);

    // GetFieldIndex returns -1 if name not found
    if (field_index >= 0) {
      indices.push_back(static_cast<int>(field_index));
    }
  }
  
  if (indices.size() == 0) {
    // Log error that no column names matched in the input and output schemas
    import_status.rows_rejected++;
    LOG(ERROR) << "Incorrect schema (expected " << col_descs.size()
    << " columns, has zero columns in common with file containing" << num_columns << "columns)" << std::endl;
    return import_status;
  }

  auto ms = measure<>::execution([&]() {
    // Use one thread to read Parquet file
    
    // Initialize import buffers
    
    if (import_buffers_.size() > 0) {
      for (int i = 0; i < import_buffers_.size(); i++) {
        import_buffers_[i]->clear();
      }
      import_buffers_.clear();
    }
    
    for (const auto cd : col_descs) {
      import_buffers_.push_back(std::unique_ptr<TypedImportBuffer>(get_loader()->get_writer()->makeTypedImportBuffer(cd)));
    }

    for (int g = 0; g < num_row_groups; g++) {
      std::shared_ptr<::arrow::Table> row_group_table;
      
      for (const auto& p : import_buffers_)
        p->clear();

      if (get_debug_timing()) {
        us = measure<std::chrono::microseconds>::execution([&]() {
          status = reader->ReadRowGroup(g, indices, &row_group_table);
        });
        total_get_row_time_us += us;
      } else {
        status = reader->ReadRowGroup(g, indices, &row_group_table);
      }

      // Add row group table rows to TypedImportBuffers

      us = measure<std::chrono::microseconds>::execution([&]() {
        std::shared_ptr<::arrow::Column> column;
        int64_t col_idx = 0;
        
        for (int i = 0; i < row_group_table->num_columns(); i++) {
          column = row_group_table->column(i);

          // Decode dictionary column to underlying data type.

          // NOTE: Arrow has more dictionary encoded types than MapD which just
          //       dictionary encodes strings. The types dictionary encoded by
          //       Arrow have to be expanded to the underlying data type before
          //       being imported.
          //
          //       The dictionary indices used by Arrow and MapD are not
          //       interchangeable.
          //

          if (column->type()->id() == ::arrow::Type::DICTIONARY) {
            std::shared_ptr<::arrow::Column> columnDecoded;

            status = DecodeDictionaryToColumn(*column,
                                              ::arrow::default_memory_pool(),
                                              &columnDecoded);
            if (!status.ok()) {
              LOG(INFO) << "Failed to decode dictionary column " << column->name()
              << " at index " << i << " in row group " << g << ": " << status;
            }
            else {
              column.reset(columnDecoded.get());
            }
          }

          col_idx = columnNameToIndex(column->name());

          if (IsArrowScalarColumn(column)) {
            status = CopyArrowScalarColumn(column, import_buffers_[col_idx].get());
            if (!status.ok()) {
              LOG(INFO) << "Failed to convert scalar column " << column->name()
              << " at index " << i << " in row group " << g << ": " << status;
            }
          }
          else if (IsArrowListColumn(column)) {
            status = CopyArrowListColumn(column, import_buffers_[col_idx].get());
            if (!status.ok()) {
              LOG(INFO) << "Failed to convert list column " << column->name()
              << " at index " << i << " in row group " << g << ": " << status;
            }
          }
        }
      });

      total_str_to_val_time_us += us;

      import_status.rows_completed += row_group_table->num_rows();
      
      import_status.rows_estimated = num_row_groups * row_group_table->num_rows();
      set_import_status(get_import_id(), import_status);

      // Call addRows on writer for current row group
      try {
        load_ms += measure<>::execution([&]() { get_loader()->load(import_buffers_, row_group_table->num_rows()); });
      } catch (const std::exception& e) {
        LOG(WARNING) << e.what();
      }
    }
  });
  if (get_debug_timing() && import_status.rows_completed > 0) {
    LOG(INFO) << "Thread" << std::this_thread::get_id() << ":"
    << import_status.rows_completed << " rows inserted in "
    << static_cast<double>(ms) / 1000.0 << "sec, Insert Time: "
    << static_cast<double>(load_ms) / 1000.0
    << "sec, get_row: " << static_cast<double>(total_get_row_time_us) / 1000000.0
    << "sec, str_to_val: " << static_cast<double>(total_str_to_val_time_us) / 1000000.0
    << "sec" << std::endl;
  }

  // Call "close()" on writer to finish writing output file
  // (This is done outside the importer to allow multiple files to be combined.)

  return import_status;
}

}  // Namespace Importer
