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
 * @file ParquetWriter.cpp
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief ParquetWriter class
 */

#include "ParquetWriter.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <errno.h>
#ifdef HAVE_CUDA
#include <cuda.h>
#endif  // HAVE_CUDA

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/crc.hpp>

#include <glog/logging.h>

#include <string>
#include <memory>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <stdexcept>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"

/*
//
// Rows are accumulated into row groups and written out as Parquet columns.
//
// References:
//
// (1) https://github.com/mapd/mapd-core/blob/master/QueryEngine/ResultSetConversion.cpp
// (2) https://github.com/blue-yonder/turbodbc/blob/master/cpp/turbodbc_arrow/Library/src/arrow_result_set.cpp
// (3) https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html
// (4) https://github.com/apache/arrow/blob/master/python/pyarrow/parquet.py
// (5) https://github.com/mapd/mapd-core/blob/master/SQLFrontend/mapdql.cpp
// (6) https://github.com/mapd/mapd-core/blob/master/Parser/ParserNode.cpp
// (7) https://github.com/cloudera/Impala/tree/cdh5-trunk/be/src/exec
// (8) https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/exec/parquet-metadata-utils.cc
// (9) https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileWriter.java
// (10) https://github.com/apache/parquet-cpp/blob/master/src/parquet/arrow/test-util.h
//
// ExportQueryStmt::execute (see (6)) - For export, a result set can be converted into an Arrow table.
//
*/

// TODO(renesugar): When using this code to export data from MapD, the indices
//                  of dictionary encoded strings in MapD will have to be
//                  replaced with the index to the offset in the per-column
//                  dictionary of the Arrow table.
//

namespace Importer_NS {

void ParquetWriter::init() {
  ARROW_RETURN_VOID_NOT_OK_ELSE(::arrow::io::FileOutputStream::Open(file_path_,
                                                      false /*append*/, &file_),
    LOG(ERROR) << "Initializing Parquet writer for file " << file_path_
                                << " failed. (" << status_.ToString() << ")";);

  /*
  // Writer properties can be set by calling ParquetWriter::Builder() to get a 
  // reference to the parquet::WriterProperties::Builder.
   
  parquet::WriterProperties::Builder builder;
  builder.compression(parquet::Compression::SNAPPY);
  builder.version(ParquetVersion::PARQUET_2_0);
  builder.compression("gzip", Compression::GZIP);
  builder.compression(Compression::SNAPPY);
  builder.encoding(Encoding::DELTA_BINARY_PACKED);
  builder.encoding("delta-length", Encoding::DELTA_LENGTH_BYTE_ARRAY);
  
  builder.memory_pool(::arrow::default_memory_pool());
  
  builder.enable_dictionary();
  
  builder.disable_dictionary();
  
  // path = column_path->ToDotString()
  builder.enable_dictionary(const std::string& path);
  
  builder.enable_dictionary(const std::shared_ptr<schema::ColumnPath>& path);
  
  // path = column_path->ToDotString()
  builder.disable_dictionary(const std::string& path);
  
  builder.disable_dictionary(const std::shared_ptr<schema::ColumnPath>& path);
  
  builder.dictionary_pagesize_limit(int64_t dictionary_psize_limit);
  
  builder.write_batch_size(int64_t write_batch_size);
  
  builder.data_pagesize(int64_t pg_size);
  
  // ::parquet::ParquetVersion::PARQUET_1_0
  // ::parquet::ParquetVersion::PARQUET_2_0
  builder.version(ParquetVersion::type version);
  
  builder.created_by(const std::string& created_by);
  
  // Data encodings:
  // ::parquet::Encoding::PLAIN
  // ::parquet::Encoding::PLAIN_DICTIONARY
  // ::parquet::Encoding::RLE
  // ::parquet::Encoding::BIT_PACKED
  // ::parquet::Encoding::DELTA_BINARY_PACKED
  // ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
  // ::parquet::Encoding::DELTA_BYTE_ARRAY
  // ::parquet::Encoding::RLE_DICTIONARY

  //
  // Define the default encoding that is used when we don't utilise dictionary encoding.
  //
  // Methods where a path is specified set the encoding on a particular column.
  //
  // This encoding applies if dictionary encoding is disabled or if we fallback
  // as the dictionary grew too large.
  //
  // NOTE: Can't use dictionary encoding as fallback encoding:
  //       1) Encoding::PLAIN_DICTIONARY
  //       2) Encoding::RLE_DICTIONARY
  //
  builder.encoding(Encoding::type encoding_type)
  
  // path = column_path->ToDotString()
  builder.encoding(const std::string& path, Encoding::type encoding_type);
  
  builder.encoding(
                   const std::shared_ptr<schema::ColumnPath>& path, Encoding::type encoding_type) ;
  
  // Compression:
  //
  // ::parquet::Compression::UNCOMPRESSED
  // ::parquet::Compression::SNAPPY
  // ::parquet::Compression::GZIP
  // ::parquet::Compression::LZO
  // ::parquet::Compression::BROTLI
  
  // sets compression for default column properties
  builder.compression(Compression::type codec);
  
  // path = column_path->ToDotString()
  builder.compression(const std::string& path, Compression::type codec);
  
  builder.compression(
                      const std::shared_ptr<schema::ColumnPath>& path, Compression::type codec);
  
  builder.enable_statistics();
  
  builder.disable_statistics();
  
  // path = column_path->ToDotString()
  builder.enable_statistics(const std::string& path);
  
  builder.enable_statistics(const std::shared_ptr<schema::ColumnPath>& path);
  
  // path = column_path->ToDotString()
  builder.disable_statistics(const std::string& path);
  
  builder.disable_statistics(const std::shared_ptr<schema::ColumnPath>& path);

  std::shared_ptr<::parquet::WriterProperties> properties = builder.build();

  std::shared_ptr<::parquet::WriterProperties> properties =
  ::parquet::default_writer_properties();
   
  ::parquet::arrow::FileWriter writer(::arrow::default_memory_pool(), MakeWriter(schema));
  */
}

bool ParquetWriter::addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count, bool checkpoint) {
  // NOTE: row_count parameter used during import; not export.

  // Map output column index to import column index
  // (When multiple files are being read, columns can be
  // in a different order or missing.)

  std::vector<int> importColumnIndex;

  // All TypedImportBuffer's have the same number of elements
  int num_rows = (import_buffers.empty() ? 0 : import_buffers[0]->size());
  int num_cols = get_column_descs().size();
  int col_idx  = 0;

  importColumnIndex.resize(num_cols, -1);

  for (int i = 0; i < import_buffers.size(); i++) {
    std::string col_name = import_buffers[i]->getColumnName();

    col_idx = columnNameToIndex(col_name);

    importColumnIndex[col_idx] = i;
  }

  // Lock the file using a mutex named using a CRC32 calculated from file path
  // ::boost::interprocess::named_mutex mutex(::boost::interprocess::open_or_create, file_mutex_name_);
  // ::boost::interprocess::scoped_lock<named_mutex> lock(mutex);

  // if output file is not open, open the output file
  if (init_) {
    // Construct the Arrow schema

    // Create a preliminary schema to gather the data using the underlying data
    // type in a dictionary.
    //
    // Just before the table is created, create a new schema with the dictionary
    // types for columns that have to be dictionary encoded.
    //

    schema_ = ArrowSchema(get_column_descs(), metadata_);

    // Open the Parquet file writer
    ARROW_RETURN_BOOL_NOT_OK(::parquet::arrow::FileWriter::Open(*schema_,
                  ::arrow::default_memory_pool(),
                  file_,
                  properties_,
                  arrow_properties_,
                  &file_writer_));

    // Create Arrow Builders for all columns
    arrow_builders_.clear();
    for (const auto cd : get_column_descs()) {
      arrow_builders_.push_back(SQLTypeToArrowBuilder(cd->columnType));

      // Identify columns that should be dictionary encoded

      if (cd->columnType.get_compression() == kENCODING_DICT) {
        dict_columns_.push_back(true);
      } else {
        dict_columns_.push_back(false);
      }
    }

    // File has been opened
    init_  = false;
    close_ = true;
  }

  if (schema_->num_fields() != import_buffers.size()) {
    LOG(ERROR) << "Number of fields in schema does not match number of import buffers. " << file_path_;
    return false;
  }

  int offset = 0;
  int row_copy_count = 0;
  int row_remain_count = num_rows;
  std::shared_ptr<arrow::Table> table;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  // Write one row group at a time to the Parquet file
  while (row_remain_count > 0) {
    row_copy_count = row_group_size() - arrow_builders_[0]->length();

    if (row_copy_count > row_remain_count) {
      row_copy_count = row_remain_count;
    }

    // Add rows to Arrow builder
    for (int col_idx = 0; col_idx < num_cols; col_idx++) {
      // Get position of output column in input row

      int import_idx = importColumnIndex[col_idx];

      if (import_idx < 0) {
        // Column not found in input row

        // copy null values
        ARROW_RETURN_BOOL_NOT_OK(import_buffers[import_idx]->copy_nulls_to_arrow_builder(offset, offset + row_copy_count, arrow_builders_[col_idx]));
      } else {
        ARROW_RETURN_BOOL_NOT_OK(import_buffers[import_idx]->copy_to_arrow_builder(offset, offset + row_copy_count, arrow_builders_[col_idx]));
      }
    }

    if (arrow_builders_[0]->length() < row_group_size()) {
      // Not enough rows to write a row group (write last remaining rows in close())
      break;
    }

    // Make table
    arrays.reserve(arrow_builders_.size());
    for (size_t i = 0; i < arrow_builders_.size(); i++) {
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_BOOL_NOT_OK(arrow_builders_[i]->Finish(&array));

      // Dictionary encode array if necessary

      // TODO(renesugar): ::parquet::arrow::FileWriter does not currently
      //                  support writing dictionary encoded types.
      dict_columns_[i] = false; // TODO(renesugar): Remove this line to enable dictionary encoding
      if (dict_columns_[i] == true) {
        std::shared_ptr<arrow::Array> arrayDict;

        ARROW_RETURN_BOOL_NOT_OK(EncodeArrayToDictionary(*array,
                                                         ::arrow::default_memory_pool(),
                                                         &arrayDict));
        arrays.emplace_back(arrayDict);
      } else {
        arrays.emplace_back(array);
      }
    }

    // Update the Arrow schema to reflect any dictionary encoded columns
    std::shared_ptr<::arrow::Schema> schema = ArrowSchema(schema_, arrays);

    ARROW_RETURN_BOOL_NOT_OK(MakeTable(schema, arrays, &table));

    // Write table to file (each table contains one row group or any remaining rows)
    ARROW_RETURN_BOOL_NOT_OK(file_writer_->WriteTable(*table, table->num_rows()));

    // Create Arrow Builders for all columns
    arrow_builders_.clear();
    for (int i = 0; i < num_cols; i++) {
      arrow_builders_.push_back(import_buffers[i]->make_arrow_builder());
    }

    offset += row_copy_count;
    row_remain_count -= row_copy_count;
  }

  return true;
}

bool ParquetWriter::doCheckpoint() {
  return true;
}

void ParquetWriter::checkpoint1() {
}

void ParquetWriter::checkpoint2() {
}

mapd_shared_mutex& ParquetWriter::get_mutex() {
  // TODO(renesugar): When reintegrating with MapD, come up with a
  //                 ChunkKey format for locking files for export file formats?

  /*
   ChunkKey chunkKey(3);
   chunkKey[0] = 0;  // top level db_id
   chunkKey[1] = 0;  // top level tb_id

   // CRC32 of file path
   boost::crc_32_type result;
   result.process_bytes(file_path_.data(), file_path_.length());

   chunkKey.push_back(result.checksum());

   */
  return mapd_mutex_;
}

TypedImportBuffer* ParquetWriter::makeTypedImportBuffer(const ColumnDescriptor* cd) {
  return new TypedImportBuffer(cd, nullptr /*get_string_dict(cd)*/);
}

void ParquetWriter::close() {
  // Return if close() already called
  if (close_ == false)
    return;

  close_ = false;

  // Write any remaining rows
  if (arrow_builders_[0]->length() > 0) {
    // Make table
    std::shared_ptr<arrow::Table> table;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t i = 0; i < arrow_builders_.size(); i++) {
      std::shared_ptr<arrow::Array> array;
      status_ = arrow_builders_[i]->Finish(&array);

      // Dictionary encode array if necessary

      // TODO(renesugar): ::parquet::arrow::FileWriter does not currently
      //                  support writing dictionary types.
      dict_columns_[i] = false; // TODO(renesugar): Remove this line to enable dictionary encoding
      if (dict_columns_[i] == true) {
        std::shared_ptr<arrow::Array> arrayDict;
        ARROW_RETURN_VOID_NOT_OK(EncodeArrayToDictionary(*array,
                                                         ::arrow::default_memory_pool(),
                                                         &arrayDict));
        arrays.emplace_back(arrayDict);
      } else {
        arrays.emplace_back(array);
      }
    }

    // Update the Arrow schema to reflect any dictionary encoded columns
    std::shared_ptr<::arrow::Schema> schema = ArrowSchema(schema_, arrays);

    ARROW_RETURN_VOID_NOT_OK(MakeTable(schema, arrays, &table));

    ARROW_RETURN_VOID_NOT_OK(table->ValidateColumns());

    // Write table to file (each table contains one row group or any remaining rows)
    ARROW_RETURN_VOID_NOT_OK(file_writer_->WriteTable(*table, table->num_rows()));
  }

  // Close the FileWriter
  ARROW_RETURN_VOID_NOT_OK(file_writer_->Close());

  // Write the bytes to file
  ARROW_RETURN_VOID_NOT_OK(file_->Close());
}

}  // namespace Importer_NS
