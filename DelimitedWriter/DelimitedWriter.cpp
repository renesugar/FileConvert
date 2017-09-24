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
 * @file DelimitedWriter.cpp
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief DelimitedWriter class
 */

#include "DelimitedWriter.h"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <errno.h>
#include <string>

#ifdef HAVE_CUDA
#include <cuda.h>
#endif  // HAVE_CUDA

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/crc.hpp>
#include <glog/logging.h>

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
#include <thread>
#include <future>
#include <mutex>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Import/Importer.h"

namespace Importer_NS {

void DelimitedWriter::init() {
  if (file_path_.empty()) {
    // redirect output to std::cout

    // NOTE: This is done to allow output to be streamed into StreamInsert

    // file not used but a file name is needed to open a file to redirect
    file_path_ = "output.mapd";

    outfile_.open(file_path_);
    if (outfile_) {
      outfile_.basic_ios<char>::rdbuf(std::cout.rdbuf());
    }
  } else {
    outfile_.open(file_path_);
  }

  if (!outfile_)
    throw std::runtime_error("Cannot open file: " + file_path_);
}

bool DelimitedWriter::addRows(
        const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
        size_t row_count, bool checkpoint) {
  // NOTE: row_count parameter used during import; not export.

  // Map output column index to import column index
  // (When multiple files are being read, columns can be in a different order
  //  or missing.)

  std::vector<int> importColumnIndex;
  int col_idx = 0;

  importColumnIndex.resize(get_column_descs().size(), -1);

  for (int i = 0; i < import_buffers.size(); i++) {
    std::string col_name = import_buffers[i]->getColumnName();

    col_idx = columnNameToIndex(col_name);

    importColumnIndex[col_idx] = i;
  }

  // All TypedImportBuffer's have the same number of elements
  int num_rows   = import_buffers[0]->size();
  int import_idx = 0;

  if (copy_params_.has_header) {
    bool not_first = false;

    col_idx = 0;
    for (const auto cd : get_column_descs()) {
      std::string col_name = cd->columnName;
      if (col_name.empty()) {
        col_name = "result_" + std::to_string(col_idx + 1);
      }
      if (not_first)
        outfile_ << copy_params_.delimiter;
      else
        not_first = true;
      outfile_ << col_name;

      col_idx++;
    }
    outfile_ << copy_params_.line_delim;
  }

  for (int row_idx = 0; row_idx < num_rows; row_idx++) {
    bool not_first = false;

    col_idx = 0;

    for (const auto cd : get_column_descs()) {
      if (not_first)
        outfile_ << copy_params_.delimiter;
      else
        not_first = true;

      std::string value;

      // Get position of output column in input row

      import_idx = importColumnIndex[col_idx];

      if (import_idx < 0) {
        // Column not found in input row
        value = copy_params_.null_str;
      } else {
        value = import_buffers[import_idx]->toString(row_idx, copy_params_);
      }

      if (copy_params_.quoted && cd->columnType.is_string() && (value != copy_params_.null_str))
        outfile_ << copy_params_.quote;

      if (!copy_params_.quoted) {
        outfile_ << value;
      } else {
        size_t q = value.find(copy_params_.quote);
        if (q == std::string::npos) {
          outfile_ << value;
        } else {
          std::string str(value);
          while (q != std::string::npos) {
            str.insert(q, 1, copy_params_.escape);
            q = str.find(copy_params_.quote, q + 2);
          }
          outfile_ << str;
        }
      }

      if (copy_params_.quoted && cd->columnType.is_string() && (value != copy_params_.null_str)) {
        outfile_ << copy_params_.quote;
      }

      col_idx++;
    }

    outfile_ << copy_params_.line_delim;
  }

  return true;
}

bool DelimitedWriter::doCheckpoint() {
  return true;
}

void DelimitedWriter::checkpoint1() {
}

void DelimitedWriter::checkpoint2() {
}

mapd_shared_mutex& DelimitedWriter::get_mutex() {
  // TODO(renesugar): When reintegrating with MapD, come up with a
  //                 ChunkKey format for locking files for export file formats?
  return mapd_mutex_;
}

TypedImportBuffer* DelimitedWriter::makeTypedImportBuffer(
                                        const ColumnDescriptor* cd) {
  return new TypedImportBuffer(cd, nullptr /*get_string_dict(cd)*/);
}

void DelimitedWriter::close() {
  // Return if close() already called
  if (close_ == true)
    return;

  close_ = true;

  if (outfile_.is_open()) {
    outfile_.close();
  }
}

}  // namespace Importer_NS
