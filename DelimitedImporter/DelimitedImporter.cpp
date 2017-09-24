
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
 * @file DelimitedImporter.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for Importer class
 */

#include "DelimitedImporter.h"

#include <unistd.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

#include <string>
#include <algorithm>
#include <map>
#include <memory>
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
#include <thread>
#include <future>
#include <mutex>

using std::ostream;

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

template <typename T>
ostream& operator<<(ostream& out, const std::vector<T>& v) {
  out << "[";
  size_t last = v.size() - 1;
  for (size_t i = 0; i < v.size(); ++i) {
    out << v[i];
    if (i != last)
      out << ", ";
  }
  out << "]";
  return out;
}

static ImportStatus import_thread(int thread_id,
                                  Importer* importer,
                                  const char* buffer,
                                  size_t begin_pos,
                                  size_t end_pos,
                                  size_t total_size) {
  ImportStatus import_status;
  int64_t total_get_row_time_us = 0;
  int64_t total_str_to_val_time_us = 0;
  auto load_ms = measure<>::execution([]() {});
  auto ms = measure<>::execution([&]() {
    const CopyParams& copy_params = importer->get_copy_params();
    const std::list<const ColumnDescriptor*>& col_descs = importer->get_column_descs();
    size_t begin = find_beginning(buffer, begin_pos, end_pos, copy_params);
    const char* thread_buf = buffer + begin_pos + begin;
    const char* thread_buf_end = buffer + end_pos;
    const char* buf_end = buffer + total_size;
    bool try_single_thread = false;
    DelimitedImporter* delimitedImporter = static_cast<DelimitedImporter*>(importer);
    std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers = delimitedImporter->get_import_buffers(thread_id);
    auto us = measure<std::chrono::microseconds>::execution([&]() {});
    for (const auto& p : import_buffers)
      p->clear();
    std::vector<std::string> row;
    for (const char* p = thread_buf; p < thread_buf_end; p++) {
      row.clear();
      if (Importer::get_debug_timing()) {
        us = measure<std::chrono::microseconds>::execution([&]() {
          p = get_row(p,
                      thread_buf_end,
                      buf_end,
                      copy_params,
                      p == thread_buf,
                      delimitedImporter->get_is_array(),
                      row,
                      try_single_thread);
        });
        total_get_row_time_us += us;
      } else {
        p = get_row(p, thread_buf_end, buf_end, copy_params, p == thread_buf,
                    delimitedImporter->get_is_array(), row, try_single_thread);
      }
      if (row.size() != col_descs.size()) {
        import_status.rows_rejected++;
        LOG(ERROR) << "Incorrect Row (expected " << col_descs.size()
        << " columns, has " << row.size() << "): " << row;
        continue;
      }

      us = measure<std::chrono::microseconds>::execution([&]() {
        size_t col_idx = 0;
        try {
          for (const auto cd : col_descs) {
            bool is_null = (row[col_idx] == copy_params.null_str);
            if (!cd->columnType.is_string() && row[col_idx].empty())
              is_null = true;
            import_buffers[col_idx]->add_value(cd, row[col_idx], is_null, copy_params);
            ++col_idx;
          }
          import_status.rows_completed++;
        } catch (const std::exception& e) {
          for (size_t col_idx_to_pop = 0; col_idx_to_pop < col_idx; ++col_idx_to_pop) {
            import_buffers[col_idx_to_pop]->pop_value();
          }
          import_status.rows_rejected++;
          LOG(ERROR) << "Input exception thrown: " << e.what() << ". Row discarded, issue at column : " << (col_idx + 1)
          << " data :" << row;
        }
      });
      total_str_to_val_time_us += us;
    }
    if (import_status.rows_completed > 0) {
      load_ms = measure<>::execution([&]() { importer->load(import_buffers, import_status.rows_completed); });
    }
  });
  if (Importer::get_debug_timing() && import_status.rows_completed > 0) {
    LOG(INFO) << "Thread" << std::this_thread::get_id() << ":"
    << import_status.rows_completed << " rows inserted in "
    << static_cast<double>(ms) / 1000.0 << "sec, Insert Time: "
    << static_cast<double>(load_ms) / 1000.0
    << "sec, get_row: " << static_cast<double>(total_get_row_time_us) / 1000000.0
    << "sec, str_to_val: " << static_cast<double>(total_str_to_val_time_us) / 1000000.0
    << "sec" << std::endl;
  }
  return import_status;
}

void DelimitedImporter::initialize_import_buffers(size_t alloc_size) {
  if (get_copy_params().threads == 0)
    max_threads = sysconf(_SC_NPROCESSORS_CONF);
  else
    max_threads = get_copy_params().threads;
  // deal with small files
  if (file_size < alloc_size) {
    alloc_size = file_size;
  }
  buffer[0] = reinterpret_cast<char*>(checked_malloc(alloc_size));
  if (max_threads > 1)
    buffer[1] = reinterpret_cast<char*>(checked_malloc(alloc_size));
  for (int i = 0; i < max_threads; i++) {
    import_buffers_vec.push_back(std::vector<std::unique_ptr<TypedImportBuffer>>());
    for (const auto cd : get_column_descs())
      import_buffers_vec[i].push_back(
                                      std::unique_ptr<TypedImportBuffer>(get_loader()->get_writer()->makeTypedImportBuffer(cd)));
  }
}

#define IMPORT_FILE_BUFFER_SIZE 100000000  // 100M file size
#define MIN_FILE_BUFFER_SIZE 50000         // 50K min buffer

ImportStatus DelimitedImporter::import(std::map<std::string, std::string>& param) {
  bool load_truncated = false;
  set_import_status(get_import_id(), import_status);
  p_file = fopen(get_file_path().c_str(), "rb");
  if (!p_file) {
    throw std::runtime_error("fopen failure for '" + get_file_path() + "': " + strerror(errno));
  }
  (void)fseek(p_file, 0, SEEK_END);
  file_size = ftell(p_file);

  size_t alloc_size = IMPORT_FILE_BUFFER_SIZE;

  // Initialize import buffers
  initialize_import_buffers(alloc_size);

  size_t current_pos = 0;
  size_t end_pos;
  (void)fseek(p_file, current_pos, SEEK_SET);
  size_t size = fread(reinterpret_cast<void*>(buffer[which_buf]), 1,
                      alloc_size, p_file);
  bool eof_reached = false;
  size_t begin_pos = 0;
  if (get_copy_params().has_header) {
    size_t i;
    for (i = 0; i < size && buffer[which_buf][i] != get_copy_params().line_delim; i++)
      ;
    if (i == size)
      LOG(WARNING) << "No line delimiter in block." << std::endl;
    begin_pos = i + 1;
  }

  while (size > 0) {
    // for each process through a buffer take a table lock
    mapd_unique_lock<mapd_shared_mutex> tableLevelWriteLock(get_loader()->get_writer()->get_mutex());  // prevent two threads from trying to insert into the same table simultaneously
    if (eof_reached)
      end_pos = size;
    else
      end_pos = find_end(buffer[which_buf], size, get_copy_params());
    if (size <= alloc_size) {
      max_threads = std::min(max_threads,
        static_cast<int>(std::ceil((static_cast<double>(end_pos - begin_pos) / MIN_FILE_BUFFER_SIZE))));
    }
    if (max_threads == 1) {
      import_status += import_thread(0, this, buffer[which_buf], begin_pos, end_pos, end_pos);
      current_pos += end_pos;
      (void)fseek(p_file, current_pos, SEEK_SET);
      size = fread(reinterpret_cast<void*>(buffer[which_buf]), 1,
                   IMPORT_FILE_BUFFER_SIZE, p_file);
      if (size < IMPORT_FILE_BUFFER_SIZE && feof(p_file))
        eof_reached = true;
    } else {
      std::vector<std::future<ImportStatus>> threads;
      for (int i = 0; i < max_threads; i++) {
        size_t begin = begin_pos + i * ((end_pos - begin_pos) / max_threads);
        size_t end = (i < max_threads - 1) ? begin_pos + (i + 1) * ((end_pos - begin_pos) / max_threads) : end_pos;
        threads.push_back(
                          std::async(std::launch::async, import_thread, i, this, buffer[which_buf], begin, end, end_pos));
      }
      current_pos += end_pos;
      which_buf = (which_buf + 1) % 2;
      (void)fseek(p_file, current_pos, SEEK_SET);
      size = fread(reinterpret_cast<void*>(buffer[which_buf]), 1,
                   IMPORT_FILE_BUFFER_SIZE, p_file);
      if (size < IMPORT_FILE_BUFFER_SIZE && feof(p_file))
        eof_reached = true;
      for (auto& p : threads)
        p.wait();
      for (auto& p : threads)
        import_status += p.get();
    }
    import_status.rows_estimated =
      (static_cast<float>(file_size) / current_pos) * import_status.rows_completed;
    set_import_status(get_import_id(), import_status);
    begin_pos = 0;
    if (import_status.rows_rejected > get_copy_params().max_reject) {
      load_truncated = true;
      LOG(ERROR) << "Maximum rows rejected exceeded. Halting load";
      break;
    }
    // checkpoint disk-resident tables before going again
    get_loader()->get_writer()->checkpoint1();
  }
  free(buffer[0]);
  buffer[0] = nullptr;
  free(buffer[1]);
  buffer[1] = nullptr;
  fclose(p_file);
  p_file = nullptr;

  import_status.load_truncated = load_truncated;
  return import_status;
}

}  // namespace Importer_NS
