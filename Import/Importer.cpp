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
 * @file Importer.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for Importer class
 */

#include "Importer.h"

#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

#include <string>
#include <memory>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <list>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../QueryEngine/TypePunning.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Shared/unreachable.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

bool debug_timing = false;

static mapd_shared_mutex status_mutex;
static std::map<std::string, ImportStatus> import_status_map;

Importer::Importer(Loader* providedLoader, const std::list<const ColumnDescriptor*>& cds, const std::string& f, const CopyParams& p)
    : columnDescriptors(cds), file_path(f), copy_params(p), loader(providedLoader), load_failed(false) {
  import_id = boost::filesystem::path(file_path).filename().string();

  size_t col_idx = 0;

  // Associate the import buffer index with each column name

  for (const auto cd : columnDescriptors) {
    columnName_to_idx_[cd->columnName] = col_idx;

    col_idx++;
  }
}

Importer::~Importer() {
}

ImportStatus Importer::get_import_status(const std::string& import_id) {
  mapd_shared_lock<mapd_shared_mutex> read_lock(status_mutex);
  return import_status_map.at(import_id);
}

void Importer::set_import_status(const std::string& import_id, ImportStatus is) {
  mapd_lock_guard<mapd_shared_mutex> write_lock(status_mutex);
  is.end = std::chrono::steady_clock::now();
  is.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(is.end - is.start);
  import_status_map[import_id] = is;
}

bool Importer::get_debug_timing() {
  return debug_timing;
}

bool Loader::loadNoCheckpoint(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count) {
  return writer_->addRows(import_buffers, row_count, false);
}

bool Loader::load(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count) {
  return writer_->addRows(import_buffers, row_count, true);
}

void Importer::load(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count) {
  if (!loader->loadNoCheckpoint(import_buffers, row_count))
    load_failed = true;
}

}  // namespace Importer_NS
