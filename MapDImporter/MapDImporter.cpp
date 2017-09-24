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

#include "MapDImporter.h"

#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

#include <map>
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
#include <thread>
#include <future>
#include <mutex>

#include "../QueryEngine/SqlTypesLayout.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {

ImportStatus MapDImporter::import(std::map<std::string, std::string> param) {
  ImportStatus import_status = DelimitedImporter::import(param);

  // checkpoint before going again
  if (get_loader()->get_writer()->doCheckpoint()) {
    // only checkpoint disk-resident tables
    // TODO(MAT): we need to review whether this checkpoint process makes sense
    auto ms = measure<>::execution([&]() {
      if (!get_load_failed()) {
        for (auto& p : get_import_buffers(0)) {
          if (!p->stringDictCheckpoint()) {
            LOG(ERROR) << "Checkpointing Dictionary for Column "
              << p->getColumnDesc()->columnName << " failed.";
            set_load_failed(true);
            break;
          }
        }
        get_loader()->get_writer()->checkpoint2();
      }
    });
    if (get_debug_timing())
      LOG(INFO) << "Checkpointing took " << static_cast<double>(ms) / 1000.0
        << " Seconds." << std::endl;
  }

  return import_status;
}

}  // namespace Importer_NS

