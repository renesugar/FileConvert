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
 * @file DelimitedImporter.h
 * @author Wei Hong < wei@mapd.com>
 * @brief Delimited file Importer class for table import from file
 */
#ifndef _DELIMITEDIMPORTER_H_
#define _DELIMITEDIMPORTER_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

#include <vector>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

#include "../Shared/fixautotools.h"
#include "../Shared/checked_alloc.h"

#include "../Import/Importer.h"

namespace Importer_NS {

class DelimitedImporter : public Importer {
public:
  DelimitedImporter(Loader* providedLoader,
                    const std::list<const ColumnDescriptor*>& cds,
                    const std::string& f,
                    const CopyParams& p)
  : Importer(providedLoader, cds, f, p)  {
    file_size = 0;
    max_threads = 0;
    p_file = nullptr;
    buffer[0] = nullptr;
    buffer[1] = nullptr;
    which_buf = 0;
    auto is_array = std::unique_ptr<bool[]>(new bool[Importer::get_column_descs().size()]);
    int i = 0;
    bool has_array = false;
    for (auto& p : Importer::get_column_descs()) {
      if (p->columnType.get_type() == kARRAY) {
        is_array.get()[i] = true;
        has_array = true;
      } else {
        is_array.get()[i] = false;
      }
      ++i;
    }
    if (has_array)
      is_array_a = std::unique_ptr<bool[]>(is_array.release());
    else
      is_array_a = std::unique_ptr<bool[]>(nullptr);
  }

  ~DelimitedImporter() {
    if (p_file != nullptr)
      fclose(p_file);
    if (buffer[0] != nullptr)
      free(buffer[0]);
    if (buffer[1] != nullptr)
      free(buffer[1]);
  }

  ImportStatus import(std::map<std::string, std::string>& param);

  std::vector<std::unique_ptr<TypedImportBuffer>>& get_import_buffers(int i) {
    return import_buffers_vec[i];
  }
  const bool* get_is_array() const { return is_array_a.get(); }

private:
  void initialize_import_buffers(size_t alloc_size);

  int max_threads;
  FILE* p_file;
  size_t file_size;
  char* buffer[2];
  int which_buf;
  std::vector<std::vector<std::unique_ptr<TypedImportBuffer>>> import_buffers_vec;
  std::unique_ptr<bool[]> is_array_a;
  ImportStatus import_status;
};

};  // namespace Importer_NS

#endif  // _DELIMITEDIMPORTER_H_

