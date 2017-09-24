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
 * @file	DelimitedWriter.h
 * @author	Rene Sugar <rene.sugar@gmail.com>
 * @brief	Delimited file writing supported by MapD
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 **/
#ifndef _DELIMITEDWRITER_H
#define _DELIMITEDWRITER_H

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <glog/logging.h>

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

class DelimitedWriter : Writer {
public:
  DelimitedWriter(const std::string& f,
                  const CopyParams& p,
                  const std::list<const ColumnDescriptor*>& columnDescriptors)
  : Writer(columnDescriptors), init_(true), close_(false), file_path_(f),
    copy_params_(p) {
    init();
  }

  ~DelimitedWriter() {
  }

  bool addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
               size_t row_count, bool checkpoint);

  bool doCheckpoint();

  void checkpoint1();

  void checkpoint2();

  mapd_shared_mutex& get_mutex();

  TypedImportBuffer* makeTypedImportBuffer(const ColumnDescriptor* cd);

  void close();

protected:
  bool init_;
  bool close_;
  std::string file_path_;
  // TODO(renesugar): This is not used for writing to files.
  mapd_shared_mutex mapd_mutex_;
  std::ofstream outfile_;
  const CopyParams& copy_params_;
  void init();
};

}  // namespace Importer_NS

#endif  // _DELIMITEDWRITER_H
