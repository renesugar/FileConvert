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
 * @file	ParquetImporter.h
 * @author	Rene Sugar <rene.sugar@gmail.com>
 * @brief	Parquet file import supported by MapD
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 **/
#ifndef _PARQUETIMPORTER_H
#define _PARQUETIMPORTER_H

#include <arrow/io/file.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <unordered_map>
#include <string>
#include <vector>
#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>

#include "../Import/Importer.h"

namespace Importer_NS {

class ParquetImporter : Importer {
public:
  ParquetImporter(Loader* providedLoader,
                  const std::list<const ColumnDescriptor*>& cds,
                  const std::string& f,
                  const CopyParams& p)
  : Importer(providedLoader, cds, f, p) {}

  ImportStatus import(std::map<std::string, std::string>& param);

private:
  std::vector<std::unique_ptr<TypedImportBuffer>> import_buffers_;
  ImportStatus import_status;
};

}  // namespace Importer_NS

#endif  // _PARQUETIMPORTER_H
