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
 * @file MapDImporter.h
 * @author Wei Hong < wei@mapd.com>
 * @brief MapD Importer class for table import from file
 */
#ifndef _MAPDIMPORTER_H_
#define _MAPDIMPORTER_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

#include "../Shared/fixautotools.h"
#include "../Catalog/TableDescriptor.h"
#include "../Catalog/Catalog.h"
#include "../Fragmenter/Fragmenter.h"
#include "../Shared/checked_alloc.h"

#include "../Import/Importer.h"
#include "../DelimitedImporter/DelimitedImporter.h"

namespace Importer_NS {

class MapDImporter : DelimitedImporter {
public:
  // const Catalog_Namespace::Catalog& c,
  // const TableDescriptor* t,
  // providedLoader = new Loader(new MapDWriter(c, t))
  MapDImporter(Loader* providedLoader, const std::string& f,
               const CopyParams& p)
  : DelimitedImporter(providedLoader, f, p) {}

  ImportStatus import(std::map<std::string, std::string> param);

private:
};

};  // namespace Importer_NS

#endif  // _MAPDIMPORTER_H_
