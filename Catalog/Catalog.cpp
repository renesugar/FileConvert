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

/**
 * @file		Catalog.cpp
 * @author	Todd Mostak <todd@map-d.com>, Wei Hong <wei@map-d.com>
 * @brief		Functions for System Catalogs
 *
 * Copyright (c) 2014 MapD Technologies, Inc.  All rights reserved.
 **/

#include "Catalog.h"
#include <list>
#include <exception>
#include <cassert>
#include <memory>
#include <random>
#include <boost/filesystem.hpp>
#include <boost/uuid/sha1.hpp>

// TODO(renesugar): Stub implementation of Catalog (discard when reintegrating)

using std::runtime_error;
using std::string;
using std::map;
using std::list;
using std::pair;
using std::vector;

namespace Catalog_Namespace {

Catalog::Catalog(const string& basePath,
                 const DBMetadata& curDB,
                 std::shared_ptr<Data_Namespace::DataMgr> dataMgr
                 )
    : basePath_(basePath),
      currentDB_(curDB),
      dataMgr_(dataMgr)
{
}

Catalog::~Catalog() {
}

const DictDescriptor* Catalog::getMetadataForDict(int dictId, bool loadDict) const {
    return nullptr;
}

list<const ColumnDescriptor*> Catalog::getAllColumnMetadataForTable(const int tableId,
                                                                    const bool fetchSystemColumns,
                                                                    const bool fetchVirtualColumns) const {
  list<const ColumnDescriptor*> columnDescriptors;
  return columnDescriptors;
}

std::vector<const TableDescriptor*> Catalog::getPhysicalTablesDescriptors(
                                                                          const TableDescriptor* logicalTableDesc) const {
  std::vector<const TableDescriptor*> physicalTables;
  
  return physicalTables;
}

}  // Catalog_Namespace
