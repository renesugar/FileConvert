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
 * @file    Catalog.h
 * @author  Todd Mostak <todd@map-d.com>, Wei Hong <wei@map-d.com>
 * @brief   This file contains the class specification and related data structures for Catalog.
 *
 * This file contains the Catalog class specification. The Catalog class is responsible for storing metadata
 * about stored objects in the system (currently just relations).  At this point it does not take advantage of the
 * database storage infrastructure; this likely will change in the future as the buildout continues. Although it
 * persists the metainfo on disk, at database startup it reads everything into in-memory dictionaries for fast access.
 *
 */

#ifndef CATALOG_H
#define CATALOG_H

#include <atomic>
#include <cstdint>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "ColumnDescriptor.h"
#include "TableDescriptor.h"
#include "DictDescriptor.h"
#include "../DataMgr/DataMgr.h"

// TODO(renesugar): Stub implementation of Catalog (discard when reintegrating)

namespace Catalog_Namespace {

/*
 * @type DBMetadata
 * @brief metadata for a mapd database
 */
struct DBMetadata {
  int32_t dbId;
  std::string dbName;
  int32_t dbOwner;
};

/**
 * @type Catalog
 * @brief class for a per-database catalog.  also includes metadata for the
 * current database and the current user.
 */

class Catalog {
 public:
  /**
   * @brief Constructor - takes basePath to already extant
   * data directory for writing
   * @param basePath directory path for writing catalog
                           * @param dbName name of the database
                           * @param fragmenter Fragmenter object
   * metadata - expects for this directory to already exist
   */

  Catalog(const std::string& basePath,
          const DBMetadata& curDB,
          std::shared_ptr<Data_Namespace::DataMgr> dataMgr
          );

  /**
   * @brief Destructor - deletes all
   * ColumnDescriptor and TableDescriptor structures
   * which were allocated on the heap and writes
   * Catalog to Sqlite
   */
  virtual ~Catalog();

  /**
   * @brief Returns a list of pointers to constant ColumnDescriptor structs for all the columns from a particular table
   * specified by table id
   * @param tableId table id we want the column metadata for
   * @return list of pointers to const ColumnDescriptor structs - one
   * for each and every column in the table
   *
   */

  std::list<const ColumnDescriptor*> getAllColumnMetadataForTable(const int tableId,
                                                                  const bool fetchSystemColumns,
                                                                  const bool fetchVirtualColumns) const;

  std::vector<const TableDescriptor*> getPhysicalTablesDescriptors(const TableDescriptor* logicalTableDesc) const;

  const DBMetadata& get_currentDB() const { return currentDB_; }
  void set_currentDB(const DBMetadata& db) { currentDB_ = db; }
  Data_Namespace::DataMgr& get_dataMgr() const { return *dataMgr_; }

  const DictDescriptor* getMetadataForDict(int dictId, bool loadDict = true) const;

  protected:
  std::string basePath_; /**< The OS file system path containing the catalog files. */
  DBMetadata currentDB_;
  std::shared_ptr<Data_Namespace::DataMgr> dataMgr_;
};

}  // Catalog_Namespace

#endif  // CATALOG_H
