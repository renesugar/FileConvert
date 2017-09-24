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
 * @file MapDWriter.h
 * @author Wei Hong < wei@mapd.com>
 * @brief Importer class for table import from file
 */
#ifndef _MAPDWRITER_H_
#define _MAPDWRITER_H_

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
#include "../Catalog/TableDescriptor.h"
#include "../Catalog/Catalog.h"
#include "../Fragmenter/Fragmenter.h"
#include "../Shared/checked_alloc.h"

#include "../Import/Importer.h"

namespace Importer_NS {

class MapDWriter : Writer {
public:
  MapDWriter(const Catalog_Namespace::Catalog& c, const TableDescriptor* t)
  : Writer(c.getAllColumnMetadataForTable(t->tableId, false, false)),
    catalog(c), table_desc(t) {
    init();
  }

  ~MapDWriter() {
  }

  const Catalog_Namespace::Catalog& get_catalog() const { return catalog; }
  const TableDescriptor* get_table_desc() const { return table_desc; }
  const std::list<const ColumnDescriptor*>& get_column_descs() const { return column_descs; }
  const Fragmenter_Namespace::InsertData& get_insert_data() const { return insert_data; }
  StringDictionary* get_string_dict(const ColumnDescriptor* cd) const {
    if ((cd->columnType.get_type() != kARRAY || !IS_STRING(cd->columnType.get_subtype())) &&
        (!cd->columnType.is_string() || cd->columnType.get_compression() != kENCODING_DICT))
      return nullptr;
    return dict_map.at(cd->columnId);
  }

  bool addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count, bool checkpoint);

  bool doCheckpoint();

  void checkpoint1();

  void checkpoint2();

  mapd_shared_mutex& get_mutex();

  TypedImportBuffer* makeTypedImportBuffer(const ColumnDescriptor* cd);

  void close();

protected:
  const Catalog_Namespace::Catalog& catalog;
  const TableDescriptor* table_desc;
  std::list<const ColumnDescriptor*> column_descs;
  Fragmenter_Namespace::InsertData insert_data;
  std::map<int, StringDictionary*> dict_map;
  void init();
  typedef std::vector<std::unique_ptr<TypedImportBuffer>> OneShardBuffers;
  void distributeToShards(std::vector<OneShardBuffers>& all_shard_import_buffers,
                          std::vector<size_t>& all_shard_row_counts,
                          const OneShardBuffers& import_buffers,
                          const size_t row_count,
                          const size_t shard_count);

private:
  bool loadToShard(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
                   size_t row_count,
                   const TableDescriptor* shard_table,
                   bool checkpoint);
};

};  // namespace Importer_NS

#endif  // _MAPDWRITER_H_
