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
 * @file MapDWriter.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for Importer class
 */

#include "MapDWriter.h"

#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>

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

#include "../QueryEngine/SqlTypesLayout.h"
#include "../QueryEngine/TypePunning.h"
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Shared/unreachable.h"

#include "gen-cpp/MapD.h"

#include "../Import/Importer.h"

namespace Importer_NS {

namespace {

  int64_t int_value_at(const TypedImportBuffer& import_buffer, const size_t index) {
    const auto& ti = import_buffer.getTypeInfo();
    const int8_t* values_buffer{nullptr};
    if (ti.is_string()) {
      CHECK_EQ(kENCODING_DICT, ti.get_compression());
      values_buffer = import_buffer.getStringDictBuffer();
    } else {
      values_buffer = import_buffer.getAsBytes();
    }
    CHECK(values_buffer);
    switch (ti.get_logical_size()) {
      case 1: {
        return values_buffer[index];
      }
      case 2: {
        return reinterpret_cast<const int16_t*>(values_buffer)[index];
      }
      case 4: {
        return reinterpret_cast<const int32_t*>(values_buffer)[index];
      }
      case 8: {
        return reinterpret_cast<const int64_t*>(values_buffer)[index];
      }
      default:
        CHECK(false);
    }
    UNREACHABLE();
    return 0;
  }

  float float_value_at(const TypedImportBuffer& import_buffer, const size_t index) {
    const auto& ti = import_buffer.getTypeInfo();
    CHECK_EQ(kFLOAT, ti.get_type());
    const auto values_buffer = import_buffer.getAsBytes();
    return reinterpret_cast<const float*>(may_alias_ptr(values_buffer))[index];
  }

  double double_value_at(const TypedImportBuffer& import_buffer, const size_t index) {
    const auto& ti = import_buffer.getTypeInfo();
    CHECK_EQ(kDOUBLE, ti.get_type());
    const auto values_buffer = import_buffer.getAsBytes();
    return reinterpret_cast<const double*>(may_alias_ptr(values_buffer))[index];
  }

}  // namespace

void MapDWriter::distributeToShards(std::vector<OneShardBuffers>& all_shard_import_buffers,
                                std::vector<size_t>& all_shard_row_counts,
                                const OneShardBuffers& import_buffers,
                                const size_t row_count,
                                const size_t shard_count) {
  all_shard_row_counts.resize(shard_count);
  for (size_t shard_idx = 0; shard_idx < shard_count; ++shard_idx) {
    all_shard_import_buffers.emplace_back();
    for (const auto& typed_import_buffer : import_buffers) {
      all_shard_import_buffers.back().emplace_back(
                                                   new TypedImportBuffer(typed_import_buffer->getColumnDesc(), typed_import_buffer->getStringDictionary()));
    }
  }
  CHECK_GT(table_desc->shardedColumnId, 0);
  int col_idx{0};
  const ColumnDescriptor* shard_col_desc{nullptr};
  for (const auto col_desc : column_descs) {
    ++col_idx;
    if (col_idx == table_desc->shardedColumnId) {
      shard_col_desc = col_desc;
      break;
    }
  }
  CHECK(shard_col_desc);
  CHECK_LE(static_cast<size_t>(table_desc->shardedColumnId), import_buffers.size());
  auto& shard_column_input_buffer = import_buffers[table_desc->shardedColumnId - 1];
  const auto& shard_col_ti = shard_col_desc->columnType;
  CHECK(shard_col_ti.is_integer() || (shard_col_ti.is_string() && shard_col_ti.get_compression() == kENCODING_DICT));
  if (shard_col_ti.is_string()) {
    const auto payloads_ptr = shard_column_input_buffer->getStringBuffer();
    CHECK(payloads_ptr);
    shard_column_input_buffer->addDictEncodedString(*payloads_ptr);
  }
  for (size_t i = 0; i < row_count; ++i) {
    const auto val = int_value_at(*shard_column_input_buffer, i);
    const auto shard = val % shard_count;
    auto& shard_output_buffers = all_shard_import_buffers[shard];
    for (size_t col_idx = 0; col_idx < import_buffers.size(); ++col_idx) {
      const auto& input_buffer = import_buffers[col_idx];
      const auto& col_ti = input_buffer->getTypeInfo();
      const auto type = col_ti.is_decimal() ? decimal_to_int_type(col_ti) : col_ti.get_type();
      switch (type) {
        case kBOOLEAN:
          shard_output_buffers[col_idx]->addBoolean(int_value_at(*input_buffer, i));
          break;
        case kSMALLINT:
          shard_output_buffers[col_idx]->addSmallint(int_value_at(*input_buffer, i));
          break;
        case kINT:
          shard_output_buffers[col_idx]->addInt(int_value_at(*input_buffer, i));
          break;
        case kBIGINT:
          shard_output_buffers[col_idx]->addBigint(int_value_at(*input_buffer, i));
          break;
        case kFLOAT:
          shard_output_buffers[col_idx]->addFloat(float_value_at(*input_buffer, i));
          break;
        case kDOUBLE:
          shard_output_buffers[col_idx]->addDouble(double_value_at(*input_buffer, i));
          break;
        case kTEXT:
        case kVARCHAR:
        case kCHAR: {
          CHECK_LT(i, input_buffer->getStringBuffer()->size());
          shard_output_buffers[col_idx]->addString((*input_buffer->getStringBuffer())[i]);
          break;
        }
        case kTIME:
        case kTIMESTAMP:
        case kDATE:
          shard_output_buffers[col_idx]->addTime(int_value_at(*input_buffer, i));
          break;
        case kARRAY:
          if (IS_STRING(col_ti.get_subtype())) {
            CHECK(input_buffer->getStringArrayBuffer());
            CHECK_LT(i, input_buffer->getStringArrayBuffer()->size());
            const auto& input_arr = (*(input_buffer->getStringArrayBuffer()))[i];
            shard_output_buffers[col_idx]->addStringArray(input_arr);
          } else {
            shard_output_buffers[col_idx]->addArray((*input_buffer->getArrayBuffer())[i]);
          }
          break;
        default:
          CHECK(false);
      }
    }
    ++all_shard_row_counts[shard];
  }
}

bool MapDWriter::addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
                      size_t row_count,
                      bool checkpoint) {
  if (table_desc->nShards) {
    std::vector<OneShardBuffers> all_shard_import_buffers;
    std::vector<size_t> all_shard_row_counts;
    const auto shard_tables = catalog.getPhysicalTablesDescriptors(table_desc);
    distributeToShards(all_shard_import_buffers, all_shard_row_counts, import_buffers, row_count, shard_tables.size());
    bool success = true;
    for (size_t shard_idx = 0; shard_idx < shard_tables.size(); ++shard_idx) {
      if (!all_shard_row_counts[shard_idx]) {
        continue;
      }
      success = success && loadToShard(all_shard_import_buffers[shard_idx],
                                       all_shard_row_counts[shard_idx],
                                       shard_tables[shard_idx],
                                       checkpoint);
    }
    return success;
  }
  return loadToShard(import_buffers, row_count, table_desc, checkpoint);
}

bool MapDWriter::loadToShard(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
                         size_t row_count,
                         const TableDescriptor* shard_table,
                         bool checkpoint) {
  Fragmenter_Namespace::InsertData ins_data(insert_data);
  ins_data.numRows = row_count;
  bool success = true;
  for (const auto& import_buff : import_buffers) {
    DataBlockPtr p;
    if (import_buff->getTypeInfo().is_number() || import_buff->getTypeInfo().is_time() ||
        import_buff->getTypeInfo().get_type() == kBOOLEAN) {
      p.numbersPtr = import_buff->getAsBytes();
    } else if (import_buff->getTypeInfo().is_string()) {
      auto string_payload_ptr = import_buff->getStringBuffer();
      if (import_buff->getTypeInfo().get_compression() == kENCODING_NONE) {
        p.stringsPtr = string_payload_ptr;
      } else {
        CHECK_EQ(kENCODING_DICT, import_buff->getTypeInfo().get_compression());
        import_buff->addDictEncodedString(*string_payload_ptr);
        p.numbersPtr = import_buff->getStringDictBuffer();
      }
    } else {
      CHECK(import_buff->getTypeInfo().get_type() == kARRAY);
      if (IS_STRING(import_buff->getTypeInfo().get_subtype())) {
        CHECK(import_buff->getTypeInfo().get_compression() == kENCODING_DICT);
        import_buff->addDictEncodedStringArray(*import_buff->getStringArrayBuffer());
        p.arraysPtr = import_buff->getStringArrayDictBuffer();
      } else
        p.arraysPtr = import_buff->getArrayBuffer();
    }
    ins_data.data.push_back(p);
  }
  {
    try {
      if (checkpoint)
        shard_table->fragmenter->insertData(ins_data);
      else
        shard_table->fragmenter->insertDataNoCheckpoint(ins_data);
    } catch (std::exception& e) {
      LOG(ERROR) << "Fragmenter Insert Exception: " << e.what();
      success = false;
    }
  }
  return success;
}

void MapDWriter::init() {
  insert_data.databaseId = catalog.get_currentDB().dbId;
  insert_data.tableId = table_desc->tableId;
  for (auto cd : column_descs) {
    insert_data.columnIds.push_back(cd->columnId);
    if (cd->columnType.get_compression() == kENCODING_DICT) {
      CHECK(cd->columnType.is_string() || cd->columnType.is_string_array());
      const auto dd = catalog.getMetadataForDict(cd->columnType.get_comp_param());
      CHECK(dd);
      dict_map[cd->columnId] = dd->stringDict.get();
    }
  }
  insert_data.numRows = 0;
}

bool MapDWriter::doCheckpoint() {
  return (get_table_desc()->persistenceLevel == Data_Namespace::MemoryLevel::DISK_LEVEL);
}

void MapDWriter::checkpoint1() {
  if (doCheckpoint()) {  // only checkpoint disk-resident tables
    // checkpoint before going again
    const auto shard_tables = get_catalog().getPhysicalTablesDescriptors(get_table_desc());
    for (const auto shard_table : shard_tables) {
      get_catalog().get_dataMgr().checkpoint(get_catalog().get_currentDB().dbId,
                                                     shard_table->tableId);
    }
  }
}

void MapDWriter::checkpoint2() {
  get_catalog().get_dataMgr().checkpoint(get_catalog().get_currentDB().dbId,
                                         get_table_desc()->tableId);
}

mapd_shared_mutex& MapDWriter::get_mutex() {
  ChunkKey chunkKey = {get_catalog().get_currentDB().dbId,
    get_table_desc()->tableId};

  return *get_catalog().get_dataMgr().getMutexForChunkPrefix(chunkKey).get();
}

TypedImportBuffer* MapDWriter::makeTypedImportBuffer(const ColumnDescriptor* cd) {
  return new TypedImportBuffer(cd, get_string_dict(cd));
}

void MapDWriter::close() {
// nothing to close
}

}  // namespace Importer_NS
