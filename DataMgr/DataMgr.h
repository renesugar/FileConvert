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
 * @file    DataMgr.h
 * @author Todd Mostak <todd@map-d.com>
 */
#ifndef DATAMGR_H
#define DATAMGR_H

#include "../Shared/types.h"
#include "../Shared/sqltypes.h"
#include "../Shared/mapd_shared_mutex.h"

#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

// TODO: Stub implementation of DataMgr (discard when reintegrating)

namespace Data_Namespace {

class DataMgr {

 public:
  DataMgr();
  ~DataMgr();
  std::shared_ptr<mapd_shared_mutex> getMutexForChunkPrefix(
      const ChunkKey& keyPrefix);  // used to manage locks at higher level
  void checkpoint(const int db_id, const int tb_id);  // checkpoint for individual table of DB
};
}  // Data_Namespace

#endif  // DATAMGR_H
