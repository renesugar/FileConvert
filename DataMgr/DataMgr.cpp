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
 * @file    DataMgr.cpp
 * @author Todd Mostak <todd@mapd.com>
 */

#include "DataMgr.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <sys/types.h>
#else
#include <unistd.h>
#endif

#include <boost/filesystem.hpp>

#include <algorithm>
#include <limits>

// TODO: Stub implementation of DataMgr (discard when reintegrating)

using namespace std;

namespace Data_Namespace {

DataMgr::DataMgr() {
}

DataMgr::~DataMgr() {
}

std::shared_ptr<mapd_shared_mutex> DataMgr::getMutexForChunkPrefix(const ChunkKey& keyPrefix) {
    // make new mutex
    std::shared_ptr<mapd_shared_mutex> tMutex = std::make_shared<mapd_shared_mutex>();
    return tMutex;
}

void DataMgr::checkpoint(const int db_id, const int tb_id) {  // checkpoint for individual table of DB
}

}  // Data_Namespace
