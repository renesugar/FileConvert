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
 * @file DelimitedSupport.h
 * @author Wei Hong <wei@mapd.com>
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for table import from file
 */
#ifndef _DELIMITEDSUPPORT_H_
#define _DELIMITEDSUPPORT_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>

#include <arrow/api.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

#include "../Shared/sqltypes.h"
#include "../Catalog/ColumnDescriptor.h"

#include "TypedImportBuffer.h"

#include "CopyParams.h"

#include "Detector.h"

namespace Importer_NS {

// Name for file mutexes
  
std::string make_mutex_name(std::string str);

// Import utility functions

const char* get_row(const char* buf,
                    const char* buf_end,
                    const char* entire_buf_end,
                    const CopyParams& copy_params,
                    bool is_begin,
                    const bool* is_array,
                    std::vector<std::string>& row,
                    bool& try_single_thread);

const std::string trim_space(const char* field, const size_t len);

const bool is_eol(const char& p, const std::string& line_delims);

int8_t* appendDatum(int8_t* buf, Datum d, const SQLTypeInfo& ti);

ArrayDatum StringToArray(const std::string& s, const SQLTypeInfo& ti, const CopyParams& copy_params);

void parseStringArray(const std::string& s, const CopyParams& copy_params, std::vector<std::string>& string_vec);

void addBinaryStringArray(const TDatum& datum, std::vector<std::string>& string_vec);

Datum TDatumToDatum(const TDatum& datum, const SQLTypeInfo& ti);

ArrayDatum TDatumToArrayDatum(const TDatum& datum, const SQLTypeInfo& ti);

size_t find_beginning(const char* buffer, size_t begin, size_t end, const CopyParams& copy_params);

size_t find_end(const char* buffer, size_t size, const CopyParams& copy_params);
  
ColumnDescriptor create_array_column(const SQLTypes& type, const std::string& name);

std::list<const ColumnDescriptor*> getAllColumnMetadataForDelimited(const std::string file_name, CopyParams&  copy_params);
  
}; // Namespace
#endif  // _DELIMITEDSUPPORT_H_
