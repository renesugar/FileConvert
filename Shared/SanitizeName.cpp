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
 * File:   MapDHandler.cpp
 * Author: michael
 *
 * Created on Jan 1, 2017, 12:40 PM
 */

#include "SanitizeName.h"

#include <sys/time.h>
#include <sys/types.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <boost/make_shared.hpp>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

#include <future>
#include <memory>
#include <string>
#include <fstream>
#include <random>
#include <map>
#include <cmath>
#include <typeinfo>
#include <thread>
#include <regex>

#include "../Parser/ReservedKeywords.h"

// NOTE: Call sanitize_name() on column names during import.
//
//       This function is also called in MapDHandler to remove invalid
//       characters from column names and table names.

std::string sanitize_name(const std::string& name) {
  boost::regex invalid_chars{R"([^0-9a-z_])", boost::regex::extended | boost::regex::icase};

  std::string col_name = boost::regex_replace(name, invalid_chars, "");
  if (reserved_keywords.find(boost::to_upper_copy<std::string>(col_name)) != reserved_keywords.end()) {
    col_name += "_";
  }
  return col_name;
}
