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
 * @file DataGen.h
 * @author MapD
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Functions for generating random data
 */
#ifndef _DATAGEN_H_
#define _DATAGEN_H_

#include <memory>
#include <string>
#include <cstdint>

#include <arrow/api.h>

#include "CopyParams.h"

namespace Importer_NS {
  
std::string GenerateArrowColumnValue(::arrow::Type::type type_id,
                                     ::arrow::Type::type elem_type_id,
                                     const CopyParams& cp);

// TODO(renesugar): Where is sample data retrieved in
//                  MapDHandler::detect_column_types used by MapD?

std::vector<std::vector<std::string>> getSampleRowsForArrowSchema(
                                const std::shared_ptr<::arrow::Schema>& schema,
                                const CopyParams& cp,
                                size_t n);

};  // namespace Importer_NS
#endif  // _DATAGEN_H_
