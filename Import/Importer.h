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
 * @file Importer.h
 * @author Wei Hong < wei@mapd.com>
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Importer class for table import from file
 */
#ifndef _IMPORTER_H_
#define _IMPORTER_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>
#include <arrow/api.h>

#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <vector>
#include <map>
#include <memory>
#include "../Shared/fixautotools.h"
#include "../Shared/checked_alloc.h"
#include "../Shared/mapd_shared_mutex.h"

#include "TypedImportBuffer.h"

#include "CopyParams.h"

#include "DelimitedSupport.h"

#include "ArrowSupport.h"

// NOTE: Importer_NS::Importer::import() is called from:
//
// 1) CopyTableStmt::execute in ParserNode.cpp
// 2) MapDHandler::import_table in MapDHandler.cpp
// 3) See importer_factory in ParserNode.cpp and MapDHandler.cpp
// 4) importGDAL in MapDHandler.cpp
//
// Importer_NS::Loader::load() is called from:
//
// 1) MapDHandler::load_table_binary
// 2) MapDHandler::load_table
//
// For export, see ExportQueryStmt::execute in ParserNode.cpp
//

// TODO(renesugar): Add importers and writers for genomic data to import/export
//                  genomic data to/from MapD (e.g. CRAM).
//
// https://samtools.github.io/hts-specs/
// http://varianttools.sourceforge.net
// http://gemini.readthedocs.io/en/latest/content/database_schema.html
//
// Apache ADAM can create Parquet files with Avro schemas.
//
// https://github.com/bigdatagenomics/bdg-formats
// https://github.com/bigdatagenomics/adam
// https://github.com/bigdatagenomics/utils
//
// Hail also uses Parquet files.
//
// https://hail.is/index.html
// https://github.com/hail-is/hail.git
//
// CRAM file format solves similar problems as the Parquet file format.
//
// http://samtools.github.io/hts-specs/CRAMv3.pdf
// http://www.ebi.ac.uk/ena/software/cram-toolkit
//
//
// https://github.com/mikessh/vdjtools
// https://github.com/mikessh/vdjtools-examples
//
// https://github.com/churchlab/vdj
//
// https://github.com/laserson/pytools
//
// https://github.com/opencb/hpg-bigdata
//
// https://software.broadinstitute.org/gatk/
// https://github.com/broadinstitute/gatk
//
// https://github.com/ga4gh/ga4gh-schemas
// https://ga4gh-schemas.readthedocs.io/en/latest/
//
// https://github.com/HadoopGenomics/Hadoop-BAM
//
// https://github.com/biojava/biojava
//
// https://github.com/arq5x/bedtools2
//
// https://github.com/hammerlab
//
// https://julialang.org/blog/2016/04/biojulia2016
//
// https://databricks.com/blog/2016/05/24/parallelizing-genome-variant-analysis.html
//

// NOTE: Sample data:
//       https://data.opendatasoft.com/explore/?sort=modified
//       https://github.com/dwyl/english-words.git

// TODO(renesugar): Add support for importing and exporting SQLite files.
//                  https://www.gaia-gis.it/fossil/libspatialite/home

// TODO(renesugar): Does MapD support time series data?
//                  https://www.timescale.com


namespace Importer_NS {

// TODO(renesugar): Making mapd_shared_mutex a class derived from
//                  std::shared_timed_mutex, etc. instead of a typedef would
//                  allow a forward reference to avoid pulling in the header
//                  defining mapd_shared_mutex
//
class Writer {
  public:
  explicit Writer(const std::list<const ColumnDescriptor*>& columnDescriptors)
      : column_descs_(columnDescriptors) {
    size_t col_idx = 0;

    // Associate the output index with each column name

    for (const auto cd : column_descs_) {
      columnName_to_idx_[cd->columnName] = col_idx;

      col_idx++;
    }
  }

  virtual ~Writer() {
  }

  virtual bool addRows(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count, bool checkpoint) = 0;
  virtual bool doCheckpoint() = 0;
  virtual void checkpoint1() = 0;
  virtual void checkpoint2() = 0;
  virtual mapd_shared_mutex& get_mutex() = 0;
  virtual TypedImportBuffer* makeTypedImportBuffer(const ColumnDescriptor* cd) = 0;
  virtual void close() = 0;

  const std::list<const ColumnDescriptor*>& get_column_descs() const { return column_descs_; }

  protected:
  const std::list<const ColumnDescriptor*> column_descs_;
  std::map<std::string, size_t> columnName_to_idx_;

  int columnNameToIndex(const std::string& columnName) {
    auto it = columnName_to_idx_.find(columnName);

    if (it == columnName_to_idx_.end()) {
      return -1;
    }

    return it->second;
  }
};

class Loader {
 public:
  explicit Loader(Writer* w) {
    writer_.reset(w);
  }

  Writer* get_writer() { return writer_.get(); }
  virtual bool load(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count);
  virtual bool loadNoCheckpoint(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers,
                                size_t row_count);

 protected:
  std::unique_ptr<Writer> writer_;
};

struct ImportStatus {
  std::chrono::steady_clock::time_point start;
  std::chrono::steady_clock::time_point end;
  size_t rows_completed;
  size_t rows_estimated;
  size_t rows_rejected;
  std::chrono::duration<size_t, std::milli> elapsed;
  bool load_truncated;
  ImportStatus()
      : start(std::chrono::steady_clock::now()),
        rows_completed(0),
        rows_estimated(0),
        rows_rejected(0),
        elapsed(0),
        load_truncated(0) {}

  ImportStatus& operator+=(const ImportStatus& is) {
    rows_completed += is.rows_completed;
    rows_rejected += is.rows_rejected;

    return *this;
  }
};

class Importer {
public:
  Importer(Loader* providedLoader,
           const std::list<const ColumnDescriptor*>& cds,
           const std::string& f, const CopyParams& p);
  ~Importer();
  // TODO(renesugar): Parameter on import() is used by GDALImporter to map
  //                  column names to source names which appear to be exactly
  //                  the same. Is this necessary?
  //                  The parameter is unused by DelimitedImporter
  //                  and ParquetImporter.
  //
  virtual ImportStatus import(std::map<std::string, std::string>& param) = 0;
  const CopyParams& get_copy_params() const { return copy_params; }
  const std::list<const ColumnDescriptor*>& get_column_descs() const { return columnDescriptors; }
  void load(const std::vector<std::unique_ptr<TypedImportBuffer>>& import_buffers, size_t row_count);
  Loader* get_loader() { return loader; }
  const std::string& get_file_path() { return file_path; }
  std::string get_import_id() { return import_id; }
  bool get_load_failed() { return load_failed; }
  void set_load_failed(bool status) { load_failed = status; }
  static ImportStatus get_import_status(const std::string& id);
  static void set_import_status(const std::string& id, const ImportStatus is);
  static bool get_debug_timing();

protected:
  ImportStatus import_status;
  std::map<std::string, size_t> columnName_to_idx_;

  int columnNameToIndex(const std::string& columnName) {
    auto it = columnName_to_idx_.find(columnName);

    if (it == columnName_to_idx_.end()) {
      return -1;
    }

    return it->second;
  }

private:
  const std::list<const ColumnDescriptor*>& columnDescriptors;
  const std::string& file_path;
  std::string import_id;
  const CopyParams& copy_params;
  Loader* loader;
  bool load_failed;
};

};  // namespace Importer_NS

#endif  // _IMPORTER_H_
