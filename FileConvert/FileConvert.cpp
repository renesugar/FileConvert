/*
 * Copyright 2017 Rene Sugar
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
 * @file FileConvert.cpp
 * @author Rene Sugar <rene.sugar@gmail.com>
 * @brief Convert data files into a Parquet or CSV/TSV file
 *
 * Copyright (c) 2017 Rene Sugar.  All rights reserved.
 */

#include <arrow/api.h>
#include <arrow/io/file.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/schema.h>

#include <boost/filesystem.hpp>
#include <boost/make_shared.hpp>
#include <boost/make_unique.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include <glog/logging.h>

#include <vector>
#include <map>
#include <cassert>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <list>
#include <memory>
#include <string>
#include <algorithm>

// NOTE: Include StringDictionary.h before Importer.h to get the
//       implementation for standalone programs.
#include "../StringDictionary/StringDictionary.h"
#include "../Shared/SanitizeName.h"
#include "../Shared/measure.h"

#include "../Import/Importer.h"

#include "../DelimitedImporter/DelimitedImporter.h"
#include "../ParquetImporter/ParquetImporter.h"
#include "../GDALImporter/GDALImporter.h"

#include "../DelimitedWriter/DelimitedWriter.h"
#include "../ParquetWriter/ParquetWriter.h"

// NOTE: Set GDAL_DATA environment variable to the path of the support files
//      for the GDAL library.
// e.g.
//      export GDAL_DATA=/usr/local/share/gdal
//

//#define MAPD_RELEASE "1.0.0"
#include "MapDRelease.h"

namespace parquet {

std::istream& operator>>(std::istream& in, ::parquet::Encoding::type& encoding) {
    std::string token;
    in >> token;
    if (token == "PLAIN")
        encoding = parquet::Encoding::PLAIN;
    else if (token == "PLAIN_DICTIONARY")
        encoding = parquet::Encoding::PLAIN_DICTIONARY;
    else if (token == "RLE")
        encoding = parquet::Encoding::RLE;
    else if (token == "BIT_PACKED")
        encoding = parquet::Encoding::BIT_PACKED;
    else if (token == "DELTA_BINARY_PACKED")
        encoding = parquet::Encoding::DELTA_BINARY_PACKED;
    else if (token == "DELTA_LENGTH_BYTE_ARRAY")
        encoding = parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    else if (token == "DELTA_BYTE_ARRAY")
        encoding = parquet::Encoding::DELTA_BYTE_ARRAY;
    else if (token == "RLE_DICTIONARY")
        encoding = parquet::Encoding::RLE_DICTIONARY;
    else
        in.setstate(std::ios_base::failbit);
    return in;
}

std::istream& operator>>(std::istream& in, ::parquet::Compression::type& codec) {
    std::string token;
    in >> token;
    if (token == "UNCOMPRESSED")
        codec = parquet::Compression::UNCOMPRESSED;
    else if (token == "SNAPPY")
        codec = parquet::Compression::SNAPPY;
    else if (token == "GZIP")
        codec = parquet::Compression::GZIP;
    else if (token == "LZO")
        codec = parquet::Compression::LZO;
    else if (token == "BROTLI")
        codec = parquet::Compression::BROTLI;
    else
        in.setstate(std::ios_base::failbit);
    return in;
}

std::istream& operator>>(std::istream& in, ::parquet::ParquetVersion::type& version) {
    std::string token;
    in >> token;
    if (token == "PARQUET_1_0")
        version = parquet::ParquetVersion::PARQUET_1_0;
    else if (token == "PARQUET_2_0")
        version = parquet::ParquetVersion::PARQUET_2_0;
    else
        in.setstate(std::ios_base::failbit);
    return in;
}

}  // namespace parquet

namespace arrow {

std::istream& operator>>(std::istream& in, ::arrow::TimeUnit::type& timeunit) {
  std::string token;
  in >> token;
  if (token == "SECOND")
    timeunit = ::arrow::TimeUnit::SECOND;
  else if (token == "MILLI")
    timeunit = ::arrow::TimeUnit::MILLI;
  else if (token == "MICRO")
    timeunit = ::arrow::TimeUnit::MICRO;
  else if (token == "NANO")
    timeunit = ::arrow::TimeUnit::NANO;
  else
    in.setstate(std::ios_base::failbit);
  return in;
}

}  // namespace arrow

/*
 // Writer properties can be set by calling ParquetWriter::Builder() to get a
 // reference to the parquet::WriterProperties::Builder.
 
 parquet::WriterProperties::Builder builder;
 builder.compression(parquet::Compression::SNAPPY);
 builder.version(ParquetVersion::PARQUET_2_0);
 builder.compression("gzip", Compression::GZIP);
 builder.compression(Compression::SNAPPY);
 builder.encoding(Encoding::DELTA_BINARY_PACKED);
 builder.encoding("delta-length", Encoding::DELTA_LENGTH_BYTE_ARRAY);
 
 builder.memory_pool(::arrow::default_memory_pool());
 
 builder.enable_dictionary();
 
 builder.disable_dictionary();
 
 // path = column_path->ToDotString()
 builder.enable_dictionary(const std::string& path);
 
 builder.enable_dictionary(const std::shared_ptr<schema::ColumnPath>& path);
 
 // path = column_path->ToDotString()
 builder.disable_dictionary(const std::string& path);
 
 builder.disable_dictionary(const std::shared_ptr<schema::ColumnPath>& path);
 
 builder.dictionary_pagesize_limit(int64_t dictionary_psize_limit);
 
 builder.write_batch_size(int64_t write_batch_size);
 
 builder.data_pagesize(int64_t pg_size);
 
 // ::parquet::ParquetVersion::PARQUET_1_0
 // ::parquet::ParquetVersion::PARQUET_2_0
 builder.version(ParquetVersion::type version);
 
 builder.created_by(const std::string& created_by);
 
 // Data encodings:
 // ::parquet::Encoding::PLAIN
 // ::parquet::Encoding::PLAIN_DICTIONARY
 // ::parquet::Encoding::RLE
 // ::parquet::Encoding::BIT_PACKED
 // ::parquet::Encoding::DELTA_BINARY_PACKED
 // ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY
 // ::parquet::Encoding::DELTA_BYTE_ARRAY
 // ::parquet::Encoding::RLE_DICTIONARY
 
 //
 // Define the default encoding that is used when we don't utilise dictionary encoding.
 //
 // Methods where a path is specified set the encoding on a particular column.
 //
 // This encoding applies if dictionary encoding is disabled or if we fallback
 // as the dictionary grew too large.
 //
 // NOTE: Can't use dictionary encoding as fallback encoding:
 //       1) Encoding::PLAIN_DICTIONARY
 //       2) Encoding::RLE_DICTIONARY
 //
 builder.encoding(Encoding::type encoding_type)
 
 // path = column_path->ToDotString()
 builder.encoding(const std::string& path, Encoding::type encoding_type);
 
 builder.encoding(
 const std::shared_ptr<schema::ColumnPath>& path, Encoding::type encoding_type) ;
 
 // Compression:
 //
 // ::parquet::Compression::UNCOMPRESSED
 // ::parquet::Compression::SNAPPY
 // ::parquet::Compression::GZIP
 // ::parquet::Compression::LZO
 // ::parquet::Compression::BROTLI
 
 // sets compression for default column properties
 builder.compression(Compression::type codec);
 
 // path = column_path->ToDotString()
 builder.compression(const std::string& path, Compression::type codec);
 
 builder.compression(
 const std::shared_ptr<schema::ColumnPath>& path, Compression::type codec);
 
 builder.enable_statistics();
 
 builder.disable_statistics();
 
 // path = column_path->ToDotString()
 builder.enable_statistics(const std::string& path);
 
 builder.enable_statistics(const std::shared_ptr<schema::ColumnPath>& path);
 
 // path = column_path->ToDotString()
 builder.disable_statistics(const std::string& path);
 
 builder.disable_statistics(const std::shared_ptr<schema::ColumnPath>& path);
 
 std::shared_ptr<::parquet::WriterProperties> properties = builder.build();
 
 std::shared_ptr<::parquet::WriterProperties> properties =
 ::parquet::default_writer_properties();
 
 ::parquet::arrow::FileWriter writer(::arrow::default_memory_pool(), MakeWriter(schema));
 */

#include "StringUtil.h"

void check_geospatial_files(const boost::filesystem::path file_path) {
  const std::list<std::string> shp_ext{".shp", ".shx", ".dbf"};
  if (std::find(shp_ext.begin(), shp_ext.end(), boost::algorithm::to_lower_copy(file_path.extension().string())) !=
      shp_ext.end()) {
    for (auto ext : shp_ext) {
      auto aux_file = file_path;
      if (!boost::filesystem::exists(aux_file.replace_extension(boost::algorithm::to_upper_copy(ext))) &&
          !boost::filesystem::exists(aux_file.replace_extension(ext))) {
        throw std::runtime_error("required file for shapefile does not exist: " + aux_file.filename().string());
      }
    }
  }
}

int main(int argc, const char** argv) {
  // File options
  std::vector<std::string> input_files;
  std::string              config_file("FileConvert.conf");
  std::string              output_filename;
  std::string              output_ext(".csv");
  std::string              schema_filename;
  std::string              metadata_filename;

  // Copy params
  Importer_NS::CopyParams  copy_params;

  Importer_NS::CopyParams  output_copy_params;

  char   cp_delimiter(',');
  std::string cp_null_str(" ");
  bool   cp_has_header(true);
  bool   cp_quoted(true);      // does the input have any quoted fields
  char   cp_quote('"');
  char   cp_escape('"');
  char   cp_line_delim('\n');
  char   cp_array_delim(',');
  char   cp_array_begin('{');
  char   cp_array_end('}');
  int    cp_threads(0);
  size_t cp_max_reject(100000);  // maximum number of records that can be rejected before copy fails
  Importer_NS::TableType cp_table_type(Importer_NS::TableType::DELIMITED);

  std::shared_ptr<Importer_NS::Loader> providedLoader = nullptr;

  // True if any Parquet command line options are set
  bool parquet_isset = false;

  // True if the writer for the output file has been created
  bool writer_isset  = false;

  // True if any Arrow command line options are set
  bool arrow_isset = false;

  // Column properties
  parquet::Encoding::type encoding = parquet::Encoding::PLAIN;
  parquet::Compression::type codec = parquet::Compression::UNCOMPRESSED;
  bool dictionary_enabled = false;
  bool statistics_enabled = false;
  std::vector<std::string> enable_dictionary_paths;
  std::vector<std::string> disable_dictionary_paths;
  std::vector<std::string> enable_statistics_paths;
  std::vector<std::string> disable_statistics_paths;

  // Schema properties

  std::vector<std::string> notnull_columns;
  bool print_schema = false;

  // Writer properties

  ::parquet::WriterProperties::Builder builder;

  ::parquet::arrow::ArrowWriterProperties::Builder arrow_builder;

  std::shared_ptr<::parquet::WriterProperties> properties = ::parquet::default_writer_properties();

  std::shared_ptr<::parquet::arrow::ArrowWriterProperties> arrow_properties =
    ::parquet::arrow::default_arrow_writer_properties();

  std::shared_ptr<::arrow::KeyValueMetadata> metadata = nullptr;

  int64_t row_group_size               = 1024 * 1024;
  int64_t write_batch_size             = properties->write_batch_size();
  int64_t dictionary_pagesize_limit    = properties->dictionary_pagesize_limit();
  int64_t pagesize                     = properties->data_pagesize();
  parquet::ParquetVersion::type parquet_version = properties->version();
  std::string created_by("FileConvert 1.0.0");

  // Arrow Writer properties

  bool enable_deprecated_int96_timestamps = false;

  ::arrow::TimeUnit::type timeunit = ::arrow::TimeUnit::MILLI;

  // Importer

  std::list<const ColumnDescriptor*> schema_col_descs;

  /*
  const std::function<std::unique_ptr<Importer_NS::Importer>(const std::shared_ptr<Importer_NS::Loader>,
                                                             const std::string&,
                                                             const Importer_NS::CopyParams&)>& importer_factory);
   */

  std::unique_ptr<std::string> return_message;

  namespace po = boost::program_options;

  // Initialize Google's logging library.
  google::InitGoogleLogging(argv[0]);

  po::options_description desc("Options");
  desc.add_options()("help,h", "Print help messages");
  desc.add_options()("version,v", "Print Release Version Number");
  desc.add_options()("config", po::value<std::string>(&config_file), "Path to importer.conf");
  desc.add_options()("input-file,i", po::value<std::vector<std::string>>(), "Path to input file(s)");
  desc.add_options()("output,o", po::value<std::string>(&output_filename), "Path to output file");
  desc.add_options()("schema,s", po::value<std::string>(&schema_filename), "Path to schema file");
  desc.add_options()("metadata,m", po::value<std::string>(&metadata_filename), "Path to metadata file");

  // Declare a group of options that will be allowed both on command line and in the config file
  po::options_description config("Configuration");

  // Copy parameters

  config.add_options()("has-header",
                     po::value<bool>(&cp_has_header),
                     "Whether the source contains a header row (true/false, default true)");
  config.add_options()("quoted",
                     po::value<bool>(&cp_quoted),
                     "Whether the source contains quoted fields (true/false, default true)");

  config.add_options()("quote",      po::value<char>(&cp_quote)->default_value(cp_quote), "Quote character");
  config.add_options()("escape",     po::value<char>(&cp_escape)->default_value(cp_escape), "Escape character");
  config.add_options()("delim",      po::value<char>(&cp_delimiter)->default_value(cp_delimiter), "Field delimiter");
  config.add_options()("null",       po::value<std::string>(&cp_null_str), "NULL string");
  config.add_options()("line-delim", po::value<char>(&cp_line_delim)->default_value(cp_line_delim), "Line delimiter");
  config.add_options()("array-delim",
                     po::value<char>(&cp_array_delim)->default_value(cp_array_delim), "Array delimiter");
  config.add_options()("array-begin",
                     po::value<char>(&cp_array_begin)->default_value(cp_array_begin), "Array begin delimiter");
  config.add_options()("array-end",
                     po::value<char>(&cp_array_end)->default_value(cp_array_end), "Array end delimiter");
  config.add_options()("threads", po::value<int>(&cp_threads)->default_value(cp_threads), "Number of threads");
  config.add_options()("max-reject",
                       po::value<size_t>(&cp_max_reject)->default_value(cp_max_reject), "Maximum number of rejected rows allowed");

  // Schema options

  config.add_options()("notnull", po::value<std::vector<std::string>>(), "Column(s) that cannot contain null values");
  config.add_options()("print-schema",
                       po::value<bool>(&print_schema)->default_value(print_schema)->implicit_value(true),
                       "Print the schema of each input file");

  // Parquet options

  config.add_options()("dictionary-enabled",
                     po::value<bool>(&dictionary_enabled)->default_value(dictionary_enabled)->implicit_value(true),
                     "Enable dictionary");
  config.add_options()("statistics-enabled",
                     po::value<bool>(&statistics_enabled)->default_value(statistics_enabled)->implicit_value(true),
                     "Enable statistics");
  config.add_options()("enable-dictionary-path", po::value<std::vector<std::string>>(), "Path to column(s) with dictionary enabled");
  config.add_options()("disable-dictionary-path", po::value<std::vector<std::string>>(), "Path to column(s) with dictionary disabled");
  config.add_options()("enable-statistics-path", po::value<std::vector<std::string>>(), "Path to column(s) with statistics enabled");
  config.add_options()("disable-statistics-path", po::value<std::vector<std::string>>(), "Path to column(s) with statistics disabled");

  config.add_options()("encoding",
                       po::value<parquet::Encoding::type>(&encoding)->multitoken(),
                       "Column encoding");
  config.add_options()("codec",
                       po::value<parquet::Compression::type>(&codec)->multitoken(),
                       "Compression codec");
  config.add_options()("parquet-version",
                     po::value<parquet::ParquetVersion::type>(&parquet_version)->multitoken(),
                     "Parquet file version");
  config.add_options()("created-by",
                       po::value<std::string>(&created_by)->default_value(created_by), "Name of application creating the output file");
  config.add_options()("row-group-size",
                       po::value<int64_t>(&row_group_size)->default_value(row_group_size), "Number of rows in row group");
  config.add_options()("write-batch-size",
                       po::value<int64_t>(&write_batch_size)->default_value(write_batch_size), "Write batch size");
  config.add_options()("dictionary-pagesize",
                       po::value<int64_t>(&dictionary_pagesize_limit)->default_value(dictionary_pagesize_limit),
                       "Dictionary page size");
  config.add_options()("pagesize", po::value<int64_t>(&pagesize)->default_value(pagesize), "Page size");

  // Arrow options

  config.add_options()("enable-deprecated-int96-timestamps",
                       po::value<bool>(&enable_deprecated_int96_timestamps)->default_value(enable_deprecated_int96_timestamps)->implicit_value(false),
                       "Enable enable deprecated int96 timestamps");
  config.add_options()("coerce-timestamps",
                       po::value<::arrow::TimeUnit::type>(&timeunit)->multitoken(),
                       "Time unit for coercing timestamps");

  po::positional_options_description positionalOptions;
  positionalOptions.add("input-file", -1);

  po::options_description desc_all("All options");
  desc_all.add(desc).add(config);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv).options(desc_all).positional(positionalOptions).run(), vm);
    po::notify(vm);

    if (vm.count("config")) {
      std::ifstream settings_file(config_file);
      po::store(po::parse_config_file(settings_file, desc_all, true), vm);
      po::notify(vm);
      settings_file.close();
    }

    if (vm.count("help")) {
      std::cout << "Usage: FileConvert <input file(s)> [--config <config file>] [--output <output file>]"
      << std::endl
      <<           "[--version|-v] [--schema|-s] <schema file> [--metadata|-m] <metadata file>"
      << std::endl
      << std::endl;
      std::cout << desc
      << std::endl
      << std::endl;
      std::cout << config << std::endl;
      return 0;
    }

    if (vm.count("version")) {
      std::cout << "FileConvert version: " << MAPD_RELEASE << std::endl;
      return 0;
    }

    if (vm.count("input-file")) {
      std::cout << "Input files are: "
      << std::endl;

      std::vector<std::string> temp = vm["input-file"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        input_files.push_back(temp[i]);
        std::cout << temp[i] << std::endl;
      }
    }

    if (vm.count("output")) {
      // output file extension
      output_ext = boost::algorithm::to_lower_copy(boost::filesystem::extension(output_filename));
    }

    if (vm.count("schema")) {
      std::string key;
      std::string line;
      std::vector<std::string> fields;
      std::string columnName;
      std::vector<std::string> metadata_keys;
      std::vector<std::string> metadata_values;

      auto file_path = boost::filesystem::path(schema_filename);
      if (!file_path.is_absolute()) {
        file_path = boost::filesystem::canonical(file_path);
        schema_filename = file_path.string();
      }

      if (!boost::filesystem::exists(file_path)) {
        throw std::runtime_error("Schema file does not exist: " + file_path.string());
      }

      std::ifstream schema_file(schema_filename);

      // type id
      key   = "mapd.SQLTypeInfo.type";
      metadata_keys.push_back(key);

      // element type of arrays
      key   = "mapd.SQLTypeInfo.subtype";
      metadata_keys.push_back(key);

      // VARCHAR/CHAR length or NUMERIC/DECIMAL precision
      key   = "mapd.SQLTypeInfo.dimension";
      metadata_keys.push_back(key);

      // NUMERIC/DECIMAL scale
      key   = "mapd.SQLTypeInfo.scale";
      metadata_keys.push_back(key);

      // nullable?
      key   = "mapd.SQLTypeInfo.notnull";
      metadata_keys.push_back(key);

      // compression scheme
      key   = "mapd.SQLTypeInfo.compression";
      metadata_keys.push_back(key);

      // compression parameter when applicable for certain schemes
      key   = "mapd.SQLTypeInfo.comp_param";
      metadata_keys.push_back(key);

      // size of the type in bytes.  -1 for variable size
      key   = "mapd.SQLTypeInfo.size";
      metadata_keys.push_back(key);

      // get header line
      std::getline(schema_file, line);

      line = trim(line);

      if (line != "FileConvert 1.0.0") {
        throw std::runtime_error("Schema file format is unknown: " + file_path.string());
      }

      // empty line
      std::getline(schema_file, line);

      while ( std::getline(schema_file, line) ) {
        std::vector<std::string> fields;
        int metadata_set   = 0;

        line = trim(line);

        if (line.empty())
          break;

        split(fields, line, ",");

        if (fields.size() != (metadata_keys.size()+1)) {
          throw std::runtime_error("Schema file format is unknown: " + file_path.string());
        }

        // Save column name
        columnName = sanitize_name(fields[0]);

        // Remove column name
        fields.erase(fields.begin());

        ColumnDescriptor* cd = new ColumnDescriptor();

        // initialize ColumnDescriptor

        cd->tableId = 0;
        cd->columnId = 0;
        cd->columnName = columnName;
        cd->sourceName = columnName;

        cd->columnType.set_type(kSQLTYPE_LAST);
        cd->columnType.set_subtype(kSQLTYPE_LAST);
        cd->columnType.set_dimension(0);
        cd->columnType.set_precision(0);
        cd->columnType.set_scale(0);
        cd->columnType.set_notnull(false);
        cd->columnType.set_size(-1);
        // cd->columnType.set_fixed_size();
        cd->columnType.set_compression(kENCODING_NONE);
        cd->columnType.set_comp_param(0);

        // cd->chunks;
        cd->isSystemCol  = false;
        cd->isVirtualCol = false;
        // cd->virtualExpr;

        // cd->columnType;
        for (int i = 0; i < metadata_keys.size(); i++) {
          if (Importer_NS::ArrowKeyValueMetadataToSQLTypeInfo(cd->columnType,
                                                              metadata_keys[i],
                                                              fields[i])) {
            metadata_set++;
          }
        }

        // Add column to schema
        schema_col_descs.push_back(cd);
      }

      schema_file.close();
    }

    if (vm.count("metadata")) {
      std::string line;
      std::vector<std::string> metadata_keys;
      std::vector<std::string> metadata_values;

      auto file_path = boost::filesystem::path(metadata_filename);
      if (!file_path.is_absolute()) {
        file_path = boost::filesystem::canonical(file_path);
        metadata_filename = file_path.string();
      }

      if (!boost::filesystem::exists(file_path)) {
        throw std::runtime_error("Schema file does not exist: " + file_path.string());
      }

      std::ifstream metadata_file(metadata_filename);

      while (std::getline(metadata_file, line)) {
        line = trim(line);

        if (line.empty())
          break;

        std::string delimiters = " ,";
        size_t current = 0;
        size_t next    = -1;

        next = line.find_first_of(delimiters, current);

        metadata_keys.push_back(trim(line.substr(current, next - current)));
        metadata_values.push_back(trim(line.substr(next+1, std::string::npos)));
      }

      metadata_file.close();

      // Create metadata
      metadata.reset(new ::arrow::KeyValueMetadata(metadata_keys, metadata_values));
    }

    if (vm.count("codec")) {
      parquet_isset = true;
      builder.compression(codec);
    }

    if (vm.count("encoding")) {
      parquet_isset = true;
      builder.encoding(encoding);
    }

    if (vm.count("parquet-version")) {
      parquet_isset = true;
      builder.version(parquet_version);
    }

    if (vm.count("created-by")) {
      parquet_isset = true;
      builder.created_by(created_by);
    }

    if (vm.count("dictionary-pagesize")) {
      parquet_isset = true;
      builder.dictionary_pagesize_limit(dictionary_pagesize_limit);
    }

    if (vm.count("write-batch-size")) {
      parquet_isset = true;
      builder.write_batch_size(write_batch_size);
    }

    if (vm.count("pagesize")) {
      parquet_isset = true;
      builder.data_pagesize(pagesize);
    }

    if (vm.count("enable-dictionary")) {
      parquet_isset = true;
      builder.enable_dictionary();
    } else {
      builder.disable_dictionary();
    }

    if (vm.count("enable-statistics")) {
      parquet_isset = true;
      builder.enable_statistics();
    } else {
      builder.disable_statistics();
    }

    if (vm.count("enable-dictionary-path")) {
      std::vector<std::string> temp = vm["enable-dictionary-path"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        enable_dictionary_paths.push_back(temp[i]);

        builder.enable_dictionary(temp[i]);
      }

      parquet_isset = true;
    }

    if (vm.count("disable-dictionary-path")) {
      std::vector<std::string> temp = vm["disable-dictionary-path"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        disable_dictionary_paths.push_back(temp[i]);

        builder.disable_dictionary(temp[i]);
      }

      parquet_isset = true;
    }

    if (vm.count("enable-statistics-path")) {
      std::vector<std::string> temp = vm["enable-statistics-path"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        enable_statistics_paths.push_back(temp[i]);

        builder.enable_statistics(temp[i]);
      }

      parquet_isset = true;
    }

    if (vm.count("disable-statistics-path")) {
      std::vector<std::string> temp = vm["disable-statistics-path"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        disable_statistics_paths.push_back(temp[i]);

        builder.disable_statistics(temp[i]);
      }

      parquet_isset = true;
    }

    if (parquet_isset == true) {
      properties = builder.build();
    }

    // Schema properties

    if (vm.count("notnull")) {
      std::vector<std::string> temp = vm["notnull"].as<std::vector<std::string>>();

      for (int i = 0; i < temp.size(); i++) {
        notnull_columns.push_back(temp[i]);
      }
    }

    // Arrow properties

    if (vm.count("enable-deprecated-int96-timestamps")) {
      arrow_isset = true;
      arrow_builder.enable_deprecated_int96_timestamps();
    } else {
      arrow_builder.disable_deprecated_int96_timestamps();
    }

    if (vm.count("coerce-timestamps")) {
      arrow_isset = true;
      arrow_builder.coerce_timestamps(timeunit);
    }

    if (arrow_isset == true) {
      arrow_properties = arrow_builder.build();
    }

    // Output copy parameters

    output_copy_params.delimiter   = cp_delimiter;
    output_copy_params.null_str    = cp_null_str;
    output_copy_params.has_header  = cp_has_header;
    output_copy_params.quoted      = cp_quoted;
    output_copy_params.quote       = cp_quote;
    output_copy_params.escape      = cp_escape;
    output_copy_params.line_delim  = cp_line_delim;
    output_copy_params.array_delim = cp_array_delim;
    output_copy_params.array_begin = cp_array_begin;
    output_copy_params.array_end   = cp_array_end;
    output_copy_params.threads     = cp_threads;
    output_copy_params.max_reject  = cp_max_reject;
    output_copy_params.table_type  = cp_table_type;
    output_copy_params.threads     = 1;
  } catch (boost::program_options::error& e) {
    std::cerr << "Usage Error: " << e.what() << std::endl;
    return 1;
  }

  // File extensions:
  //
  // .csv
  // .tsv
  // .parquet
  //
  // .shp
  // .shx
  // .dbf

  // File conversions:
  //
  // .csv       .parquet
  // .csv       .tsv
  // .csv       .csv
  // .parquet   .csv
  // .parquet   .tsv
  // .parquet   .parquet
  // .tsv       .csv
  // .tsv       .parquet
  // .tsv       .tsv

  // TODO(renesugar): Add support for GDAL writer? (.shp, .shx, .dbf)

  // NOTE: MapD writer cannot be tested until reintegrating with MapD

  // NOTE: Specify an output file in CSV format to be able to insert data
  //       into MapD using StreamInsert

  // Get file extension of output file

  // for each input file
  //   If writer has not been created
  //     Use schema of first input file if schema has not been specified
  //     Create writer for output file type
  //   Get file extension of current input file
  //   Create importer for current input file
  //   Write to output file type

  size_t rows_completed = 0;
  size_t rows_rejected = 0;
  size_t total_time = 0;
  bool load_truncated = false;

  for (std::string input_file : input_files) {
    std::list<const ColumnDescriptor *> cds;
    std::string input_ext;

    if (!print_schema) {
      std::cout << "Processing input file: " << input_file << std::endl;
    }

    auto file_path = boost::filesystem::path(input_file);
    if (!file_path.is_absolute()) {
      file_path = boost::filesystem::canonical(file_path);
      input_file = file_path.string();
    }

    if (!boost::filesystem::exists(file_path)) {
      throw std::runtime_error("File does not exist: " + file_path.string());
    }

    input_ext = boost::algorithm::to_lower_copy(boost::filesystem::extension(input_file));

    if ((input_ext == ".csv") ||
        (input_ext == ".tsv")) {
      cds = Importer_NS::getAllColumnMetadataForDelimited(input_file, copy_params);

      // Use one thread for import
      copy_params.threads = 1;
    } else if (input_ext == ".parquet") {
      const std::shared_ptr<::arrow::Schema> schema = Importer_NS::getArrowSchemaFromFile(input_file);
      copy_params.table_type = Importer_NS::TableType::ARROW;
      cds = Importer_NS::getAllColumnMetadataForArrowSchema(schema, false, false);
    } else if ((input_ext == ".shp") ||
              (input_ext == ".shx") ||
              (input_ext == ".dbf")) {
      copy_params.table_type = Importer_NS::TableType::POLYGON;
      check_geospatial_files(input_file);
      cds = Importer_NS::GDALImporter::gdalToColumnDescriptors(input_file);
    } else {
      throw std::runtime_error("Unknown import format: " + file_path.string());
    }

    // Update schema to indicate which columns cannot contain null values

    std::list<const ColumnDescriptor *> copy_cds;
    for (auto cd : cds) {
      ColumnDescriptor* curr_cd = new ColumnDescriptor();

      curr_cd->tableId    = cd->tableId;
      curr_cd->columnId   = cd->columnId;
      curr_cd->columnName = cd->columnName;
      curr_cd->sourceName = cd->sourceName;

      curr_cd->columnType.set_type(cd->columnType.get_type());
      curr_cd->columnType.set_subtype(cd->columnType.get_subtype());
      curr_cd->columnType.set_dimension(cd->columnType.get_dimension());
      curr_cd->columnType.set_precision(cd->columnType.get_precision());
      curr_cd->columnType.set_scale(cd->columnType.get_scale());
      curr_cd->columnType.set_notnull(cd->columnType.get_notnull());
      curr_cd->columnType.set_size(cd->columnType.get_size());
      // curr_cd->columnType.set_fixed_size();
      curr_cd->columnType.set_compression(cd->columnType.get_compression());
      curr_cd->columnType.set_comp_param(cd->columnType.get_comp_param());

      // curr_cd->chunks;
      curr_cd->isSystemCol  = cd->isSystemCol;
      curr_cd->isVirtualCol = cd->isVirtualCol;
      // curr_cd->virtualExpr;

      bool is_notnull =
      (std::find(notnull_columns.begin(), notnull_columns.end(), cd->columnName)
       != notnull_columns.end());
      if ( is_notnull ) {
        curr_cd->columnType.set_notnull(true);
      } else {
        curr_cd->columnType.set_notnull(false);
      }

      copy_cds.push_back(curr_cd);
    }

    // Update the column descriptor list with the new values

    cds.clear();
    size_t col_idx = 0;
    for (auto cd : copy_cds) {
      cds.push_back(cd);
      col_idx++;
    }
    copy_cds.clear();

    if (print_schema) {
      // Identify the input file
      std::cout << std::endl;
      std::cout << input_file << ":" << std::endl;
      std::cout << std::endl;

      // Identify format of schema file
      std::cout << "FileConvert 1.0.0" << std::endl;
      std::cout << std::endl;

      for (const auto cd : cds) {
        std::vector<std::string> metadata_keys;
        std::vector<std::string> metadata_values;
        bool first = true;

        std::cout << cd->columnName << ",";

        Importer_NS::SQLTypeInfoToArrowKeyValueMetadata(cd->columnType, metadata_keys, metadata_values);

        for (const auto value : metadata_values) {
          if (first == false) {
            std::cout << ",";
          }

          std::cout << value;

          first = false;
        }

        std::cout << std::endl;
      }

      // next input file
      continue;
    }

    // Initialize output file writer using schema from the first input file
    // or the schema file specified on the command line.
    //
    // Use the --print-schema option to create a schema file for each input
    // file and edit to create the output schema file.
    //

    // NOTE: The same writer is used to combine multiple input files
    //       into one output file.

    if (writer_isset == false) {
      writer_isset = true;

      // If the output schema has not been specified, use the schema from
      // the first input file.

      if (schema_col_descs.size() == 0) {
        for (const auto cd : cds) {
          schema_col_descs.push_back(cd);
        }
      }

      // Create writer

      if ((output_ext == ".csv") || (output_ext == ".tsv")) {
        std::unique_ptr<Importer_NS::DelimitedWriter> delimitedWriter(
          new Importer_NS::DelimitedWriter(output_filename,
                                         output_copy_params,
                                         schema_col_descs));
        providedLoader = std::make_shared<Importer_NS::Loader>(
                            reinterpret_cast<Importer_NS::Writer*>(delimitedWriter.release()));
      } else if (output_ext == ".parquet") {
        std::unique_ptr<Importer_NS::ParquetWriter> parquetWriter(
          new Importer_NS::ParquetWriter(output_filename, schema_col_descs));

        parquetWriter->set_row_group_size(row_group_size);
        parquetWriter->set_metadata(metadata);
        parquetWriter->set_writer_properties(properties);
        parquetWriter->set_arrow_properties(arrow_properties);

        providedLoader = std::make_shared<Importer_NS::Loader>(reinterpret_cast<Importer_NS::Writer*>(parquetWriter.release()));
      } else {
        throw std::runtime_error("Unknown import format: " + output_filename);
      }

      // MapD
      //
      // const Catalog_Namespace::Catalog& c,
      // const TableDescriptor* t,
      // providedLoader = std::make_shared<Loader>(new MapDWriter(c, t))

      // Importer factory

      // TODO(renesugar): When reintegrating with MapD, use the MapDWriter()

      auto importer_factory = [](const std::shared_ptr<Importer_NS::Loader> providedLoader,
                                 const std::list<const ColumnDescriptor*>& cds,
                                 const std::string& file,
                                 const Importer_NS::CopyParams& copy_params) {
        std::string input_file;
        std::string input_ext;
        std::unique_ptr<Importer_NS::Importer> importer;

        auto file_path = boost::filesystem::path(file);
        if (!file_path.is_absolute()) {
          file_path = boost::filesystem::canonical(file_path);
        }
        input_file = file_path.string();

        if (!boost::filesystem::exists(file_path)) {
          throw std::runtime_error("File does not exist: " + file_path.string());
        }

        input_ext = boost::algorithm::to_lower_copy(boost::filesystem::extension(input_file));

        if ((input_ext == ".csv") ||
            (input_ext == ".tsv")) {
          auto p = boost::make_unique<Importer_NS::DelimitedImporter>(providedLoader.get(), cds, file, copy_params);
          importer.reset(reinterpret_cast<Importer_NS::Importer*>(p.release()));
          return importer;
        } else if (input_ext == ".parquet") {
          auto p = boost::make_unique<Importer_NS::ParquetImporter>(providedLoader.get(), cds, file, copy_params);
          importer.reset(reinterpret_cast<Importer_NS::Importer*>(p.release()));
          return importer;
        } else if ((input_ext == ".shp") ||
                  (input_ext == ".shx") ||
                  (input_ext == ".dbf")) {
          auto p = boost::make_unique<Importer_NS::GDALImporter>(providedLoader.get(), cds, file, copy_params);
          importer.reset(reinterpret_cast<Importer_NS::Importer*>(p.release()));
          return importer;
        } else {
          throw std::runtime_error("Unknown import format: " + file_path.string());
        }
      };

      // Column name to source map

      // TODO(renesugar): In MapD, column name and source name appear to be the same.
      //                  Under what conditions are they different?

      std::map<std::string, std::string> colname_to_src;
      for (const auto cd : cds) {
        colname_to_src[cd->columnName] = cd->sourceName;
      }

      // Create importer based upon file extension
      auto importer = importer_factory(providedLoader, cds, input_file, copy_params);

      auto ms = measure<>::execution([&]() {
        auto res = importer->import(colname_to_src);
        rows_completed += res.rows_completed;
        rows_rejected += res.rows_rejected;
        load_truncated = res.load_truncated;
      });
      total_time += ms;
      if (load_truncated || rows_rejected > copy_params.max_reject) {
        LOG(ERROR) << "COPY exited early due to reject records count during multi file processing ";
        // if we have crossed the truncated load threshold
        load_truncated = true;
        break;
      }
    }
  }

  // Close writer to finish writing data

  if (providedLoader != nullptr) {
    if ((output_ext == ".csv") || (output_ext == ".tsv")) {
      providedLoader->get_writer()->close();
    } else if (output_ext == ".parquet") {
      providedLoader->get_writer()->close();
    }
  }

  if (!print_schema) {
    std::string tr;
    if (!load_truncated) {
      tr = std::string("Loaded: " + std::to_string(rows_completed) + " recs, Rejected: " + std::to_string(rows_rejected) +
                       " recs in " + std::to_string(static_cast<double>(total_time) / 1000.0) + " secs");
    } else {
      tr = std::string("Loader truncated due to reject count.  Processed : " + std::to_string(rows_completed) +
                       " recs, Rejected: " + std::to_string(rows_rejected) + " recs in " +
                       std::to_string(static_cast<double>(total_time) / 1000.0) + " secs");
    }

    return_message.reset(new std::string(tr));
    LOG(INFO) << tr;
  }

  return 0;
}
