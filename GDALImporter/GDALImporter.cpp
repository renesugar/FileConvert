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
 * @file Importer.cpp
 * @author Wei Hong <wei@mapd.com>
 * @brief Functions for Importer class
 */

#include "GDALImporter.h"

#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <ogrsf_frmts.h>
#include <gdal.h>

#include <string>
#include <algorithm>
#include <map>
#include <memory>
#include <utility>
#include <set>
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
#include "../Shared/mapdpath.h"
#include "../Shared/measure.h"
#include "../Shared/geosupport.h"

#include "gen-cpp/MapD.h"

namespace Importer_NS {
  // providedLoader = new Loader(new MapDWriter(c, t))
  GDALImporter::GDALImporter(Loader* providedLoader,
                             const std::list<const ColumnDescriptor*>& cds,
                             const std::string& f,
                             const CopyParams& p)
  : Importer(providedLoader, cds, f, p) {}

  void GDALErrorHandler(CPLErr eErrClass, int err_no, const char* msg) {
    throw std::runtime_error("GDAL error: " + std::string(msg));
  }

  void GDALImporter::readVerticesFromGDALGeometryZ(const std::string& fileName,
                                                   OGRPolygon* poPolygon,
                                                   PolyData2d& poly,
                                                   bool) {
    std::vector<std::shared_ptr<p2t::Point>> vertexShPtrs;
    std::vector<p2t::Point*> vertexPtrs;
    std::vector<int> tris;
    std::unordered_map<p2t::Point*, int> pointIndices;
    OGRPoint ptTemp;

    OGRLinearRing* poExteriorRing = poPolygon->getExteriorRing();
    if (!poExteriorRing->isClockwise()) {
      poExteriorRing->reverseWindingOrder();
    }
    if (!poExteriorRing->isClockwise()) {
      return;
    }
    poExteriorRing->closeRings();
    int nExtVerts = poExteriorRing->getNumPoints();
    std::set<std::pair<double, double>> dedupe;

    poly.beginLine();
    for (int k = 0; k < nExtVerts - 1; k++) {
      poExteriorRing->getPoint(k, &ptTemp);
      auto xy = std::make_pair(ptTemp.getX(), ptTemp.getY());
      if (k > 0 && vertexPtrs.back()->x == xy.first && vertexPtrs.back()->y == xy.second) {
        continue;
      }
      auto a = dedupe.insert(std::make_pair(xy.first, xy.second));
      if (!a.second) {
        throw std::runtime_error("invalid geometry: duplicate vertex found");
      }
      vertexShPtrs.emplace_back(new p2t::Point(xy.first, xy.second));
      poly.addLinePoint(vertexShPtrs.back());
      vertexPtrs.push_back(vertexShPtrs.back().get());
      pointIndices.insert({vertexShPtrs.back().get(), vertexPtrs.size() - 1});
    }
    poly.endLine();

    p2t::CDT triangulator(vertexPtrs);

    triangulator.Triangulate();

    int idx0, idx1, idx2;

    std::unordered_map<p2t::Point*, int>::iterator itr;

    poly.beginPoly();
    for (p2t::Triangle* tri : triangulator.GetTriangles()) {
      itr = pointIndices.find(tri->GetPoint(0));
      if (itr == pointIndices.end()) {
        throw std::runtime_error("failed to triangulate polygon");
      }
      idx0 = itr->second;

      itr = pointIndices.find(tri->GetPoint(1));
      if (itr == pointIndices.end()) {
        throw std::runtime_error("failed to triangulate polygon");
      }
      idx1 = itr->second;

      itr = pointIndices.find(tri->GetPoint(2));
      if (itr == pointIndices.end()) {
        throw std::runtime_error("failed to triangulate polygon");
      }
      idx2 = itr->second;

      poly.addTriangle(idx0, idx1, idx2);
    }
    poly.endPoly();
  }

  void initGDAL() {
    static bool gdal_initialized = false;
    if (!gdal_initialized) {
      char * gdal_data_path = nullptr;

      gdal_data_path = getenv("GDAL_DATA");
      if (gdal_data_path == nullptr) {
        // FIXME(andrewseidl): investigate if CPLPushFinderLocation can be public
        setenv("GDAL_DATA", std::string(mapd_root_abs_path() + "/ThirdParty/gdal-data").c_str(), true);
      }
      GDALAllRegister();
      OGRRegisterAll();
      gdal_initialized = true;
    }
  }

  static OGRDataSource* openGDALDataset(const std::string& fileName) {
    initGDAL();
    CPLSetErrorHandler(*GDALErrorHandler);
    OGRDataSource* poDS;
#if GDAL_VERSION_MAJOR == 1
    poDS = (OGRDataSource*)OGRSFDriverRegistrar::Open(fileName.c_str(), false);
#else
    poDS = (OGRDataSource*)GDALOpenEx(fileName.c_str(), GDAL_OF_VECTOR, nullptr, nullptr, nullptr);
#endif
    if (poDS == nullptr) {
      LOG(INFO) << "ogr error: " << CPLGetLastErrorMsg();
    }
    return poDS;
  }

  void GDALImporter::readMetadataSampleGDAL(const std::string& fileName,
                                            std::map<std::string, std::vector<std::string>>& metadata,
                                            int rowLimit) {
    auto poDS = openGDALDataset(fileName);
    if (poDS == nullptr) {
      throw std::runtime_error("Unable to open geo file " + fileName);
    }
    OGRLayer* poLayer;
    poLayer = poDS->GetLayer(0);
    if (poLayer == nullptr) {
      throw std::runtime_error("No layers found in " + fileName);
    }
    OGRFeatureDefn* poFDefn = poLayer->GetLayerDefn();

    // typeof GetFeatureCount() is different between GDAL 1.x (int32_t) and 2.x (int64_t)
    auto nFeats = poLayer->GetFeatureCount();
    size_t numFeatures =
    std::max(static_cast<decltype(nFeats)>(0), std::min(static_cast<decltype(nFeats)>(rowLimit), nFeats));
    for (auto iField = 0; iField < poFDefn->GetFieldCount(); iField++) {
      OGRFieldDefn* poFieldDefn = poFDefn->GetFieldDefn(iField);
      // FIXME(andrewseidl): change this to the faster one used by readVerticesFromGDAL
      metadata.emplace(poFieldDefn->GetNameRef(), std::vector<std::string>(numFeatures));
    }
    OGRFeature* poFeature;
    poLayer->ResetReading();
    size_t iFeature = 0;
    while ((poFeature = poLayer->GetNextFeature()) != nullptr && iFeature < numFeatures) {
      OGRGeometry* poGeometry;
      poGeometry = poFeature->GetGeometryRef();
      if (poGeometry != nullptr) {
        switch (wkbFlatten(poGeometry->getGeometryType())) {
          case wkbPolygon:
          case wkbMultiPolygon:
            break;
          default:
            throw std::runtime_error("Unsupported geometry type: " + std::string(poGeometry->getGeometryName()));
        }
        for (auto i : metadata) {
          auto iField = poFeature->GetFieldIndex(i.first.c_str());
          metadata[i.first].at(iFeature) = std::string(poFeature->GetFieldAsString(iField));
        }
        OGRFeature::DestroyFeature(poFeature);
      }
      iFeature++;
    }
    GDALClose(poDS);
  }

  void GDALImporter::readVerticesFromGDAL(
                                          const std::string& fileName,
                                          std::vector<PolyData2d>& polys,
                                          std::pair<std::map<std::string, size_t>, std::vector<std::vector<std::string>>>& metadata) {
    auto poDS = openGDALDataset(fileName);
    if (poDS == nullptr) {
      throw std::runtime_error("Unable to open geo file " + fileName);
    }
    OGRLayer* poLayer;
    poLayer = poDS->GetLayer(0);
    if (poLayer == nullptr) {
      throw std::runtime_error("No layers found in " + fileName);
    }
    OGRFeatureDefn* poFDefn = poLayer->GetLayerDefn();
    size_t numFeatures = poLayer->GetFeatureCount();
    size_t nFields = poFDefn->GetFieldCount();
    for (size_t iField = 0; iField < nFields; iField++) {
      OGRFieldDefn* poFieldDefn = poFDefn->GetFieldDefn(iField);
      metadata.first[poFieldDefn->GetNameRef()] = iField;
      metadata.second.push_back(std::vector<std::string>(numFeatures));
    }
    OGRFeature* poFeature;
    poLayer->ResetReading();
    auto poSR = new OGRSpatialReference();
    poSR->importFromEPSG(3857);

    // typeof GetFeatureCount() is different between GDAL 1.x (int32_t) and 2.x (int64_t)
    auto nFeats = poLayer->GetFeatureCount();
    decltype(nFeats) iFeature = 0;
    for (iFeature = 0; iFeature < nFeats; iFeature++) {
      poFeature = poLayer->GetNextFeature();
      if (poFeature == nullptr) {
        break;
      }
      try {
        OGRGeometry* poGeometry;
        poGeometry = poFeature->GetGeometryRef();
        if (poGeometry != nullptr) {
          poGeometry->transformTo(poSR);
          if (polys.size()) {
            polys.emplace_back(polys.back().startVert() + polys.back().numVerts(),
                               polys.back().startIdx() + polys.back().numIndices());
          } else {
            polys.emplace_back();
          }
          switch (wkbFlatten(poGeometry->getGeometryType())) {
            case wkbPolygon: {
              OGRPolygon* poPolygon = (OGRPolygon*)poGeometry;
              readVerticesFromGDALGeometryZ(fileName, poPolygon, polys.back(), false);
              break;
            }
            case wkbMultiPolygon: {
              OGRMultiPolygon* poMultiPolygon = (OGRMultiPolygon*)poGeometry;
              int NumberOfGeometries = poMultiPolygon->getNumGeometries();
              for (auto j = 0; j < NumberOfGeometries; j++) {
                OGRGeometry* poPolygonGeometry = poMultiPolygon->getGeometryRef(j);
                OGRPolygon* poPolygon = (OGRPolygon*)poPolygonGeometry;
                readVerticesFromGDALGeometryZ(fileName, poPolygon, polys.back(), false);
              }
              break;
            }
            case wkbPoint:
            default:
              throw std::runtime_error("Unsupported geometry type: " + std::string(poGeometry->getGeometryName()));
          }
          for (size_t iField = 0; iField < nFields; iField++) {
            metadata.second[iField][iFeature] = std::string(poFeature->GetFieldAsString(iField));
          }
          OGRFeature::DestroyFeature(poFeature);
        }
      } catch (const std::exception& e) {
        throw std::runtime_error(e.what() + std::string(" Feature: ") + std::to_string(iFeature + 1));
      }
    }
    GDALClose(poDS);
  }

  std::pair<SQLTypes, bool> ogr_to_type(const OGRFieldType& ogr_type) {
    switch (ogr_type) {
      case OFTInteger:
        return std::make_pair(kINT, false);
      case OFTIntegerList:
        return std::make_pair(kINT, true);
#if GDAL_VERSION_MAJOR > 1
      case OFTInteger64:
        return std::make_pair(kBIGINT, false);
      case OFTInteger64List:
        return std::make_pair(kBIGINT, true);
#endif
      case OFTReal:
        return std::make_pair(kDOUBLE, false);
      case OFTRealList:
        return std::make_pair(kDOUBLE, true);
      case OFTString:
        return std::make_pair(kTEXT, false);
      case OFTStringList:
        return std::make_pair(kTEXT, true);
      case OFTDate:
        return std::make_pair(kDATE, false);
      case OFTTime:
        return std::make_pair(kTIME, false);
      case OFTDateTime:
        return std::make_pair(kTIMESTAMP, false);
      case OFTBinary:
      default:
        break;
    }
    throw std::runtime_error("Unknown OGR field type: " + std::to_string(ogr_type));
  }

  std::list<const ColumnDescriptor*> GDALImporter::gdalToColumnDescriptors(const std::string& fileName) {
    std::list<const ColumnDescriptor*> cds;
    auto poDS = openGDALDataset(fileName);
    try {
      if (poDS == nullptr) {
        throw std::runtime_error("Unable to open geo file " + fileName + " : " + CPLGetLastErrorMsg());
      }
      OGRLayer* poLayer;
      poLayer = poDS->GetLayer(0);
      if (poLayer == nullptr) {
        throw std::runtime_error("No layers found in " + fileName);
      }
      OGRFeature* poFeature;
      poLayer->ResetReading();
      // TODO(andrewseidl): support multiple features
      if ((poFeature = poLayer->GetNextFeature()) != nullptr) {
        OGRFeatureDefn* poFDefn = poLayer->GetLayerDefn();
        int iField;
        for (iField = 0; iField < poFDefn->GetFieldCount(); iField++) {
          OGRFieldDefn* poFieldDefn = poFDefn->GetFieldDefn(iField);
          auto typePair = ogr_to_type(poFieldDefn->GetType());
          ColumnDescriptor* cd = new ColumnDescriptor();
          cd->columnName = poFieldDefn->GetNameRef();
          cd->sourceName = poFieldDefn->GetNameRef();
          SQLTypeInfo ti;
          if (typePair.second) {
            ti.set_type(kARRAY);
            ti.set_subtype(typePair.first);
          } else {
            ti.set_type(typePair.first);
          }
          if (typePair.first == kTEXT) {
            ti.set_compression(kENCODING_DICT);
            ti.set_comp_param(32);
          }
          ti.set_fixed_size();
          cd->columnType = ti;
          cds.push_back(cd);
        }
      }
    } catch (const std::exception& e) {
      GDALClose(poDS);
      throw;
    }
    GDALClose(poDS);

    return cds;
  }

  ImportStatus GDALImporter::import(std::map<std::string, std::string>& colname_to_src) {
    set_import_status(get_import_id(), import_status);
    std::vector<PolyData2d> polys;
    std::pair<std::map<std::string, size_t>, std::vector<std::vector<std::string>>> metadata;

    readVerticesFromGDAL(get_file_path().c_str(), polys, metadata);

    std::vector<std::unique_ptr<TypedImportBuffer>> import_buffers_vec;
    for (const auto cd : get_column_descs())
      import_buffers_vec.push_back(
                                   std::unique_ptr<TypedImportBuffer>(get_loader()->get_writer()->makeTypedImportBuffer(cd)));

    for (size_t ipoly = 0; ipoly < polys.size(); ++ipoly) {
      auto poly = polys[ipoly];
      try {
        auto icol = 0;
        for (auto cd : get_column_descs()) {
          if (cd->columnName == MAPD_GEO_PREFIX + "coords") {
            std::vector<TDatum> coords;
            for (auto coord : poly.coords) {
              TDatum td;
              td.val.real_val = coord;
              coords.push_back(td);
            }
            TDatum tdd;
            tdd.val.arr_val = coords;
            tdd.is_null = false;
            import_buffers_vec[icol++]->add_value(cd, tdd, false);
          } else if (cd->columnName == MAPD_GEO_PREFIX + "indices") {
            std::vector<TDatum> indices;
            for (auto tris : poly.triangulation_indices) {
              TDatum td;
              td.val.int_val = tris;
              indices.push_back(td);
            }
            TDatum tdd;
            tdd.val.arr_val = indices;
            tdd.is_null = false;
            import_buffers_vec[icol++]->add_value(cd, tdd, false);
          } else if (cd->columnName == MAPD_GEO_PREFIX + "linedrawinfo") {
            std::vector<TDatum> ldis;
            ldis.resize(4 * poly.lineDrawInfo.size());
            size_t ildi = 0;
            for (auto ldi : poly.lineDrawInfo) {
              ldis[ildi++].val.int_val = ldi.count;
              ldis[ildi++].val.int_val = ldi.instanceCount;
              ldis[ildi++].val.int_val = ldi.firstIndex;
              ldis[ildi++].val.int_val = ldi.baseInstance;
            }
            TDatum tdd;
            tdd.val.arr_val = ldis;
            tdd.is_null = false;
            import_buffers_vec[icol++]->add_value(cd, tdd, false);
          } else if (cd->columnName == MAPD_GEO_PREFIX + "polydrawinfo") {
            std::vector<TDatum> pdis;
            pdis.resize(5 * poly.polyDrawInfo.size());
            size_t ipdi = 0;
            for (auto pdi : poly.polyDrawInfo) {
              pdis[ipdi++].val.int_val = pdi.count;
              pdis[ipdi++].val.int_val = pdi.instanceCount;
              pdis[ipdi++].val.int_val = pdi.firstIndex;
              pdis[ipdi++].val.int_val = pdi.baseVertex;
              pdis[ipdi++].val.int_val = pdi.baseInstance;
            }
            TDatum tdd;
            tdd.val.arr_val = pdis;
            tdd.is_null = false;
            import_buffers_vec[icol++]->add_value(cd, tdd, false);
          } else {
            auto ifield = metadata.first.at(colname_to_src[cd->columnName]);
            auto str = metadata.second.at(ifield).at(ipoly);
            import_buffers_vec[icol++]->add_value(cd, str, false, get_copy_params());
          }
        }
      } catch (const std::exception& e) {
        LOG(WARNING) << "importGDAL exception thrown: " << e.what() << ". Row discarded.";
      }
    }

    try {
      get_loader()->load(import_buffers_vec, polys.size());
      return import_status;
    } catch (const std::exception& e) {
      LOG(WARNING) << e.what();
    }

    return import_status;
  }

}  // namespace Importer_NS
