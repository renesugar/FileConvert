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
 * @file GDALImporter.h
 * @author Wei Hong < wei@mapd.com>
 * @brief Importer class for table import from file
 */
#ifndef _GDALIMPORTER_H_
#define _GDALIMPORTER_H_

#include <boost/noncopyable.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>
#include <poly2tri/poly2tri.h>
#include <ogrsf_frmts.h>
#include <gdal.h>

#include <utility>
#include <vector>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>

#include "../Shared/fixautotools.h"
#include "../Shared/ShapeDrawData.h"
#include "../Shared/checked_alloc.h"

#include "../Import/Importer.h"

namespace Importer_NS {

struct PolyData2d {
  std::vector<double> coords;
  std::vector<unsigned int> triangulation_indices;
  std::vector<Rendering::GL::Resources::IndirectDrawVertexData> lineDrawInfo;
  std::vector<Rendering::GL::Resources::IndirectDrawIndexData> polyDrawInfo;

  explicit PolyData2d(unsigned int startVert = 0, unsigned int startIdx = 0)
  : _ended(true), _startVert(startVert), _startIdx(startIdx), _startTriIdx(0) {}
  ~PolyData2d() {}

  size_t numVerts() const { return coords.size() / 2; }
  size_t numLineLoops() const { return lineDrawInfo.size(); }
  size_t numTris() const {
    CHECK_EQ(triangulation_indices.size() % 3, 0);
    return triangulation_indices.size() / 3;
  }
  size_t numIndices() const { return triangulation_indices.size(); }

  unsigned int startVert() const { return _startVert; }
  unsigned int startIdx() const { return _startIdx; }

  void beginPoly() {
    assert(_ended);
    _ended = false;
    _startTriIdx = numVerts() - lineDrawInfo.back().count;

    if (!polyDrawInfo.size()) {
      // polyDrawInfo.emplace_back(0, _startIdx + triangulation_indices.size(), lineDrawInfo.back().firstIndex);
      polyDrawInfo.emplace_back(0, _startIdx, _startVert);
    }
  }

  void endPoly() {
    assert(!_ended);
    _ended = true;
  }

  void beginLine() {
    assert(_ended);
    _ended = false;

    lineDrawInfo.emplace_back(0, _startVert + numVerts());
  }

  void addLinePoint(const std::shared_ptr<p2t::Point>& vertPtr) {
    _addPoint(vertPtr->x, vertPtr->y);
    lineDrawInfo.back().count++;
  }

  bool endLine() {
    bool rtn = false;
    auto& lineDrawItem = lineDrawInfo.back();
    size_t idx0 = (lineDrawItem.firstIndex - _startVert) * 2;
    size_t idx1 = idx0 + (lineDrawItem.count - 1) * 2;
    if (coords[idx0] == coords[idx1] && coords[idx0 + 1] == coords[idx1 + 1]) {
      coords.pop_back();
      coords.pop_back();
      lineDrawItem.count--;
      rtn = true;
    }

    // repeat the first 3 vertices to fully create the "loop"
    // since it will be drawn using the GL_LINE_STRIP_ADJACENCY
    // primitive type
    int num = lineDrawItem.count;
    for (int i = 0; i < 3; ++i) {
      int idx = (idx0 + ((i % num) * 2));
      coords.push_back(coords[idx]);
      coords.push_back(coords[idx + 1]);
    }
    lineDrawItem.count += 3;

    // add an empty coord as a separator
    // coords.push_back(-10000000.0);
    // coords.push_back(-10000000.0);

    _ended = true;
    return rtn;
  }

  void addTriangle(unsigned int idx0, unsigned int idx1, unsigned int idx2) {
    // triangulation_indices.push_back(idx0);
    // triangulation_indices.push_back(idx1);
    // triangulation_indices.push_back(idx2);

    triangulation_indices.push_back(_startTriIdx + idx0);
    triangulation_indices.push_back(_startTriIdx + idx1);
    triangulation_indices.push_back(_startTriIdx + idx2);

    polyDrawInfo.back().count += 3;
  }

private:
  bool _ended;
  unsigned int _startVert;
  unsigned int _startIdx;
  unsigned int _startTriIdx;

  void _addPoint(double x, double y) {
    coords.push_back(x);
    coords.push_back(y);
  }
};

class GDALImporter : Importer {
public:
  GDALImporter(Loader* providedLoader,
               const std::list<const ColumnDescriptor*>& cds,
               const std::string& f,
               const CopyParams& p);
  ImportStatus import(std::map<std::string, std::string>& colname_to_src);
  static std::list<const ColumnDescriptor*> gdalToColumnDescriptors(const std::string& fileName);
  static void readMetadataSampleGDAL(const std::string& fileName,
                                     std::map<std::string, std::vector<std::string>>& metadata,
                                     int rowLimit);

private:
  void readVerticesFromGDAL(const std::string& fileName,
                            std::vector<PolyData2d>& polys,
                            std::pair<std::map<std::string, size_t>,
                            std::vector<std::vector<std::string>>>& metadata);
  void readVerticesFromGDALGeometryZ(const std::string& fileName,
                                     OGRPolygon* poPolygon, PolyData2d& poly,
                                     bool hasZ);
  void initGDAL();

  ImportStatus import_status;
};

};  // namespace Importer_NS

#endif  // _GDALIMPORTER_H_
