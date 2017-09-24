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

#ifndef SHARED_SHAPEDRAWDATA_H_
#define SHARED_SHAPEDRAWDATA_H_

namespace Rendering {
namespace GL {
namespace Resources {

struct IndirectDrawVertexData {
  unsigned int count;
  unsigned int instanceCount;
  unsigned int firstIndex;
  unsigned int baseInstance;

  IndirectDrawVertexData() : count(0), instanceCount(0), firstIndex(0), baseInstance(0) {}

  IndirectDrawVertexData(unsigned int count,
                         unsigned int firstIndex = 0,
                         unsigned int instanceCount = 1,
                         unsigned int baseInstance = 0)
      : count(count), instanceCount(instanceCount), firstIndex(firstIndex), baseInstance(baseInstance) {}
};

struct IndirectDrawIndexData {
  unsigned int count;
  unsigned int instanceCount;
  unsigned int firstIndex;
  unsigned int baseVertex;
  unsigned int baseInstance;

  IndirectDrawIndexData() : count(0), instanceCount(0), firstIndex(0), baseVertex(0), baseInstance(0) {}

  IndirectDrawIndexData(unsigned int count,
                        unsigned int firstIndex = 0,
                        unsigned int baseVertex = 0,
                        unsigned int instanceCount = 1,
                        unsigned int baseInstance = 0)
      : count(count),
        instanceCount(instanceCount),
        firstIndex(firstIndex),
        baseVertex(baseVertex),
        baseInstance(baseInstance) {}
};

}  // namespace GL
}  // namespace Resources
}  // namespace Rendering

#endif  // SHARED_SHAPEDRAWDATA_H_
