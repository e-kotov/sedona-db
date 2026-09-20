// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>
#include "geom_types.hpp"

#ifdef __OBJC__
#import <Metal/Metal.h>
#else
typedef void* id;
#endif

class MetalSpatialRefiner {
 public:
#ifdef ENABLE_TEST_INTERNALS
  MetalSpatialRefiner(id device = nullptr, int bound_mode = 0);
#else
  MetalSpatialRefiner(id device = nullptr);
#endif
  ~MetalSpatialRefiner();

  void clear();

  void push_polygons(const PolygonRecord* polys, uint32_t poly_count,
                     const PartRecord* parts, uint32_t part_count,
                     const RingRecord* rings, uint32_t ring_count,
                     const Point2D* vertices, uint32_t vertex_count);

  void finish_building();

  void refine(const DecomposedPoint* points, uint32_t point_count,
              const uint32_t* candidate_build_indices,
              const uint32_t* candidate_probe_indices, uint32_t candidate_count,
              uint8_t* out_states);

  // Ring edge-index mode: 0 = off (linear scan), 1 = y-slabs only (+x ray),
  // 2 = y-slabs and x-slabs (both rays). Must be set before finish_building().
  // Defaults to 2, overridable with SEDONA_METAL_REFINE_INDEX=off|x|xy.
  void set_index_mode(int mode);
  // Test hook: scales the per-polygon pad (values < 1 force the kernel pad fallback).
  void set_index_pad_scale(float scale) { index_pad_scale_ = scale; }

  // Counters: [0] rings indexed on y, [1] rings indexed on x, [2] index entries,
  // [3] index bytes, [4] index build microseconds, [5] pairs refined,
  // [6] pairs using the indexed +x path, [7] pairs where the +y retry ran,
  // [8] pairs where the +y retry was indexed, [9] pairs with a pad fallback,
  // [10] vertices in indexed rings (y), [11] total vertices.
  static constexpr uint32_t kNumStats = 12;
  void get_stats(uint64_t* out, uint32_t n) const;

  const char* get_last_error() const;
  const char* get_device_name() const;
  uint64_t get_memory_usage() const;
  void set_last_error(const std::string& err);

 private:
  void set_error(const std::string& err);
  void build_ring_index();

#ifdef __OBJC__
  id<MTLDevice> device_;
  id<MTLCommandQueue> command_queue_;
  id<MTLComputePipelineState> pipeline_state_;

  id<MTLBuffer> buf_polygons_;
  id<MTLBuffer> buf_parts_;
  id<MTLBuffer> buf_rings_;
  id<MTLBuffer> buf_vertices_;
  id<MTLBuffer> buf_ring_index_;
  id<MTLBuffer> buf_slab_offsets_;
  id<MTLBuffer> buf_edge_ids_;
#else
  void* device_;
  void* command_queue_;
  void* pipeline_state_;

  void* buf_polygons_;
  void* buf_parts_;
  void* buf_rings_;
  void* buf_vertices_;
  void* buf_ring_index_;
  void* buf_slab_offsets_;
  void* buf_edge_ids_;
#endif

  std::vector<PolygonRecord> host_polygons_;
  std::vector<PartRecord> host_parts_;
  std::vector<RingRecord> host_rings_;
  std::vector<Point2D> host_vertices_;

  int index_mode_;
  float index_pad_scale_ = 1.0f;
  bool print_stats_;
  mutable std::mutex stats_mutex_;
  uint64_t stats_[kNumStats] = {};

  bool is_built_;
  uint32_t num_polygons_;
  uint64_t allocated_bytes_;
  std::string device_name_;
  mutable std::mutex error_mutex_;
  mutable std::string last_error_;
};
