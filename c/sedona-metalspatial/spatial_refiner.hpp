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

// Configuration of the ray-traced edge index used by the refine kernel for large rings.
// Must match SedonaMetalRtConfig in sedona_metalspatial_c.h.
struct RtConfig {
  uint32_t enabled;            // 0: every ring takes the linear scan
  uint32_t min_ring_vertices;  // rings with fewer vertices take the linear scan
  uint32_t segs_per_box;       // consecutive ring segments grouped per bounding box
  uint32_t slot_base;      // test only: first z slot, to exercise the slot limit guard
  uint32_t collect_stats;  // compile the kernel with diagnostic counters

  // Defaults, overridable through SEDONA_METAL_RT_* environment variables.
  static RtConfig from_env();
};

// Number of values written by MetalSpatialRefiner::get_rt_info().
constexpr uint32_t kRtInfoLen = 16;

class MetalSpatialRefiner {
 public:
#ifdef ENABLE_TEST_INTERNALS
  MetalSpatialRefiner(id device = nullptr, int bound_mode = 0,
                      const RtConfig* rt_config = nullptr);
#else
  MetalSpatialRefiner(id device = nullptr, const RtConfig* rt_config = nullptr);
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

  const char* get_last_error() const;
  const char* get_device_name() const;
  uint64_t get_memory_usage() const;
  // Writes kRtInfoLen values: [0] indexed rings, [1] boxes, [2] acceleration structure
  // bytes, [3] build scratch bytes, [4] GPU build microseconds, [5] host box preparation
  // microseconds, [6] rings skipped by the slot limit, [7] rings skipped by numeric
  // guards, [8..15] kernel counters RT_STAT_* (only with collect_stats).
  void get_rt_info(uint64_t* out) const;
  void set_last_error(const std::string& err);

 private:
  void set_error(const std::string& err);
  void build_rt_index();

#ifdef __OBJC__
  id<MTLDevice> device_;
  id<MTLCommandQueue> command_queue_;
  id<MTLComputePipelineState> pipeline_state_;

  id<MTLBuffer> buf_polygons_;
  id<MTLBuffer> buf_parts_;
  id<MTLBuffer> buf_rings_;
  id<MTLBuffer> buf_vertices_;

  id<MTLComputePipelineState> rt_pipeline_state_;
  id<MTLAccelerationStructure> rt_accel_;
  id<MTLBuffer> buf_ring_rt_slots_;
  id<MTLBuffer> buf_rt_infos_;
  id<MTLBuffer> buf_rt_stats_;
#else
  void* device_;
  void* command_queue_;
  void* pipeline_state_;

  void* buf_polygons_;
  void* buf_parts_;
  void* buf_rings_;
  void* buf_vertices_;

  void* rt_pipeline_state_;
  void* rt_accel_;
  void* buf_ring_rt_slots_;
  void* buf_rt_infos_;
  void* buf_rt_stats_;
#endif

  RtConfig rt_config_;
  int bound_mode_;
  uint64_t rt_info_[8];

  std::vector<PolygonRecord> host_polygons_;
  std::vector<PartRecord> host_parts_;
  std::vector<RingRecord> host_rings_;
  std::vector<Point2D> host_vertices_;

  bool is_built_;
  uint32_t num_polygons_;
  uint64_t allocated_bytes_;
  std::string device_name_;
  mutable std::mutex error_mutex_;
  mutable std::string last_error_;
};
