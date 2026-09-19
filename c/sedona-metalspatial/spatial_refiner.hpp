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

#include "geom_types.hpp"
#include <vector>
#include <string>
#include <cstdint>

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

    void push_polygons(
        const PolygonRecord* polys, uint32_t poly_count,
        const PartRecord* parts, uint32_t part_count,
        const RingRecord* rings, uint32_t ring_count,
        const Point2D* vertices, uint32_t vertex_count);

    void finish_building();

    void refine(
        const DecomposedPoint* points, uint32_t point_count,
        const uint32_t* candidate_build_indices,
        const uint32_t* candidate_probe_indices,
        uint32_t candidate_count,
        uint8_t* out_states);

    const char* get_last_error() const;
    const char* get_device_name() const;

private:
    void set_error(const std::string& err);

#ifdef __OBJC__
    id<MTLDevice> device_;
    id<MTLCommandQueue> command_queue_;
    id<MTLComputePipelineState> pipeline_state_;

    id<MTLBuffer> buf_polygons_;
    id<MTLBuffer> buf_parts_;
    id<MTLBuffer> buf_rings_;
    id<MTLBuffer> buf_vertices_;
#else
    void* device_;
    void* command_queue_;
    void* pipeline_state_;

    void* buf_polygons_;
    void* buf_parts_;
    void* buf_rings_;
    void* buf_vertices_;
#endif

    std::vector<PolygonRecord> host_polygons_;
    std::vector<PartRecord> host_parts_;
    std::vector<RingRecord> host_rings_;
    std::vector<Point2D> host_vertices_;

    bool is_built_;
    std::string device_name_;
    mutable std::string last_error_;
};
