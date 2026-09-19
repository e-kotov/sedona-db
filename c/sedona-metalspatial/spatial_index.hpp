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

#ifdef __OBJC__
#import <Metal/Metal.h>
typedef id<MTLDevice> MetalDeviceHandle;
#else
#include <cstddef>
typedef void* MetalDeviceHandle;
#endif

#include <vector>
#include <cstdint>
#include <memory>

enum class IndexType {
    Auto = 0,       // Hardware RT if available and applicable, otherwise Fast Spatial Hash
    HardwareRT = 1, // Apple Silicon Metal 3 Hardware Ray Tracing BVH
    SpatialHash = 2 // Fast 2D Uniform Grid Compute Index
};

#ifndef SEDONA_BOUNDING_BOX_DEFINED
#define SEDONA_BOUNDING_BOX_DEFINED
struct BoundingBox {
    float xmin;
    float ymin;
    float xmax;
    float ymax;
};

struct MatchPair {
    uint32_t build_idx;
    uint32_t probe_idx;
};
#endif

class MetalSpatialIndex {
public:
    explicit MetalSpatialIndex(MetalDeviceHandle device = nullptr);
    ~MetalSpatialIndex();

    // Mode control & inspection
    void set_index_type(IndexType type);
    IndexType get_index_type() const;
    IndexType get_active_index_type() const;
    bool supports_hardware_rt() const;
    bool is_valid() const;

    // SedonaDB Spatial Index API
    bool push_build(const float* rects_flat, uint32_t count);
    bool finish_building();
    bool probe(const float* rects_flat, uint32_t count,
               std::vector<uint32_t>& out_build, std::vector<uint32_t>& out_probe);

    // Diagnostics / performance metrics / errors
    double get_last_build_time_ms() const;
    double get_last_probe_time_ms() const;
    uint32_t get_build_count() const;
    uint64_t get_memory_usage() const;
    const char* get_last_error() const;
    void set_last_error(const std::string& err);
    void clear();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
