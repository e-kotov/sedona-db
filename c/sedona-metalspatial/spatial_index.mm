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

#import "spatial_index.hpp"
#import "metal_shaders.h"
#import <Metal/Metal.h>
#include <iostream>
#include <vector>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <mutex>

inline bool is_valid_box(const BoundingBox& b) {
    return !(std::isnan(b.xmin) || std::isnan(b.ymin) || std::isnan(b.xmax) || std::isnan(b.ymax) ||
             std::isinf(b.xmin) || std::isinf(b.ymin) || std::isinf(b.xmax) || std::isinf(b.ymax) ||
             b.xmin > b.xmax || b.ymin > b.ymax);
}

struct GridLevelParamsInternal {
    float cell_w;
    float cell_h;
    uint32_t grid_dim;
    uint32_t cell_offset;
};

struct HierarchicalGridParamsInternal {
    float min_x;
    float min_y;
    uint32_t num_levels;
    uint32_t num_build;
    uint32_t num_probe;
    uint32_t probe_offset;
    uint32_t max_results;
    GridLevelParamsInternal levels[4];
};

struct MetalSpatialIndex::Impl {
    id<MTLDevice> device = nil;
    id<MTLCommandQueue> queue = nil;

    IndexType requested_type = IndexType::Auto;
    std::atomic<IndexType> active_type{IndexType::SpatialHash};
    bool has_hw_rt = false;
    std::atomic<bool> is_built{false};
    std::mutex build_mutex;

    // Compiled pipelines
    id<MTLComputePipelineState> pso_rt_probe = nil;
    id<MTLComputePipelineState> pso_grid_count = nil;
    id<MTLComputePipelineState> pso_grid_populate = nil;
    id<MTLComputePipelineState> pso_grid_probe = nil;

    // Build data
    std::vector<BoundingBox> build_boxes;
    id<MTLBuffer> buf_build_boxes = nil;

    // Hardware RT Acceleration Structure
    id<MTLAccelerationStructure> rt_accel = nil;
    id<MTLBuffer> buf_rt_bboxes = nil;

    // Hierarchical Grid structures
    HierarchicalGridParamsInternal hier_params{};
    id<MTLBuffer> buf_grid_offsets = nil;
    id<MTLBuffer> buf_grid_entries = nil;

    // Diagnostics / Errors
    double last_build_time_ms = 0.0;
    std::atomic<double> last_probe_time_ms{0.0};
    std::string last_error;
    std::mutex error_mutex;

    void set_last_error(const std::string& err) {
        std::lock_guard<std::mutex> lock(error_mutex);
        last_error = err;
    }

    std::string get_last_error() {
        std::lock_guard<std::mutex> lock(error_mutex);
        return last_error;
    }

    void init_pipelines() {
        @autoreleasepool {
            has_hw_rt = [device supportsRaytracing];

            NSError *err = nil;
            MTLCompileOptions *opts = [MTLCompileOptions new];
            opts.languageVersion = MTLLanguageVersion3_0;
            if (@available(macOS 15, *)) {
                opts.mathMode = MTLMathModeSafe;
            } else {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
                opts.fastMathEnabled = NO;
#pragma clang diagnostic pop
            }

            // 1. Compile Spatial Hash Shaders
            NSString *hashSrc = [NSString stringWithUTF8String:SPATIAL_HASH_METAL_SOURCE];
            id<MTLLibrary> hashLib = [device newLibraryWithSource:hashSrc options:opts error:&err];
            if (!hashLib) {
                set_last_error(std::string("Error compiling Spatial Hash shaders: ") + [[err localizedDescription] UTF8String]);
            } else {
                id<MTLFunction> fnCount = [hashLib newFunctionWithName:@"count_cell_entries"];
                id<MTLFunction> fnPop = [hashLib newFunctionWithName:@"populate_cells"];
                id<MTLFunction> fnProbe = [hashLib newFunctionWithName:@"probe_grid"];
                pso_grid_count = [device newComputePipelineStateWithFunction:fnCount error:&err];
                pso_grid_populate = [device newComputePipelineStateWithFunction:fnPop error:&err];
                pso_grid_probe = [device newComputePipelineStateWithFunction:fnProbe error:&err];
                if (!pso_grid_count || !pso_grid_populate || !pso_grid_probe) {
                    set_last_error(std::string("Error creating Spatial Hash PSOs: ") + (err ? [[err localizedDescription] UTF8String] : "unknown"));
                }
            }

            // 2. Compile Hardware Ray Tracing Shaders (if supported)
            if (has_hw_rt) {
                NSString *rtSrc = [NSString stringWithUTF8String:BVH_METAL_SOURCE];
                id<MTLLibrary> rtLib = [device newLibraryWithSource:rtSrc options:opts error:&err];
                if (!rtLib) {
                    has_hw_rt = false;
                } else {
                    id<MTLFunction> fnRtProbe = [rtLib newFunctionWithName:@"rt_probe_points"];
                    pso_rt_probe = [device newComputePipelineStateWithFunction:fnRtProbe error:&err];
                    if (!pso_rt_probe) {
                        has_hw_rt = false;
                    }
                }
            }
        }
    }

    bool build_rt_index() {
        @autoreleasepool {
            uint32_t n = static_cast<uint32_t>(build_boxes.size());
            if (n == 0) return true;
            std::vector<MTLAxisAlignedBoundingBox> mtl_boxes(n);
            // Expand conservative bounds to prevent grazing-ray precision misses on boundaries across any coordinate scale
            for (uint32_t i = 0; i < n; ++i) {
                const auto& b = build_boxes[i];
                if (!is_valid_box(b)) {
                    // Park NaN/Inf/inverted rows at a dummy bounding box outside ray range (z in [10, 11])
                    mtl_boxes[i].min = MTLPackedFloat3Make(0.0f, 0.0f, 10.0f);
                    mtl_boxes[i].max = MTLPackedFloat3Make(0.0f, 0.0f, 11.0f);
                    continue;
                }
                // Conservative expansion to prevent grazing ray misses across any coordinate scale,
                // avoiding denormal flush-to-zero (FTZ) around zero boundaries on Apple Silicon RT units.
                float scale_x = std::max(std::max(std::abs(b.xmin), std::abs(b.xmax)), b.xmax - b.xmin);
                float scale_y = std::max(std::max(std::abs(b.ymin), std::abs(b.ymax)), b.ymax - b.ymin);
                float pad_x = std::max(scale_x * 1e-4f, 1e-5f);
                float pad_y = std::max(scale_y * 1e-4f, 1e-5f);
                float xmin_pad = b.xmin - pad_x;
                float xmax_pad = b.xmax + pad_x;
                float ymin_pad = b.ymin - pad_y;
                float ymax_pad = b.ymax + pad_y;
                mtl_boxes[i].min = MTLPackedFloat3Make(xmin_pad, ymin_pad, -0.5f);
                mtl_boxes[i].max = MTLPackedFloat3Make(xmax_pad, ymax_pad, 0.5f);
            }

            buf_rt_bboxes = [device newBufferWithBytes:mtl_boxes.data()
                                                length:sizeof(MTLAxisAlignedBoundingBox) * n
                                               options:MTLResourceStorageModeShared];
            if (!buf_rt_bboxes) {
                set_last_error("Failed to allocate RT bounding box buffer");
                return false;
            }

            MTLAccelerationStructureBoundingBoxGeometryDescriptor *geomDesc =
                [MTLAccelerationStructureBoundingBoxGeometryDescriptor descriptor];
            geomDesc.boundingBoxBuffer = buf_rt_bboxes;
            geomDesc.boundingBoxBufferOffset = 0;
            geomDesc.boundingBoxCount = n;
            geomDesc.boundingBoxStride = sizeof(MTLAxisAlignedBoundingBox);

            MTLPrimitiveAccelerationStructureDescriptor *accelDesc =
                [MTLPrimitiveAccelerationStructureDescriptor descriptor];
            accelDesc.geometryDescriptors = @[geomDesc];

            MTLAccelerationStructureSizes sizes = [device accelerationStructureSizesWithDescriptor:accelDesc];
            NSUInteger accelSize = std::max(sizes.accelerationStructureSize, (NSUInteger)256);
            NSUInteger scratchSize = std::max(sizes.buildScratchBufferSize, (NSUInteger)256);

            rt_accel = [device newAccelerationStructureWithSize:accelSize];
            if (!rt_accel) {
                set_last_error("Failed to allocate RT acceleration structure");
                return false;
            }
            id<MTLBuffer> scratch = [device newBufferWithLength:scratchSize
                                                        options:MTLResourceStorageModePrivate];
            if (!scratch) {
                set_last_error("Failed to allocate RT scratch buffer");
                return false;
            }

            id<MTLCommandBuffer> cmd = [queue commandBuffer];
            if (!cmd) {
                set_last_error("Failed to create command buffer for RT build");
                return false;
            }
            id<MTLAccelerationStructureCommandEncoder> enc = [cmd accelerationStructureCommandEncoder];
            if (!enc) {
                set_last_error("Failed to create RT acceleration structure encoder");
                return false;
            }
            [enc buildAccelerationStructure:rt_accel descriptor:accelDesc scratchBuffer:scratch scratchBufferOffset:0];
            [enc endEncoding];
            [cmd commit];
            [cmd waitUntilCompleted];
            if ([cmd status] != MTLCommandBufferStatusCompleted) {
                NSString* errStr = [cmd.error localizedDescription] ?: @"Error during RT BVH build";
                set_last_error([errStr UTF8String]);
                return false;
            }
            return true;
        }
    }

    bool build_grid_index() {
        @autoreleasepool {
            uint32_t n = static_cast<uint32_t>(build_boxes.size());
            if (n == 0) return true;
            float min_x = 1e30f, min_y = 1e30f, max_x = -1e30f, max_y = -1e30f;
            uint32_t valid_count = 0;
            for (uint32_t i = 0; i < n; ++i) {
                if (is_valid_box(build_boxes[i])) {
                    min_x = std::min(min_x, build_boxes[i].xmin);
                    min_y = std::min(min_y, build_boxes[i].ymin);
                    max_x = std::max(max_x, build_boxes[i].xmax);
                    max_y = std::max(max_y, build_boxes[i].ymax);
                    valid_count++;
                }
            }

            if (valid_count == 0) {
                // All boxes are NaN/Inf/invalid. Handle gracefully: empty index, 0 matches.
                hier_params.min_x = 0.0f;
                hier_params.min_y = 0.0f;
                hier_params.num_levels = 1;
                hier_params.num_build = 0;
                hier_params.num_probe = 0;
                hier_params.probe_offset = 0;
                hier_params.max_results = 0;
                hier_params.levels[0] = {1.0f, 1.0f, 1, 0};

                std::vector<uint32_t> offsets(2, 0);
                buf_grid_offsets = [device newBufferWithBytes:offsets.data()
                                                       length:sizeof(uint32_t) * 2
                                                      options:MTLResourceStorageModeShared];
                buf_grid_entries = [device newBufferWithLength:sizeof(uint32_t)
                                                       options:MTLResourceStorageModeShared];
                if (!buf_grid_offsets || !buf_grid_entries) {
                    set_last_error("Failed to allocate empty grid buffers");
                    return false;
                }
                return true;
            }

            float span_x = std::max(max_x - min_x, 1e-4f);
            float span_y = std::max(max_y - min_y, 1e-4f);

            hier_params.min_x = min_x - span_x * 0.001f;
            hier_params.min_y = min_y - span_y * 0.001f;
            float total_w = span_x * 1.002f;
            float total_h = span_y * 1.002f;
            hier_params.num_levels = 4;
            hier_params.num_build = n;
            hier_params.num_probe = 0;
            hier_params.probe_offset = 0;
            hier_params.max_results = 0;

            uint32_t dims[4] = {
                2,
                8,
                32,
                static_cast<uint32_t>(std::clamp(static_cast<float>(std::sqrt(valid_count) * 1.5f), 64.0f, 256.0f))
            };

            uint32_t cur_offset = 0;
            for (uint32_t l = 0; l < 4; ++l) {
                hier_params.levels[l].grid_dim = dims[l];
                hier_params.levels[l].cell_offset = cur_offset;
                hier_params.levels[l].cell_w = total_w / dims[l];
                hier_params.levels[l].cell_h = total_h / dims[l];
                cur_offset += dims[l] * dims[l];
            }
            uint32_t total_cells = cur_offset;

            if (!buf_build_boxes) {
                buf_build_boxes = [device newBufferWithBytes:build_boxes.data()
                                                      length:sizeof(BoundingBox) * n
                                                     options:MTLResourceStorageModeShared];
                if (!buf_build_boxes) {
                    set_last_error("Failed to allocate build boxes buffer");
                    return false;
                }
            }

            id<MTLBuffer> buf_counts = [device newBufferWithLength:sizeof(uint32_t) * total_cells
                                                           options:MTLResourceStorageModeShared];
            if (!buf_counts) {
                set_last_error("Failed to allocate grid counts buffer");
                return false;
            }
            memset([buf_counts contents], 0, sizeof(uint32_t) * total_cells);

            id<MTLBuffer> buf_params = [device newBufferWithBytes:&hier_params
                                                           length:sizeof(HierarchicalGridParamsInternal)
                                                          options:MTLResourceStorageModeShared];
            if (!buf_params) {
                set_last_error("Failed to allocate grid params buffer");
                return false;
            }

            // 1. Count entries per cell across all levels
            id<MTLCommandBuffer> cmd1 = [queue commandBuffer];
            if (!cmd1) {
                set_last_error("Failed to create command buffer for count pass");
                return false;
            }
            id<MTLComputeCommandEncoder> enc1 = [cmd1 computeCommandEncoder];
            if (!enc1) {
                set_last_error("Failed to create compute command encoder for count pass");
                return false;
            }
            [enc1 setComputePipelineState:pso_grid_count];
            [enc1 setBuffer:buf_build_boxes offset:0 atIndex:0];
            [enc1 setBuffer:buf_counts offset:0 atIndex:1];
            [enc1 setBuffer:buf_params offset:0 atIndex:2];
            NSUInteger tg_count = std::min((NSUInteger)256, pso_grid_count.maxTotalThreadsPerThreadgroup);
            [enc1 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(tg_count, 1, 1)];
            [enc1 endEncoding];
            [cmd1 commit];
            [cmd1 waitUntilCompleted];
            if ([cmd1 status] != MTLCommandBufferStatusCompleted) {
                NSString* errStr = [cmd1.error localizedDescription] ?: @"Error during count pass";
                set_last_error([errStr UTF8String]);
                return false;
            }

            // 2. Prefix sum for cell offsets with explicit overflow check
            uint32_t* counts = (uint32_t*)[buf_counts contents];
            std::vector<uint32_t> offsets(total_cells + 1, 0);
            uint64_t running_total = 0;
            for (uint32_t i = 0; i < total_cells; ++i) {
                offsets[i] = static_cast<uint32_t>(running_total);
                running_total += counts[i];
                if (running_total > UINT32_MAX) {
                    set_last_error("Grid cell entries count exceeded 32-bit integer limit");
                    return false;
                }
            }
            offsets[total_cells] = static_cast<uint32_t>(running_total);
            uint32_t total_entries = static_cast<uint32_t>(running_total);

            buf_grid_offsets = [device newBufferWithBytes:offsets.data()
                                                   length:sizeof(uint32_t) * (total_cells + 1)
                                                  options:MTLResourceStorageModeShared];
            buf_grid_entries = [device newBufferWithLength:sizeof(uint32_t) * std::max(1u, total_entries)
                                                   options:MTLResourceStorageModeShared];
            if (!buf_grid_offsets || !buf_grid_entries) {
                set_last_error("Failed to allocate grid offsets or entries buffer");
                return false;
            }

            id<MTLBuffer> buf_heads = [device newBufferWithLength:sizeof(uint32_t) * total_cells
                                                          options:MTLResourceStorageModeShared];
            if (!buf_heads) {
                set_last_error("Failed to allocate grid heads buffer");
                return false;
            }
            memset([buf_heads contents], 0, sizeof(uint32_t) * total_cells);

            // 3. Populate cell entries
            id<MTLCommandBuffer> cmd2 = [queue commandBuffer];
            if (!cmd2) {
                set_last_error("Failed to create command buffer for populate pass");
                return false;
            }
            id<MTLComputeCommandEncoder> enc2 = [cmd2 computeCommandEncoder];
            if (!enc2) {
                set_last_error("Failed to create compute command encoder for populate pass");
                return false;
            }
            [enc2 setComputePipelineState:pso_grid_populate];
            [enc2 setBuffer:buf_build_boxes offset:0 atIndex:0];
            [enc2 setBuffer:buf_heads offset:0 atIndex:1];
            [enc2 setBuffer:buf_grid_offsets offset:0 atIndex:2];
            [enc2 setBuffer:buf_grid_entries offset:0 atIndex:3];
            [enc2 setBuffer:buf_params offset:0 atIndex:4];
            NSUInteger tg_pop = std::min((NSUInteger)256, pso_grid_populate.maxTotalThreadsPerThreadgroup);
            [enc2 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(tg_pop, 1, 1)];
            [enc2 endEncoding];
            [cmd2 commit];
            [cmd2 waitUntilCompleted];
            if ([cmd2 status] != MTLCommandBufferStatusCompleted) {
                NSString* errStr = [cmd2.error localizedDescription] ?: @"Error during populate pass";
                set_last_error([errStr UTF8String]);
                return false;
            }
            return true;
        }
    }
};

MetalSpatialIndex::MetalSpatialIndex(MetalDeviceHandle device)
    : impl_(std::make_unique<Impl>())
{
    @autoreleasepool {
        id<MTLDevice> mtl_dev = (id<MTLDevice>)device;
        if (!mtl_dev) {
            mtl_dev = MTLCreateSystemDefaultDevice();
        }
        impl_->device = mtl_dev;
        if (mtl_dev) {
            impl_->queue = [mtl_dev newCommandQueue];
            impl_->init_pipelines();
        }
    }
}

MetalSpatialIndex::~MetalSpatialIndex() = default;

void MetalSpatialIndex::set_index_type(IndexType type) {
    impl_->requested_type = type;
}

IndexType MetalSpatialIndex::get_index_type() const {
    return impl_->requested_type;
}

IndexType MetalSpatialIndex::get_active_index_type() const {
    return impl_->active_type.load();
}

bool MetalSpatialIndex::supports_hardware_rt() const {
    return impl_->has_hw_rt;
}

bool MetalSpatialIndex::is_valid() const {
    if (!impl_->device || !impl_->queue) return false;
    if (!impl_->pso_grid_count || !impl_->pso_grid_populate || !impl_->pso_grid_probe) {
        return false;
    }
    if (impl_->has_hw_rt && !impl_->pso_rt_probe) {
        return false;
    }
    return true;
}

bool MetalSpatialIndex::push_build(const float* rects_flat, uint32_t count) {
    if (count > 0 && !rects_flat) {
        impl_->set_last_error("Null rects pointer with non-zero count");
        return false;
    }
    if (count == 0) return true;
    const auto* boxes = reinterpret_cast<const BoundingBox*>(rects_flat);
    impl_->build_boxes.insert(impl_->build_boxes.end(), boxes, boxes + count);
    impl_->is_built.store(false);
    impl_->buf_build_boxes = nil;
    impl_->rt_accel = nil;
    impl_->buf_rt_bboxes = nil;
    impl_->buf_grid_offsets = nil;
    impl_->buf_grid_entries = nil;
    return true;
}

bool MetalSpatialIndex::finish_building() {
    if (impl_->build_boxes.empty() || impl_->is_built.load()) return true;
    if (!is_valid()) {
        impl_->set_last_error("Index is invalid (missing device, queue or pipeline state)");
        return false;
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    uint32_t n = static_cast<uint32_t>(impl_->build_boxes.size());
    impl_->buf_build_boxes = [impl_->device newBufferWithBytes:impl_->build_boxes.data()
                                                        length:sizeof(BoundingBox) * n
                                                       options:MTLResourceStorageModeShared];
    if (!impl_->buf_build_boxes) {
        impl_->set_last_error("Failed to allocate build boxes buffer");
        return false;
    }

    // Build Spatial Hash Grid (always available as universal engine / fast fallback)
    if (!impl_->build_grid_index()) {
        return false;
    }

    // If Hardware RT is requested or Auto, build Hardware BVH
    if (impl_->has_hw_rt && (impl_->requested_type == IndexType::HardwareRT || impl_->requested_type == IndexType::Auto)) {
        if (!impl_->build_rt_index()) {
            return false;
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    impl_->last_build_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    impl_->is_built.store(true);
    return true;
}

bool MetalSpatialIndex::probe(const float* rects_flat, uint32_t count,
                              std::vector<uint32_t>& out_build, std::vector<uint32_t>& out_probe)
{
    out_build.clear();
    out_probe.clear();
    if (count > 0 && !rects_flat) {
        impl_->set_last_error("Null rects pointer with non-zero count");
        return false;
    }
    if (count == 0 || impl_->build_boxes.empty()) return true;
    if (!is_valid()) {
        impl_->set_last_error("Index is invalid (missing device, queue or pipeline state)");
        return false;
    }

    if (!impl_->is_built.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(impl_->build_mutex);
        if (!impl_->is_built.load(std::memory_order_relaxed)) {
            if (!finish_building()) {
                return false;
            }
        }
    }

    @autoreleasepool {
        const auto* probe_boxes = reinterpret_cast<const BoundingBox*>(rects_flat);

        // Detect if all probe geometries are points (xmin == xmax && ymin == ymax)
        bool is_points = true;
        for (uint32_t i = 0; i < count; ++i) {
            if (!is_valid_box(probe_boxes[i])) continue;
            if (probe_boxes[i].xmin != probe_boxes[i].xmax || probe_boxes[i].ymin != probe_boxes[i].ymax) {
                is_points = false;
                break;
            }
        }

        // Determine active engine
        bool use_rt = false;
        if (impl_->requested_type == IndexType::HardwareRT) {
            use_rt = impl_->has_hw_rt && (impl_->rt_accel != nil) && is_points;
        } else if (impl_->requested_type == IndexType::Auto) {
            use_rt = impl_->has_hw_rt && (impl_->rt_accel != nil) && is_points;
        }

        impl_->active_type.store(use_rt ? IndexType::HardwareRT : IndexType::SpatialHash, std::memory_order_relaxed);

        auto t0 = std::chrono::high_resolution_clock::now();

        id<MTLBuffer> buf_probe = [impl_->device newBufferWithBytes:rects_flat
                                                             length:sizeof(BoundingBox) * count
                                                            options:MTLResourceStorageModeShared];
        if (!buf_probe) {
            impl_->set_last_error("Failed to allocate probe buffer");
            return false;
        }

        uint64_t max_pairs_capacity = impl_->device.maxBufferLength / sizeof(MatchPair);
        uint64_t initial_max = std::min(max_pairs_capacity, std::max(static_cast<uint64_t>(count) * 8ULL, 1000000ULL));
        initial_max = std::min(initial_max, static_cast<uint64_t>(UINT32_MAX));
        uint32_t max_results = static_cast<uint32_t>(initial_max);

        id<MTLBuffer> buf_out = [impl_->device newBufferWithLength:sizeof(MatchPair) * max_results
                                                           options:MTLResourceStorageModeShared];
        id<MTLBuffer> buf_count = [impl_->device newBufferWithLength:sizeof(uint32_t)
                                                             options:MTLResourceStorageModeShared];
        if (!buf_out || !buf_count || ![buf_count contents] || ![buf_out contents]) {
            impl_->set_last_error("Failed to allocate match result buffers");
            return false;
        }
        *((uint32_t*)[buf_count contents]) = 0;

        const uint32_t CHUNK_SIZE = 65536;

        auto dispatch_query = [&](uint32_t curr_max) -> bool {
            for (uint32_t probe_offset = 0; probe_offset < count; probe_offset += CHUNK_SIZE) {
                uint32_t chunk_count = std::min(count - probe_offset, CHUNK_SIZE);
                if (use_rt) {
                    id<MTLBuffer> buf_num_probes = [impl_->device newBufferWithBytes:&chunk_count length:sizeof(uint32_t) options:MTLResourceStorageModeShared];
                    id<MTLBuffer> buf_max_results = [impl_->device newBufferWithBytes:&curr_max length:sizeof(uint32_t) options:MTLResourceStorageModeShared];
                    id<MTLBuffer> buf_probe_offset = [impl_->device newBufferWithBytes:&probe_offset length:sizeof(uint32_t) options:MTLResourceStorageModeShared];
                    if (!buf_num_probes || !buf_max_results || !buf_probe_offset) {
                        impl_->set_last_error("Failed to allocate query parameter buffers");
                        return false;
                    }

                    id<MTLCommandBuffer> cmd = [impl_->queue commandBuffer];
                    if (!cmd) {
                        impl_->set_last_error("Failed to create probe command buffer");
                        return false;
                    }
                    id<MTLComputeCommandEncoder> enc = [cmd computeCommandEncoder];
                    if (!enc) {
                        impl_->set_last_error("Failed to create probe compute command encoder");
                        return false;
                    }
                    [enc setComputePipelineState:impl_->pso_rt_probe];
                    [enc setAccelerationStructure:impl_->rt_accel atBufferIndex:0];
                    [enc setBuffer:impl_->buf_build_boxes offset:0 atIndex:1];
                    [enc setBuffer:buf_probe offset:0 atIndex:2];
                    [enc setBuffer:buf_out offset:0 atIndex:3];
                    [enc setBuffer:buf_count offset:0 atIndex:4];
                    [enc setBuffer:buf_num_probes offset:0 atIndex:5];
                    [enc setBuffer:buf_max_results offset:0 atIndex:6];
                    [enc setBuffer:buf_probe_offset offset:0 atIndex:7];
                    NSUInteger tg_rt = std::min((NSUInteger)256, impl_->pso_rt_probe.maxTotalThreadsPerThreadgroup);
                    [enc dispatchThreads:MTLSizeMake(chunk_count, 1, 1) threadsPerThreadgroup:MTLSizeMake(tg_rt, 1, 1)];
                    [enc endEncoding];
                    [cmd commit];
                    [cmd waitUntilCompleted];
                    if ([cmd status] != MTLCommandBufferStatusCompleted) {
                        NSString* errStr = [cmd.error localizedDescription] ?: @"Error during RT probe execution";
                        impl_->set_last_error([errStr UTF8String]);
                        return false;
                    }
                } else {
                    HierarchicalGridParamsInternal params = impl_->hier_params;
                    params.num_probe = chunk_count;
                    params.probe_offset = probe_offset;
                    params.max_results = curr_max;

                    id<MTLBuffer> buf_params = [impl_->device newBufferWithBytes:&params
                                                                          length:sizeof(HierarchicalGridParamsInternal)
                                                                         options:MTLResourceStorageModeShared];
                    if (!buf_params) {
                        impl_->set_last_error("Failed to allocate grid params buffer");
                        return false;
                    }

                    id<MTLCommandBuffer> cmd = [impl_->queue commandBuffer];
                    if (!cmd) {
                        impl_->set_last_error("Failed to create probe command buffer");
                        return false;
                    }
                    id<MTLComputeCommandEncoder> enc = [cmd computeCommandEncoder];
                    if (!enc) {
                        impl_->set_last_error("Failed to create probe compute command encoder");
                        return false;
                    }
                    [enc setComputePipelineState:impl_->pso_grid_probe];
                    [enc setBuffer:impl_->buf_build_boxes offset:0 atIndex:0];
                    [enc setBuffer:buf_probe offset:0 atIndex:1];
                    [enc setBuffer:impl_->buf_grid_offsets offset:0 atIndex:2];
                    [enc setBuffer:impl_->buf_grid_entries offset:0 atIndex:3];
                    [enc setBuffer:buf_out offset:0 atIndex:4];
                    [enc setBuffer:buf_count offset:0 atIndex:5];
                    [enc setBuffer:buf_params offset:0 atIndex:6];
                    NSUInteger tg_grid = std::min((NSUInteger)256, impl_->pso_grid_probe.maxTotalThreadsPerThreadgroup);
                    [enc dispatchThreads:MTLSizeMake(chunk_count, 1, 1) threadsPerThreadgroup:MTLSizeMake(tg_grid, 1, 1)];
                    [enc endEncoding];
                    [cmd commit];
                    [cmd waitUntilCompleted];
                    if ([cmd status] != MTLCommandBufferStatusCompleted) {
                        NSString* errStr = [cmd.error localizedDescription] ?: @"Error during grid probe execution";
                        impl_->set_last_error([errStr UTF8String]);
                        return false;
                    }
                }
            }
            return true;
        };

        if (!dispatch_query(max_results)) {
            return false;
        }

        uint32_t total_matches = *((uint32_t*)[buf_count contents]);
        if (total_matches > max_results) {
            uint64_t needed_bytes = static_cast<uint64_t>(total_matches) * sizeof(MatchPair);
            if (needed_bytes > impl_->device.maxBufferLength || total_matches == UINT32_MAX) {
                impl_->set_last_error("Match results exceeded maximum Metal buffer capacity");
                return false;
            }
            max_results = total_matches;
            buf_out = [impl_->device newBufferWithLength:sizeof(MatchPair) * max_results
                                                 options:MTLResourceStorageModeShared];
            if (!buf_out || ![buf_out contents]) {
                impl_->set_last_error("Failed to reallocate match results buffer");
                return false;
            }
            *((uint32_t*)[buf_count contents]) = 0;
            if (!dispatch_query(max_results)) {
                return false;
            }
            total_matches = *((uint32_t*)[buf_count contents]);
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        impl_->last_probe_time_ms.store(std::chrono::duration<double, std::milli>(t1 - t0).count(), std::memory_order_relaxed);

        uint32_t valid_matches = std::min(total_matches, max_results);
        const auto* pairs = reinterpret_cast<const MatchPair*>([buf_out contents]);
        out_build.resize(valid_matches);
        out_probe.resize(valid_matches);

        for (uint32_t i = 0; i < valid_matches; ++i) {
            out_build[i] = pairs[i].build_idx;
            out_probe[i] = pairs[i].probe_idx;
        }
        return true;
    }
}

const char* MetalSpatialIndex::get_last_error() const {
    static thread_local std::string s_err;
    s_err = impl_->get_last_error();
    return s_err.c_str();
}

void MetalSpatialIndex::set_last_error(const std::string& err) {
    impl_->set_last_error(err);
}

double MetalSpatialIndex::get_last_build_time_ms() const {
    return impl_->last_build_time_ms;
}

double MetalSpatialIndex::get_last_probe_time_ms() const {
    return impl_->last_probe_time_ms.load();
}

uint32_t MetalSpatialIndex::get_build_count() const {
    return static_cast<uint32_t>(impl_->build_boxes.size());
}

void MetalSpatialIndex::clear() {
    impl_->build_boxes.clear();
    impl_->buf_build_boxes = nil;
    impl_->rt_accel = nil;
    impl_->buf_rt_bboxes = nil;
    impl_->buf_grid_offsets = nil;
    impl_->buf_grid_entries = nil;
    impl_->is_built.store(false);
    impl_->last_build_time_ms = 0.0;
    impl_->last_probe_time_ms.store(0.0);
}
