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

#import "spatial_refiner.hpp"
#import <Foundation/Foundation.h>
#include <algorithm>
#include <stdexcept>
#import "metal_shaders.h"

#ifdef ENABLE_TEST_INTERNALS
MetalSpatialRefiner::MetalSpatialRefiner(id device, int bound_mode)
#else
MetalSpatialRefiner::MetalSpatialRefiner(id device)
#endif
    : device_(device),
      command_queue_(nil),
      pipeline_state_(nil),
      buf_polygons_(nil),
      buf_parts_(nil),
      buf_rings_(nil),
      buf_vertices_(nil),
      exact_pipeline_(nil),
      buf_exact_vertices_(nil),
      buf_exact_poly_ok_(nil),
      is_built_(false),
      num_polygons_(0),
      allocated_bytes_(0) {
  @autoreleasepool {
    if (!device_) {
      device_ = MTLCreateSystemDefaultDevice();
    }
    if (!device_) {
      set_error("No Metal-compatible GPU found");
      throw std::runtime_error("No Metal-compatible GPU found");
    }

    device_name_ = [device_.name UTF8String];
    command_queue_ = [device_ newCommandQueue];
    if (!command_queue_) {
      set_error("Failed to create Metal command queue");
      throw std::runtime_error("Failed to create Metal command queue");
    }

    MTLCompileOptions* options = [[MTLCompileOptions alloc] init];
    options.fastMathEnabled = NO;
    if (@available(macOS 15.0, *)) {
      options.mathMode = MTLMathModeSafe;
    }
#ifdef ENABLE_TEST_INTERNALS
    options.preprocessorMacros = @{@"BOUND_MODE" : @(bound_mode)};
#else
    options.preprocessorMacros = @{@"BOUND_MODE" : @(0)};
#endif

    NSError* error = nil;
    NSString* src = [NSString stringWithUTF8String:REFINE_METAL_SOURCE];
    id<MTLLibrary> library = [device_ newLibraryWithSource:src
                                                   options:options
                                                     error:&error];
    if (!library) {
      std::string err_str =
          error ? [[error localizedDescription] UTF8String] : "Unknown shader error";
      set_error("Failed to compile refine.metal: " + err_str);
      throw std::runtime_error("Failed to compile refine.metal: " + err_str);
    }

    id<MTLFunction> kernel_fn = [library newFunctionWithName:@"point_in_polygon_refine"];
    if (!kernel_fn) {
      set_error("Function point_in_polygon_refine not found in library");
      throw std::runtime_error("Function point_in_polygon_refine not found in library");
    }

    pipeline_state_ = [device_ newComputePipelineStateWithFunction:kernel_fn
                                                             error:&error];
    if (!pipeline_state_) {
      std::string err_str =
          error ? [[error localizedDescription] UTF8String] : "Unknown pipeline error";
      set_error("Failed to create pipeline state: " + err_str);
      throw std::runtime_error("Failed to create pipeline state: " + err_str);
    }
  }
}

MetalSpatialRefiner::~MetalSpatialRefiner() { clear(); }

void MetalSpatialRefiner::set_error(const std::string& err) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  last_error_ = err;
}

const char* MetalSpatialRefiner::get_last_error() const {
  static thread_local std::string s_err;
  std::lock_guard<std::mutex> lock(error_mutex_);
  s_err = last_error_;
  return s_err.c_str();
}

const char* MetalSpatialRefiner::get_device_name() const { return device_name_.c_str(); }

uint64_t MetalSpatialRefiner::get_memory_usage() const { return allocated_bytes_; }

void MetalSpatialRefiner::set_last_error(const std::string& err) { set_error(err); }

void MetalSpatialRefiner::clear() {
  @autoreleasepool {
    buf_polygons_ = nil;
    buf_parts_ = nil;
    buf_rings_ = nil;
    buf_vertices_ = nil;
    buf_exact_vertices_ = nil;
    buf_exact_poly_ok_ = nil;
    exact_allocated_bytes_ = 0;

    host_polygons_.clear();
    host_parts_.clear();
    host_rings_.clear();
    host_vertices_.clear();

    is_built_ = false;
    allocated_bytes_ = 0;
    num_polygons_ = 0;
    last_error_.clear();
  }
}

void MetalSpatialRefiner::push_polygons(const PolygonRecord* polys, uint32_t poly_count,
                                        const PartRecord* parts, uint32_t part_count,
                                        const RingRecord* rings, uint32_t ring_count,
                                        const Point2D* vertices, uint32_t vertex_count) {
  if (is_built_) {
    set_error(
        "Cannot push polygons to an already finalized refiner. Call clear() first.");
    throw std::runtime_error("Refiner already built");
  }

  uint32_t part_offset = (uint32_t)host_parts_.size();
  uint32_t ring_offset = (uint32_t)host_rings_.size();
  uint32_t vertex_offset = (uint32_t)host_vertices_.size();

  // Re-base parts offsets
  for (uint32_t i = 0; i < part_count; ++i) {
    PartRecord p = parts[i];
    p.ring_start += ring_offset;
    host_parts_.push_back(p);
  }

  // Re-base rings offsets
  for (uint32_t i = 0; i < ring_count; ++i) {
    RingRecord r = rings[i];
    r.vertex_start += vertex_offset;
    host_rings_.push_back(r);
  }

  // Append vertices
  for (uint32_t i = 0; i < vertex_count; ++i) {
    host_vertices_.push_back(vertices[i]);
  }

  // Re-base polygon parts offset
  for (uint32_t i = 0; i < poly_count; ++i) {
    PolygonRecord poly = polys[i];
    if (poly.is_valid) {
      poly.part_start += part_offset;
    }
    host_polygons_.push_back(poly);
  }
}

void MetalSpatialRefiner::finish_building() {
  @autoreleasepool {
    if (is_built_) return;

    // Allocate GPU buffers with shared storage mode
    size_t poly_size =
        std::max(host_polygons_.size() * sizeof(PolygonRecord), (size_t)16);
    size_t part_size = std::max(host_parts_.size() * sizeof(PartRecord), (size_t)16);
    size_t ring_size = std::max(host_rings_.size() * sizeof(RingRecord), (size_t)16);
    size_t vert_size = std::max(host_vertices_.size() * sizeof(Point2D), (size_t)16);

    buf_polygons_ =
        [device_ newBufferWithBytes:host_polygons_.empty()
                                        ? (const void*)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                        : host_polygons_.data()
                             length:poly_size
                            options:MTLResourceStorageModeShared];

    buf_parts_ =
        [device_ newBufferWithBytes:host_parts_.empty()
                                        ? (const void*)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                        : host_parts_.data()
                             length:part_size
                            options:MTLResourceStorageModeShared];

    buf_rings_ =
        [device_ newBufferWithBytes:host_rings_.empty()
                                        ? (const void*)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                        : host_rings_.data()
                             length:ring_size
                            options:MTLResourceStorageModeShared];

    buf_vertices_ =
        [device_ newBufferWithBytes:host_vertices_.empty()
                                        ? (const void*)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                        : host_vertices_.data()
                             length:vert_size
                            options:MTLResourceStorageModeShared];

    if (!buf_polygons_ || !buf_parts_ || !buf_rings_ || !buf_vertices_) {
      set_error("Failed to allocate Metal buffers for polygon geometry");
      throw std::runtime_error("Metal buffer allocation failed");
    }

    num_polygons_ = static_cast<uint32_t>(host_polygons_.size());
    allocated_bytes_ = poly_size + part_size + ring_size + vert_size;

    // Free host geometry vectors to release resident host memory
    host_polygons_.clear();
    host_polygons_.shrink_to_fit();
    host_parts_.clear();
    host_parts_.shrink_to_fit();
    host_rings_.clear();
    host_rings_.shrink_to_fit();
    host_vertices_.clear();
    host_vertices_.shrink_to_fit();

    is_built_ = true;
  }
}

void MetalSpatialRefiner::refine(const DecomposedPoint* points, uint32_t point_count,
                                 const uint32_t* candidate_build_indices,
                                 const uint32_t* candidate_probe_indices,
                                 uint32_t candidate_count, uint8_t* out_states) {
  if (candidate_count == 0) return;

  if (!is_built_) {
    set_error("Refiner::finish_building() must be called before refine()");
    throw std::runtime_error("Refiner not built");
  }

  @autoreleasepool {
    size_t pts_size = std::max(point_count * sizeof(DecomposedPoint), (size_t)16);
    id<MTLBuffer> buf_points =
        [device_ newBufferWithBytes:point_count == 0
                                        ? (const void*)"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
                                        : points
                             length:pts_size
                            options:MTLResourceStorageModeShared];
    if (!buf_points) {
      set_error("Failed to allocate Metal buffer for probe points");
      throw std::runtime_error("Buffer allocation failed");
    }

    uint32_t num_polygons = num_polygons_;
    const uint32_t CHUNK_SIZE = 65536;

    for (uint32_t offset = 0; offset < candidate_count; offset += CHUNK_SIZE) {
      @autoreleasepool {
        uint32_t chunk_len = std::min(CHUNK_SIZE, candidate_count - offset);

        // Build chunk CandidatePairs
        std::vector<CandidatePair> chunk_pairs(chunk_len);
        for (uint32_t i = 0; i < chunk_len; ++i) {
          chunk_pairs[i] = CandidatePair{candidate_build_indices[offset + i],
                                         candidate_probe_indices[offset + i]};
        }

        id<MTLBuffer> buf_candidates =
            [device_ newBufferWithBytes:chunk_pairs.data()
                                 length:chunk_len * sizeof(CandidatePair)
                                options:MTLResourceStorageModeShared];

        id<MTLBuffer> buf_states =
            [device_ newBufferWithLength:chunk_len * sizeof(uint8_t)
                                 options:MTLResourceStorageModeShared];

        if (!buf_candidates || !buf_states) {
          set_error("Failed to allocate candidate buffers for chunk");
          throw std::runtime_error("Chunk buffer allocation failed");
        }

        id<MTLCommandBuffer> cmd_buffer = [command_queue_ commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [cmd_buffer computeCommandEncoder];

        [encoder setComputePipelineState:pipeline_state_];
        [encoder setBuffer:buf_candidates offset:0 atIndex:0];
        [encoder setBuffer:buf_polygons_ offset:0 atIndex:1];
        [encoder setBuffer:buf_parts_ offset:0 atIndex:2];
        [encoder setBuffer:buf_rings_ offset:0 atIndex:3];
        [encoder setBuffer:buf_vertices_ offset:0 atIndex:4];
        [encoder setBuffer:buf_points offset:0 atIndex:5];
        [encoder setBuffer:buf_states offset:0 atIndex:6];
        [encoder setBytes:&chunk_len length:sizeof(uint32_t) atIndex:7];
        [encoder setBytes:&num_polygons length:sizeof(uint32_t) atIndex:8];

        NSUInteger max_threads = pipeline_state_.maxTotalThreadsPerThreadgroup;
        NSUInteger tg_size = std::min((NSUInteger)256, max_threads);
        MTLSize threadgroups = MTLSizeMake((chunk_len + tg_size - 1) / tg_size, 1, 1);
        MTLSize threads_per_tg = MTLSizeMake(tg_size, 1, 1);

        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threads_per_tg];
        [encoder endEncoding];

        [cmd_buffer commit];
        [cmd_buffer waitUntilCompleted];

        if (cmd_buffer.status != MTLCommandBufferStatusCompleted) {
          std::string err_desc =
              cmd_buffer.error ? [[cmd_buffer.error localizedDescription] UTF8String]
                               : "Unknown GPU error";
          set_error("Refine kernel execution failed: " + err_desc);
          throw std::runtime_error("GPU execution failed");
        }

        const uint8_t* states_ptr = (const uint8_t*)buf_states.contents;
        std::memcpy(out_states + offset, states_ptr, chunk_len * sizeof(uint8_t));
      }
    }
  }
}

#ifdef ENABLE_TEST_INTERNALS
// --- PROTOTYPE: exact second-stage resolver (measurement only) ---

void MetalSpatialRefiner::finish_exact(const FixedVertex* vertices, uint32_t vertex_count,
                                       const uint32_t* poly_exact_ok, uint32_t poly_count) {
  @autoreleasepool {
    if (!is_built_) {
      set_error("finish_building() must be called before finish_exact()");
      throw std::runtime_error("Refiner not built");
    }

    if (!exact_pipeline_) {
      // The exact pass never touches a float, so its result does not depend on
      // the math mode; verified by running the correctness gates under
      // MTLMathModeFast. Safe is kept to match the production pipeline.
      MTLCompileOptions* options = [[MTLCompileOptions alloc] init];
      if (@available(macOS 15.0, *)) {
        options.mathMode = MTLMathModeSafe;
      }
      NSError* error = nil;
      NSString* src = [NSString stringWithUTF8String:REFINE_EXACT_METAL_SOURCE];
      id<MTLLibrary> library = [device_ newLibraryWithSource:src options:options error:&error];
      if (!library) {
        std::string err_str =
            error ? [[error localizedDescription] UTF8String] : "Unknown shader error";
        set_error("Failed to compile refine_exact.metal: " + err_str);
        throw std::runtime_error("Failed to compile refine_exact.metal: " + err_str);
      }
      id<MTLFunction> kernel_fn = [library newFunctionWithName:@"point_in_polygon_exact"];
      if (!kernel_fn) {
        set_error("Function point_in_polygon_exact not found");
        throw std::runtime_error("Function point_in_polygon_exact not found");
      }
      exact_pipeline_ = [device_ newComputePipelineStateWithFunction:kernel_fn error:&error];
      if (!exact_pipeline_) {
        std::string err_str =
            error ? [[error localizedDescription] UTF8String] : "Unknown pipeline error";
        set_error("Failed to create exact pipeline state: " + err_str);
        throw std::runtime_error("Failed to create exact pipeline state: " + err_str);
      }
    }

    static const char kZero[16] = {0};
    size_t vert_size = std::max(vertex_count * sizeof(FixedVertex), (size_t)16);
    size_t ok_size = std::max(poly_count * sizeof(uint32_t), (size_t)16);

    buf_exact_vertices_ =
        [device_ newBufferWithBytes:vertex_count == 0 ? (const void*)kZero : (const void*)vertices
                             length:vert_size
                            options:MTLResourceStorageModeShared];
    buf_exact_poly_ok_ = [device_
        newBufferWithBytes:poly_count == 0 ? (const void*)kZero : (const void*)poly_exact_ok
                    length:ok_size
                   options:MTLResourceStorageModeShared];

    if (!buf_exact_vertices_ || !buf_exact_poly_ok_) {
      set_error("Failed to allocate Metal buffers for exact geometry");
      throw std::runtime_error("Exact buffer allocation failed");
    }
    exact_allocated_bytes_ = vert_size + ok_size;
  }
}

void MetalSpatialRefiner::refine_exact(const FixedProbe* points, uint32_t point_count,
                                       const uint32_t* candidate_build_indices,
                                       const uint32_t* candidate_probe_indices,
                                       uint32_t candidate_count, uint8_t* out_states) {
  if (candidate_count == 0) return;
  if (!exact_pipeline_ || !buf_exact_vertices_) {
    set_error("finish_exact() must be called before refine_exact()");
    throw std::runtime_error("Exact stage not built");
  }

  @autoreleasepool {
    static const char kZero[16] = {0};
    size_t pts_size = std::max(point_count * sizeof(FixedProbe), (size_t)16);
    id<MTLBuffer> buf_points =
        [device_ newBufferWithBytes:point_count == 0 ? (const void*)kZero : (const void*)points
                             length:pts_size
                            options:MTLResourceStorageModeShared];
    if (!buf_points) {
      set_error("Failed to allocate Metal buffer for exact probe points");
      throw std::runtime_error("Buffer allocation failed");
    }

    uint32_t num_polygons = num_polygons_;
    const uint32_t CHUNK_SIZE = 65536;

    for (uint32_t offset = 0; offset < candidate_count; offset += CHUNK_SIZE) {
      @autoreleasepool {
        uint32_t chunk_len = std::min(CHUNK_SIZE, candidate_count - offset);

        std::vector<CandidatePair> chunk_pairs(chunk_len);
        for (uint32_t i = 0; i < chunk_len; ++i) {
          chunk_pairs[i] = CandidatePair{candidate_build_indices[offset + i],
                                         candidate_probe_indices[offset + i]};
        }

        id<MTLBuffer> buf_candidates =
            [device_ newBufferWithBytes:chunk_pairs.data()
                                 length:chunk_len * sizeof(CandidatePair)
                                options:MTLResourceStorageModeShared];
        id<MTLBuffer> buf_states =
            [device_ newBufferWithLength:chunk_len * sizeof(uint8_t)
                                 options:MTLResourceStorageModeShared];
        if (!buf_candidates || !buf_states) {
          set_error("Failed to allocate exact candidate buffers for chunk");
          throw std::runtime_error("Chunk buffer allocation failed");
        }

        id<MTLCommandBuffer> cmd_buffer = [command_queue_ commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [cmd_buffer computeCommandEncoder];

        [encoder setComputePipelineState:exact_pipeline_];
        [encoder setBuffer:buf_candidates offset:0 atIndex:0];
        [encoder setBuffer:buf_polygons_ offset:0 atIndex:1];
        [encoder setBuffer:buf_parts_ offset:0 atIndex:2];
        [encoder setBuffer:buf_rings_ offset:0 atIndex:3];
        [encoder setBuffer:buf_exact_vertices_ offset:0 atIndex:4];
        [encoder setBuffer:buf_points offset:0 atIndex:5];
        [encoder setBuffer:buf_exact_poly_ok_ offset:0 atIndex:6];
        [encoder setBuffer:buf_states offset:0 atIndex:7];
        [encoder setBytes:&chunk_len length:sizeof(uint32_t) atIndex:8];
        [encoder setBytes:&num_polygons length:sizeof(uint32_t) atIndex:9];

        NSUInteger max_threads = exact_pipeline_.maxTotalThreadsPerThreadgroup;
        NSUInteger tg_size = std::min((NSUInteger)256, max_threads);
        MTLSize threadgroups = MTLSizeMake((chunk_len + tg_size - 1) / tg_size, 1, 1);
        MTLSize threads_per_tg = MTLSizeMake(tg_size, 1, 1);

        [encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threads_per_tg];
        [encoder endEncoding];
        [cmd_buffer commit];
        [cmd_buffer waitUntilCompleted];

        if (cmd_buffer.status != MTLCommandBufferStatusCompleted) {
          std::string err_desc =
              cmd_buffer.error ? [[cmd_buffer.error localizedDescription] UTF8String]
                               : "Unknown GPU error";
          set_error("Exact kernel execution failed: " + err_desc);
          throw std::runtime_error("GPU execution failed");
        }

        std::memcpy(out_states + offset, (const uint8_t*)buf_states.contents,
                    chunk_len * sizeof(uint8_t));
      }
    }
  }
}
#endif  // ENABLE_TEST_INTERNALS
