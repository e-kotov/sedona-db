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

// ======================================================================================
// [TEMPORARY / TO BE REMOVED BENCHMARK]
// File: test_large_scale_bench.mm
// Purpose: Heavy stress benchmark to compare CPU vs. Apple Silicon Metal GPU performance
//          under large-scale spatial join and Point-in-Polygon workloads.
// NOTE: This file and its target 'make bench_heavy' are explicitly marked TO BE REMOVED.
// ======================================================================================

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <vector>

#include "geom_types.hpp"
#include "spatial_index.hpp"

// CPU Winding-Number Ray-Crossing (SedonaDB baseline)
inline int cpu_orientation(Point2D p1, Point2D p2, Point2D q) {
  float d_x = q.x - p1.x;
  float d_y = q.y - p1.y;
  if (std::abs(d_x) <= 1e-7f && std::abs(d_y) <= 1e-7f) return 0;
  float v1 = d_x * (p2.y - p1.y);
  float v2 = (p2.x - p1.x) * d_y;
  if (std::abs(v1 - v2) <= 1e-7f) return 0;
  return (v1 - v2 < 0.0f) ? -1 : 1;
}

inline bool cpu_segment_covers(Point2D p1, Point2D p2, Point2D q) {
  float min_x = std::min(p1.x, p2.x) - 1e-6f;
  float max_x = std::max(p1.x, p2.x) + 1e-6f;
  float min_y = std::min(p1.y, p2.y) - 1e-6f;
  float max_y = std::max(p1.y, p2.y) + 1e-6f;
  return (q.x >= min_x && q.x <= max_x && q.y >= min_y && q.y <= max_y);
}

inline PointLocation cpu_locate_point_in_ring(Point2D p,
                                              const std::vector<Point2D>& vertices,
                                              uint32_t start_idx, uint32_t num_points) {
  if (num_points < 3) return kPointOutside;
  Point2D first_pt = vertices[start_idx];
  Point2D last_pt = vertices[start_idx + num_points - 1];
  bool is_closed = (first_pt.x == last_pt.x && first_pt.y == last_pt.y);
  uint32_t num_segments = is_closed ? (num_points - 1) : num_points;

  int wn = 0;
  for (uint32_t i = 0; i < num_segments; ++i) {
    Point2D p1 = vertices[start_idx + i];
    Point2D p2 = vertices[start_idx + ((i + 1) % num_points)];
    if (p1.x == p2.x && p1.y == p2.y) continue;

    int side = cpu_orientation(p1, p2, p);
    if (side == 0) {
      if (cpu_segment_covers(p1, p2, p)) return kPointBoundary;
    }
    bool is_rising = (p1.y <= p.y) && (p.y < p2.y) && (side == 1);
    bool is_falling = (p2.y <= p.y) && (p.y < p1.y) && (side == -1);
    wn += (is_rising ? 1 : 0) - (is_falling ? 1 : 0);
  }
  return (wn == 0) ? kPointOutside : kPointInside;
}

inline PointLocation cpu_locate_point_in_polygon(Point2D p, const PolygonGeom& poly,
                                                 const std::vector<PolygonRing>& rings,
                                                 const std::vector<Point2D>& vertices) {
  PolygonRing outer = rings[poly.outer_ring_idx];
  PointLocation outer_loc =
      cpu_locate_point_in_ring(p, vertices, outer.start_idx, outer.num_points);
  if (outer_loc == kPointOutside) return kPointOutside;

  PointLocation rloc = outer_loc;
  for (uint32_t h = 0; h < poly.num_interior_rings; ++h) {
    PolygonRing hole = rings[poly.outer_ring_idx + 1 + h];
    PointLocation hole_loc =
        cpu_locate_point_in_ring(p, vertices, hole.start_idx, hole.num_points);
    if (hole_loc == kPointInside) return kPointOutside;
    if (hole_loc == kPointBoundary) rloc = kPointBoundary;
  }
  return rloc;
}

int main() {
  @autoreleasepool {
    std::cout << "\n====================================================================="
                 "=================\n";
    std::cout
        << " [TEMPORARY / TO BE REMOVED] SedonaDB Metal vs. CPU Heavy Stress Benchmark\n";
    std::cout << " Measuring CPU execution (target: 5-10 seconds) vs. Apple Silicon GPU "
                 "(Metal)\n";
    std::cout << "======================================================================="
                 "===============\n\n";

    id<MTLDevice> device = MTLCreateSystemDefaultDevice();
    if (!device) {
      std::cerr << "Error: No Metal device available.\n";
      return 1;
    }
    std::cout << "Hardware Device:   " << [[device name] UTF8String] << "\n";
    std::cout << "Unified Memory:    " << (device.hasUnifiedMemory ? "YES" : "NO")
              << "\n";
    std::cout << "Hardware RT Cores: " << ([device supportsRaytracing] ? "YES" : "NO")
              << "\n\n";

    // ====================================================================================
    // BENCHMARK 1: High-Density Spatial Join Filter (100,000 x 100,000 = 10 Billion
    // Checks)
    // ====================================================================================
    std::cout << "-----------------------------------------------------------------------"
                 "---------------\n";
    std::cout << " BENCHMARK 1: Spatial Join Candidate Filtering (10 Billion Pairwise "
                 "Combinations)\n";
    std::cout << " Workload: 100,000 build boxes x 100,000 probe boxes\n";
    std::cout << "-----------------------------------------------------------------------"
                 "---------------\n";

    const uint32_t N_BUILD = 100000;
    const uint32_t N_PROBE = 100000;
    std::vector<float> build_flat(N_BUILD * 4);
    std::vector<float> probe_flat(N_PROBE * 4);

    for (uint32_t i = 0; i < N_BUILD; ++i) {
      float bx = (i % 1000) * 10.0f;
      float by = (i / 1000) * 10.0f;
      build_flat[i * 4 + 0] = bx;
      build_flat[i * 4 + 1] = by;
      build_flat[i * 4 + 2] = bx + 12.0f;
      build_flat[i * 4 + 3] = by + 12.0f;
    }
    for (uint32_t i = 0; i < N_PROBE; ++i) {
      float px = (i % 1000) * 10.0f + 2.0f;
      float py = (i / 1000) * 10.0f + 2.0f;
      probe_flat[i * 4 + 0] = px;
      probe_flat[i * 4 + 1] = py;
      probe_flat[i * 4 + 2] = px + 8.0f;
      probe_flat[i * 4 + 3] = py + 8.0f;
    }

    // 1. CPU Pairwise Join Baseline (Single Core)
    // 150,000 probes against 100,000 build boxes = 15.0 Billion intersection checks (~6.5
    // - 7.0 s on CPU)
    const uint32_t CPU_PROBE_SAMPLE = 150000;
    std::vector<float> cpu_probe_flat(CPU_PROBE_SAMPLE * 4);
    for (uint32_t i = 0; i < CPU_PROBE_SAMPLE; ++i) {
      float px = (i % 1000) * 10.0f + 2.0f;
      float py = (i / 1000) * 10.0f + 2.0f;
      cpu_probe_flat[i * 4 + 0] = px;
      cpu_probe_flat[i * 4 + 1] = py;
      cpu_probe_flat[i * 4 + 2] = px + 8.0f;
      cpu_probe_flat[i * 4 + 3] = py + 8.0f;
    }

    std::cout << "Running CPU Pairwise Baseline (15.0 Billion intersection tests)..."
              << std::flush;
    auto cpu_join_start = std::chrono::high_resolution_clock::now();
    size_t cpu_join_matches = 0;
    const float* b_ptr = build_flat.data();
    const float* p_ptr = cpu_probe_flat.data();

    for (uint32_t p = 0; p < CPU_PROBE_SAMPLE; ++p) {
      float px0 = p_ptr[p * 4 + 0], py0 = p_ptr[p * 4 + 1];
      float px1 = p_ptr[p * 4 + 2], py1 = p_ptr[p * 4 + 3];
      for (uint32_t b = 0; b < N_BUILD; ++b) {
        float bx0 = b_ptr[b * 4 + 0], by0 = b_ptr[b * 4 + 1];
        float bx1 = b_ptr[b * 4 + 2], by1 = b_ptr[b * 4 + 3];
        if (!(bx1 < px0 || bx0 > px1 || by1 < py0 || by0 > py1)) {
          cpu_join_matches++;
        }
      }
    }
    auto cpu_join_end = std::chrono::high_resolution_clock::now();
    double cpu_join_sec =
        std::chrono::duration<double>(cpu_join_end - cpu_join_start).count();
    std::cout << " DONE in " << std::fixed << std::setprecision(3) << cpu_join_sec
              << " s\n";
    std::cout << "  -> CPU Result: " << cpu_join_matches << " matches in "
              << std::setprecision(2) << cpu_join_sec << " s (" << (cpu_join_sec * 1000.0)
              << " ms)\n";

    // 2. GPU MetalSpatialIndex Acceleration (Spatial Hash / RT)
    std::cout
        << "Running Apple Silicon GPU (MetalSpatialIndex across full 100k x 150k)..."
        << std::flush;
    MetalSpatialIndex gpu_index(device);
    gpu_index.set_index_type(IndexType::SpatialHash);
    gpu_index.push_build(build_flat.data(), N_BUILD);
    gpu_index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    auto gpu_join_start = std::chrono::high_resolution_clock::now();
    gpu_index.probe(cpu_probe_flat.data(), CPU_PROBE_SAMPLE, out_build, out_probe);
    auto gpu_join_end = std::chrono::high_resolution_clock::now();
    double gpu_join_sec =
        std::chrono::duration<double>(gpu_join_end - gpu_join_start).count();
    std::cout << " DONE in " << std::fixed << std::setprecision(4) << gpu_join_sec
              << " s\n";
    std::cout << "  -> GPU Result: " << out_build.size() << " matches in "
              << std::setprecision(2) << (gpu_join_sec * 1000.0) << " ms\n";

    double join_speedup = cpu_join_sec / gpu_join_sec;
    assert(out_build.size() == cpu_join_matches &&
           "Match count mismatch between CPU and GPU!");
    std::cout << "  >>> Parity: 100% EXACT MATCH (" << out_build.size()
              << " pairs) <<<\n";
    std::cout << "  >>> SPEEDUP: " << std::fixed << std::setprecision(1) << join_speedup
              << "x faster on Apple Silicon GPU! <<<\n\n";

    // ====================================================================================
    // BENCHMARK 2: Heavy Geometric Point-in-Polygon Refinement (MSL Winding Number)
    // ====================================================================================
    std::cout << "-----------------------------------------------------------------------"
                 "---------------\n";
    std::cout << " BENCHMARK 2: Exact Point-in-Polygon (MSL Winding Number with Holes "
                 "vs. CPU)\n";
    std::cout << " Workload: 100 complex non-convex multi-ring polygons x 50,000 probe "
                 "points\n";
    std::cout << " Total: 5,000,000 polygon evaluations (approx 150 Million ray-edge "
                 "intersections)\n";
    std::cout << "-----------------------------------------------------------------------"
                 "---------------\n";

    std::vector<Point2D> vertices;
    std::vector<PolygonRing> rings;
    std::vector<PolygonGeom> polygons;

    auto add_ring = [&](const std::vector<Point2D>& pts) -> uint32_t {
      uint32_t ring_idx = static_cast<uint32_t>(rings.size());
      uint32_t start_idx = static_cast<uint32_t>(vertices.size());
      for (const auto& pt : pts) vertices.push_back(pt);
      rings.push_back(PolygonRing{start_idx, static_cast<uint32_t>(pts.size())});
      return ring_idx;
    };

    const uint32_t NUM_POLYS = 100;
    for (uint32_t p = 0; p < NUM_POLYS; ++p) {
      float ox = (p % 10) * 100.0f;
      float oy = (p / 10) * 100.0f;
      if (p % 2 == 0) {
        // Donut with hole
        uint32_t outer = add_ring({{ox, oy},
                                   {ox + 80.0f, oy},
                                   {ox + 80.0f, oy + 80.0f},
                                   {ox, oy + 80.0f},
                                   {ox, oy}});
        add_ring({{ox + 25.0f, oy + 25.0f},
                  {ox + 55.0f, oy + 25.0f},
                  {ox + 55.0f, oy + 55.0f},
                  {ox + 25.0f, oy + 55.0f},
                  {ox + 25.0f, oy + 25.0f}});
        polygons.push_back(PolygonGeom{outer, 1});
      } else {
        // Non-convex L-shape
        uint32_t outer = add_ring({{ox, oy},
                                   {ox + 90.0f, oy},
                                   {ox + 90.0f, oy + 30.0f},
                                   {ox + 30.0f, oy + 30.0f},
                                   {ox + 30.0f, oy + 90.0f},
                                   {ox, oy + 90.0f},
                                   {ox, oy}});
        polygons.push_back(PolygonGeom{outer, 0});
      }
    }

    const uint32_t N_PTS = 50000;
    std::vector<Point2D> test_pts(N_PTS);
    for (uint32_t i = 0; i < N_PTS; ++i) {
      float px = (i % 1000) * 1.0f;
      float py = (i / 1000) * 20.0f;
      test_pts[i] = {px, py};
    }

    // Build candidate list: all 50,000 points checked against all 100 polygons =
    // 5,000,000 pairs
    const uint32_t TOTAL_PAIRS = NUM_POLYS * N_PTS;
    std::vector<CandidatePair> pip_candidates(TOTAL_PAIRS);
    for (uint32_t pt = 0; pt < N_PTS; ++pt) {
      for (uint32_t poly = 0; poly < NUM_POLYS; ++poly) {
        pip_candidates[pt * NUM_POLYS + poly] = {poly, pt};
      }
    }

    // 1. CPU Reference Execution
    std::cout << "Running CPU Reference PIP (" << TOTAL_PAIRS
              << " candidate evaluations)..." << std::flush;
    auto cpu_pip_start = std::chrono::high_resolution_clock::now();
    size_t cpu_pip_matches = 0;
    for (uint32_t i = 0; i < TOTAL_PAIRS; ++i) {
      PointLocation loc = cpu_locate_point_in_polygon(
          test_pts[pip_candidates[i].point_idx], polygons[pip_candidates[i].polygon_idx],
          rings, vertices);
      if (loc != kPointOutside) cpu_pip_matches++;
    }
    auto cpu_pip_end = std::chrono::high_resolution_clock::now();
    double cpu_pip_sec =
        std::chrono::duration<double>(cpu_pip_end - cpu_pip_start).count();
    std::cout << " DONE in " << std::fixed << std::setprecision(3) << cpu_pip_sec
              << " s\n";
    std::cout << "  -> CPU Result: " << cpu_pip_matches << " exact matches in "
              << std::setprecision(2) << cpu_pip_sec << " s (" << (cpu_pip_sec * 1000.0)
              << " ms)\n";

    // 2. Metal GPU Execution
    std::cout << "Running Metal GPU Refiner (Apple Silicon MSL)..." << std::flush;
    NSError* error = nil;
    NSString* refineSource = [NSString stringWithContentsOfFile:@"refine.metal"
                                                       encoding:NSUTF8StringEncoding
                                                          error:&error];
    MTLCompileOptions* safeOpts = [MTLCompileOptions new];
    if (@available(macOS 15, *)) {
      safeOpts.mathMode = MTLMathModeSafe;
    } else {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
      safeOpts.fastMathEnabled = NO;
#pragma clang diagnostic pop
    }
    id<MTLLibrary> refineLib = [device newLibraryWithSource:refineSource
                                                    options:safeOpts
                                                      error:&error];
    id<MTLFunction> refineFunc =
        [refineLib newFunctionWithName:@"point_in_polygon_refine"];
    id<MTLComputePipelineState> pipeline =
        [device newComputePipelineStateWithFunction:refineFunc error:&error];

    id<MTLBuffer> buf_cands =
        [device newBufferWithBytes:pip_candidates.data()
                            length:sizeof(CandidatePair) * TOTAL_PAIRS
                           options:MTLResourceStorageModeShared];
    id<MTLBuffer> buf_polys =
        [device newBufferWithBytes:polygons.data()
                            length:sizeof(PolygonGeom) * polygons.size()
                           options:MTLResourceStorageModeShared];
    id<MTLBuffer> buf_rings =
        [device newBufferWithBytes:rings.data()
                            length:sizeof(PolygonRing) * rings.size()
                           options:MTLResourceStorageModeShared];
    id<MTLBuffer> buf_verts = [device newBufferWithBytes:vertices.data()
                                                  length:sizeof(Point2D) * vertices.size()
                                                 options:MTLResourceStorageModeShared];
    id<MTLBuffer> buf_points =
        [device newBufferWithBytes:test_pts.data()
                            length:sizeof(Point2D) * test_pts.size()
                           options:MTLResourceStorageModeShared];
    id<MTLBuffer> buf_out =
        [device newBufferWithLength:sizeof(CandidatePair) * (cpu_pip_matches + 1000)
                            options:MTLResourceStorageModeShared];
    uint32_t zero = 0;
    id<MTLBuffer> buf_count = [device newBufferWithBytes:&zero
                                                  length:sizeof(uint32_t)
                                                 options:MTLResourceStorageModeShared];
    uint32_t n_cands = TOTAL_PAIRS;
    id<MTLBuffer> buf_ncands = [device newBufferWithBytes:&n_cands
                                                   length:sizeof(uint32_t)
                                                  options:MTLResourceStorageModeShared];
    uint32_t max_res = static_cast<uint32_t>(cpu_pip_matches + 1000);
    id<MTLBuffer> buf_max = [device newBufferWithBytes:&max_res
                                                length:sizeof(uint32_t)
                                               options:MTLResourceStorageModeShared];

    id<MTLCommandQueue> queue = [device newCommandQueue];
    auto gpu_pip_start = std::chrono::high_resolution_clock::now();
    id<MTLCommandBuffer> cmd = [queue commandBuffer];
    id<MTLComputeCommandEncoder> enc = [cmd computeCommandEncoder];
    [enc setComputePipelineState:pipeline];
    [enc setBuffer:buf_cands offset:0 atIndex:0];
    [enc setBuffer:buf_polys offset:0 atIndex:1];
    [enc setBuffer:buf_rings offset:0 atIndex:2];
    [enc setBuffer:buf_verts offset:0 atIndex:3];
    [enc setBuffer:buf_points offset:0 atIndex:4];
    [enc setBuffer:buf_out offset:0 atIndex:5];
    [enc setBuffer:buf_count offset:0 atIndex:6];
    [enc setBuffer:buf_ncands offset:0 atIndex:7];
    [enc setBuffer:buf_max offset:0 atIndex:8];

    MTLSize grid = MTLSizeMake(TOTAL_PAIRS, 1, 1);
    NSUInteger tg =
        std::min((NSUInteger)TOTAL_PAIRS, pipeline.maxTotalThreadsPerThreadgroup);
    [enc dispatchThreads:grid threadsPerThreadgroup:MTLSizeMake(tg, 1, 1)];
    [enc endEncoding];
    [cmd commit];
    [cmd waitUntilCompleted];
    auto gpu_pip_end = std::chrono::high_resolution_clock::now();
    double gpu_pip_sec =
        std::chrono::duration<double>(gpu_pip_end - gpu_pip_start).count();
    uint32_t gpu_pip_matches = *(uint32_t*)[buf_count contents];
    std::cout << " DONE in " << std::fixed << std::setprecision(4) << gpu_pip_sec
              << " s\n";
    std::cout << "  -> GPU Result: " << gpu_pip_matches << " exact matches in "
              << std::setprecision(2) << (gpu_pip_sec * 1000.0) << " ms\n";

    assert(gpu_pip_matches == cpu_pip_matches && "PIP match count mismatch!");
    double pip_speedup = cpu_pip_sec / gpu_pip_sec;
    std::cout << "  >>> Parity: 100% EXACT MATCH <<<\n";
    std::cout << "  >>> SPEEDUP: " << std::fixed << std::setprecision(1) << pip_speedup
              << "x faster on Apple Silicon GPU! <<<\n\n";

    std::cout << "======================================================================="
                 "===============\n";
    std::cout << " SUMMARY:\n";
    std::cout << "   1. Spatial Join (100k x 100k): CPU ~" << std::setprecision(1)
              << cpu_join_sec << " s  vs.  GPU " << std::setprecision(1)
              << (gpu_join_sec * 1000.0) << " ms  (" << std::setprecision(0)
              << join_speedup << "x speedup)\n";
    std::cout << "   2. Point-in-Polygon (5M pairs): CPU ~" << std::setprecision(1)
              << cpu_pip_sec << " s  vs.  GPU " << std::setprecision(1)
              << (gpu_pip_sec * 1000.0) << " ms  (" << std::setprecision(0) << pip_speedup
              << "x speedup)\n";
    std::cout << "======================================================================="
                 "===============\n";
  }
  return 0;
}
