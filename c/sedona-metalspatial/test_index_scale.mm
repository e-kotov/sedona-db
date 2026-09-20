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

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <dispatch/dispatch.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <iostream>
#include <random>
#include <set>
#include <vector>
#import "spatial_index.hpp"

// =====================================================================
// CPU Reference Implementations
// =====================================================================

inline bool cpu_boxes_intersect(const BoundingBox& a, const BoundingBox& b) {
  return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
}

inline bool cpu_is_valid_box(const BoundingBox& b) {
  return !(std::isnan(b.xmin) || std::isnan(b.ymin) || std::isnan(b.xmax) ||
           std::isnan(b.ymax) || std::isinf(b.xmin) || std::isinf(b.ymin) ||
           std::isinf(b.xmax) || std::isinf(b.ymax) || b.xmin > b.xmax ||
           b.ymin > b.ymax);
}

// Naive O(N * M) CPU ground truth (used for sanity and moderate tests)
std::set<std::pair<uint32_t, uint32_t>> cpu_naive_join(
    const std::vector<BoundingBox>& build, const std::vector<BoundingBox>& probe) {
  std::set<std::pair<uint32_t, uint32_t>> matches;
  for (uint32_t p = 0; p < probe.size(); ++p) {
    for (uint32_t b = 0; b < build.size(); ++b) {
      if (cpu_boxes_intersect(build[b], probe[p])) {
        matches.insert({b, p});
      }
    }
  }
  return matches;
}

// Fast CPU Spatial Grid ground truth for 50k x 50k scale verification
std::set<std::pair<uint32_t, uint32_t>> cpu_grid_join(
    const std::vector<BoundingBox>& build, const std::vector<BoundingBox>& probe,
    float min_x, float min_y, float max_x, float max_y) {
  uint32_t G = 256;
  float cw = (max_x - min_x + 2.0f) / G;
  float ch = (max_y - min_y + 2.0f) / G;

  std::vector<std::vector<uint32_t>> grid(G * G);
  for (uint32_t i = 0; i < build.size(); ++i) {
    int x0 = std::clamp(int((build[i].xmin - min_x) / cw), 0, int(G - 1));
    int x1 = std::clamp(int((build[i].xmax - min_x) / cw), 0, int(G - 1));
    int y0 = std::clamp(int((build[i].ymin - min_y) / ch), 0, int(G - 1));
    int y1 = std::clamp(int((build[i].ymax - min_y) / ch), 0, int(G - 1));
    for (int y = y0; y <= y1; ++y) {
      for (int x = x0; x <= x1; ++x) {
        grid[y * G + x].push_back(i);
      }
    }
  }

  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> thread_results(probe.size());
  auto* p_results = thread_results.data();

  dispatch_apply(
      probe.size(), dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^(size_t p) {
        int px0 = std::clamp(int((probe[p].xmin - min_x) / cw), 0, int(G - 1));
        int px1 = std::clamp(int((probe[p].xmax - min_x) / cw), 0, int(G - 1));
        int py0 = std::clamp(int((probe[p].ymin - min_y) / ch), 0, int(G - 1));
        int py1 = std::clamp(int((probe[p].ymax - min_y) / ch), 0, int(G - 1));
        for (int y = py0; y <= py1; ++y) {
          for (int x = px0; x <= px1; ++x) {
            for (uint32_t b : grid[y * G + x]) {
              if (cpu_boxes_intersect(build[b], probe[p])) {
                int bx0 = std::clamp(int((build[b].xmin - min_x) / cw), 0, int(G - 1));
                int by0 = std::clamp(int((build[b].ymin - min_y) / ch), 0, int(G - 1));
                if (x == std::max(px0, bx0) && y == std::max(py0, by0)) {
                  p_results[p].push_back({b, static_cast<uint32_t>(p)});
                }
              }
            }
          }
        }
      });

  std::set<std::pair<uint32_t, uint32_t>> matches;
  for (const auto& vec : thread_results) {
    for (const auto& pair : vec) {
      matches.insert(pair);
    }
  }
  return matches;
}

// =====================================================================
// Test 1: Sanity and Edge Cases (Multi-query)
// =====================================================================
void test_sanity_cases(id<MTLDevice> device) {
  std::cout << "\n--- Test 1: Sanity & Edge Cases (Multi-Query) ---\n";

  std::vector<BoundingBox> build = {
      {0.0f, 0.0f, 10.0f, 10.0f},    // 0: [0,0] to [10,10]
      {10.0f, 0.0f, 20.0f, 10.0f},   // 1: Adjacent touching right edge of 0
      {0.0f, 10.0f, 10.0f, 20.0f},   // 2: Adjacent touching top edge of 0
      {10.0f, 10.0f, 20.0f, 20.0f},  // 3: Corner touching (10, 10)
      {50.0f, 50.0f, 60.0f, 60.0f},  // 4: Disjoint
      {2.0f, 2.0f, 8.0f, 8.0f}       // 5: Contained completely inside 0
  };

  std::vector<BoundingBox> probe = {
      {10.0f, 10.0f, 10.0f, 10.0f},      // 0: Point touching corner of 0, 1, 2, 3
      {5.0f, 5.0f, 5.0f, 5.0f},          // 1: Point inside 0 and 5
      {55.0f, 55.0f, 55.0f, 55.0f},      // 2: Point inside 4
      {100.0f, 100.0f, 100.0f, 100.0f},  // 3: Disjoint point
      {0.0f, 0.0f, 20.0f, 20.0f}         // 4: Big box covering 0, 1, 2, 3, 5
  };

  auto cpu_truth = cpu_naive_join(build, probe);

  // Test with MetalSpatialIndex (Spatial Hash)
  {
    MetalSpatialIndex index(device);
    index.set_index_type(IndexType::SpatialHash);
    index.push_build(reinterpret_cast<const float*>(build.data()), build.size());
    index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    index.probe(reinterpret_cast<const float*>(probe.data()), probe.size(), out_build,
                out_probe);

    assert(out_build.size() == out_probe.size());
    std::set<std::pair<uint32_t, uint32_t>> gpu_matches;
    for (size_t i = 0; i < out_build.size(); ++i) {
      gpu_matches.insert({out_build[i], out_probe[i]});
    }

    assert(gpu_matches.size() == cpu_truth.size());
    assert(gpu_matches == cpu_truth);
    std::cout << "  Spatial Hash Sanity: PASS (exact " << gpu_matches.size()
              << " matches)\n";
  }

  // Test with MetalSpatialIndex (Hardware RT for point probes)
  if ([device supportsRaytracing]) {
    MetalSpatialIndex index(device);
    index.set_index_type(IndexType::HardwareRT);
    index.push_build(reinterpret_cast<const float*>(build.data()), build.size());
    index.finish_building();

    // Use points subset (probes 0, 1, 2, 3)
    std::vector<BoundingBox> point_probes(probe.begin(), probe.begin() + 4);
    auto cpu_pt_truth = cpu_naive_join(build, point_probes);

    std::vector<uint32_t> out_build, out_probe;
    index.probe(reinterpret_cast<const float*>(point_probes.data()), point_probes.size(),
                out_build, out_probe);
    assert(index.get_active_index_type() == IndexType::HardwareRT);

    std::set<std::pair<uint32_t, uint32_t>> gpu_matches;
    for (size_t i = 0; i < out_build.size(); ++i) {
      gpu_matches.insert({out_build[i], out_probe[i]});
    }

    assert(gpu_matches.size() == cpu_pt_truth.size());
    assert(gpu_matches == cpu_pt_truth);
    std::cout << "  Hardware RT Sanity:  PASS (exact " << gpu_matches.size()
              << " matches, engine: HardwareRT)\n";
  } else {
    std::cout
        << "  Hardware RT Sanity:  [SKIPPED] (Hardware RT not supported on device)\n";
  }
}

// =====================================================================
// Test 2: Hardware Ray Tracing Scale Test (50,000 build x 50,000 probe)
// =====================================================================
void test_rt_scale(id<MTLDevice> device) {
  std::cout
      << "\n--- Test 2: Hardware Ray Tracing Index (50k Build x 50k Probe Points) ---\n";
  if (![device supportsRaytracing]) {
    std::cout << "  [SKIPPED] Hardware Ray Tracing not supported on this device\n";
    return;
  }

  const uint32_t N_BUILD = 50000;
  const uint32_t N_PROBE = 50000;

  std::mt19937 rng(1337);
  std::uniform_real_distribution<float> pos_dist(0.0f, 1000.0f);
  std::uniform_real_distribution<float> size_dist(0.2f, 3.0f);

  std::vector<BoundingBox> build(N_BUILD);
  float min_x = 1e9f, min_y = 1e9f, max_x = -1e9f, max_y = -1e9f;
  for (uint32_t i = 0; i < N_BUILD; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    float w = size_dist(rng);
    float h = size_dist(rng);
    build[i] = {x, y, x + w, y + h};
    min_x = std::min(min_x, x);
    min_y = std::min(min_y, y);
    max_x = std::max(max_x, x + w);
    max_y = std::max(max_y, y + h);
  }

  std::vector<BoundingBox> probe(N_PROBE);
  for (uint32_t i = 0; i < N_PROBE; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    probe[i] = {x, y, x, y};  // Points as [x, y, x, y]
  }

  // Compute ground truth
  std::cout << "  Computing CPU ground truth for 50k x 50k points...\n";
  auto cpu_truth = cpu_grid_join(build, probe, min_x, min_y, max_x, max_y);
  std::cout << "  CPU ground truth: " << cpu_truth.size() << " matches\n";

  // Run GPU Hardware Ray Tracing Index
  MetalSpatialIndex index(device);
  index.set_index_type(IndexType::HardwareRT);
  index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);

  auto t_b0 = std::chrono::high_resolution_clock::now();
  index.finish_building();
  auto t_b1 = std::chrono::high_resolution_clock::now();
  double build_ms = std::chrono::duration<double, std::milli>(t_b1 - t_b0).count();

  std::vector<uint32_t> out_build, out_probe;
  auto t_p0 = std::chrono::high_resolution_clock::now();
  index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE, out_build,
              out_probe);
  auto t_p1 = std::chrono::high_resolution_clock::now();
  double probe_ms = std::chrono::duration<double, std::milli>(t_p1 - t_p0).count();

  assert(index.get_active_index_type() == IndexType::HardwareRT);

  std::cout << "  GPU RT BVH Build Time: " << build_ms << " ms\n";
  std::cout << "  GPU RT Query Time:     " << probe_ms
            << " ms (Kernel: " << index.get_last_probe_time_ms() << " ms)\n";

  // Exact parity assertion
  std::set<std::pair<uint32_t, uint32_t>> gpu_matches;
  for (size_t i = 0; i < out_build.size(); ++i) {
    gpu_matches.insert({out_build[i], out_probe[i]});
  }

  assert(gpu_matches.size() == cpu_truth.size());
  assert(gpu_matches == cpu_truth);
  std::cout << "  Parity: EXACT MATCH (" << gpu_matches.size()
            << " pairs, active engine: HardwareRT)\n";
}

// =====================================================================
// Test 3: Fast Compute Spatial Hash Scale Test (50,000 build x 50,000 probe)
// =====================================================================
void test_spatial_hash_scale(id<MTLDevice> device) {
  std::cout << "\n--- Test 3: Fast Compute Spatial Hash (50k Build Boxes x 50k Probe "
               "Boxes) ---\n";

  const uint32_t N_BUILD = 50000;
  const uint32_t N_PROBE = 50000;

  std::mt19937 rng(4242);
  std::uniform_real_distribution<float> pos_dist(0.0f, 1000.0f);
  std::uniform_real_distribution<float> size_dist(0.1f, 2.5f);

  std::vector<BoundingBox> build(N_BUILD);
  float min_x = 1e9f, min_y = 1e9f, max_x = -1e9f, max_y = -1e9f;
  for (uint32_t i = 0; i < N_BUILD; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    float w = size_dist(rng);
    float h = size_dist(rng);
    build[i] = {x, y, x + w, y + h};
    min_x = std::min(min_x, x);
    min_y = std::min(min_y, y);
    max_x = std::max(max_x, x + w);
    max_y = std::max(max_y, y + h);
  }

  std::vector<BoundingBox> probe(N_PROBE);
  for (uint32_t i = 0; i < N_PROBE; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    float w = size_dist(rng);
    float h = size_dist(rng);
    probe[i] = {x, y, x + w, y + h};
  }

  // Compute ground truth
  std::cout << "  Computing CPU ground truth for 50k x 50k boxes...\n";
  auto cpu_truth = cpu_grid_join(build, probe, min_x, min_y, max_x, max_y);
  std::cout << "  CPU ground truth: " << cpu_truth.size() << " matches\n";

  // Run GPU Spatial Hash Index
  MetalSpatialIndex index(device);
  index.set_index_type(IndexType::SpatialHash);
  index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);

  auto t_b0 = std::chrono::high_resolution_clock::now();
  index.finish_building();
  auto t_b1 = std::chrono::high_resolution_clock::now();
  double build_ms = std::chrono::duration<double, std::milli>(t_b1 - t_b0).count();

  std::vector<uint32_t> out_build, out_probe;
  auto t_p0 = std::chrono::high_resolution_clock::now();
  index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE, out_build,
              out_probe);
  auto t_p1 = std::chrono::high_resolution_clock::now();
  double probe_ms = std::chrono::duration<double, std::milli>(t_p1 - t_p0).count();

  assert(index.get_active_index_type() == IndexType::SpatialHash);

  std::cout << "  GPU Grid Build Time: " << build_ms << " ms\n";
  std::cout << "  GPU Grid Probe Time: " << probe_ms
            << " ms (Kernel: " << index.get_last_probe_time_ms() << " ms)\n";

  // Exact parity assertion
  std::set<std::pair<uint32_t, uint32_t>> gpu_matches;
  for (size_t i = 0; i < out_build.size(); ++i) {
    gpu_matches.insert({out_build[i], out_probe[i]});
  }

  assert(gpu_matches.size() == cpu_truth.size());
  assert(gpu_matches == cpu_truth);
  std::cout << "  Parity: EXACT MATCH (" << gpu_matches.size()
            << " pairs, active engine: SpatialHash)\n";
}

// =====================================================================
// Test 4: Comprehensive Edge Cases & Buffer Overflow Protection
// =====================================================================
void test_edge_cases(id<MTLDevice> device) {
  std::cout << "\n--- Test 4: Comprehensive Edge Cases & Lifecycle ---\n";

  std::vector<uint32_t> out_build, out_probe;

  // 1. Empty Index
  {
    MetalSpatialIndex index(device);
    BoundingBox p = {0.0f, 0.0f, 1.0f, 1.0f};
    index.probe(reinterpret_cast<const float*>(&p), 1, out_build, out_probe);
    assert(out_build.empty() && out_probe.empty());

    index.finish_building();
    index.probe(reinterpret_cast<const float*>(&p), 1, out_build, out_probe);
    assert(out_build.empty() && out_probe.empty());
    std::cout << "  Empty Index: PASS (0 matches, safe)\n";
  }

  // 2. Zero Probes
  {
    MetalSpatialIndex index(device);
    BoundingBox b = {0.0f, 0.0f, 1.0f, 1.0f};
    index.push_build(reinterpret_cast<const float*>(&b), 1);
    index.finish_building();
    index.probe(nullptr, 0, out_build, out_probe);
    assert(out_build.empty() && out_probe.empty());
    std::cout << "  Zero Probes: PASS (0 matches, safe)\n";
  }

  // 3. Auto-building when probe is called directly
  {
    MetalSpatialIndex index(device);
    BoundingBox b = {5.0f, 5.0f, 10.0f, 10.0f};
    index.push_build(reinterpret_cast<const float*>(&b), 1);
    // Do NOT call index.finish_building() - probe must auto-build!
    BoundingBox p = {6.0f, 6.0f, 6.0f, 6.0f};
    index.probe(reinterpret_cast<const float*>(&p), 1, out_build, out_probe);
    assert(out_build.size() == 1 && out_probe.size() == 1);
    assert(out_build[0] == 0 && out_probe[0] == 0);
    std::cout << "  Auto-Building on Probe: PASS (1 match, auto-built)\n";
  }

  // 4. Completely Disjoint Sets
  {
    MetalSpatialIndex index(device);
    BoundingBox b = {0.0f, 0.0f, 1.0f, 1.0f};
    BoundingBox p = {1000.0f, 1000.0f, 1001.0f, 1001.0f};
    index.push_build(reinterpret_cast<const float*>(&b), 1);
    index.finish_building();
    index.probe(reinterpret_cast<const float*>(&p), 1, out_build, out_probe);
    assert(out_build.empty() && out_probe.empty());
    std::cout << "  Disjoint Sets: PASS (0 false positives)\n";
  }

  // 5. Boundary & Corner Touching across engines
  {
    std::vector<BoundingBox> b_touch = {{0.0f, 0.0f, 1.0f, 1.0f},
                                        {1.0f, 0.0f, 2.0f, 1.0f}};
    std::vector<BoundingBox> p_touch = {
        {1.0f, 0.5f, 1.0f, 0.5f},  // Point exactly on shared edge
        {1.0f, 1.0f, 1.0f, 1.0f}   // Point on shared corner
    };

    std::vector<IndexType> types = {IndexType::SpatialHash};
    if ([device supportsRaytracing]) {
      types.push_back(IndexType::HardwareRT);
    }

    for (auto t : types) {
      MetalSpatialIndex index(device);
      index.set_index_type(t);
      index.push_build(reinterpret_cast<const float*>(b_touch.data()), 2);
      index.finish_building();
      index.probe(reinterpret_cast<const float*>(p_touch.data()), 2, out_build,
                  out_probe);
      assert(out_build.size() == 4);  // Each point touches both boxes
    }
    std::cout << "  Boundary & Corner Touching: PASS (exact 4 matches on both engines)\n";
  }

  // 6. Incremental Build, Re-building & Clear Lifecycle
  {
    MetalSpatialIndex index(device);
    BoundingBox b1 = {0.0f, 0.0f, 1.0f, 1.0f};
    BoundingBox b2 = {10.0f, 10.0f, 11.0f, 11.0f};

    index.push_build(reinterpret_cast<const float*>(&b1), 1);
    index.finish_building();
    index.probe(reinterpret_cast<const float*>(&b1), 1, out_build, out_probe);
    assert(out_build.size() == 1);

    // Push more boxes and re-probe
    index.push_build(reinterpret_cast<const float*>(&b2), 1);
    assert(index.get_build_count() == 2);
    std::vector<BoundingBox> p_both = {b1, b2};
    index.probe(reinterpret_cast<const float*>(p_both.data()), 2, out_build, out_probe);
    assert(out_build.size() == 2);

    // Clear and verify clean state
    index.clear();
    assert(index.get_build_count() == 0);
    index.push_build(reinterpret_cast<const float*>(&b2), 1);
    index.finish_building();
    index.probe(reinterpret_cast<const float*>(&b1), 1, out_build, out_probe);
    assert(out_build.empty());
    index.probe(reinterpret_cast<const float*>(&b2), 1, out_build, out_probe);
    assert(out_build.size() == 1 && out_build[0] == 0);
    std::cout << "  Incremental Build & Clear Lifecycle: PASS\n";
  }
}

// =====================================================================
// Test 5: Direct Hardware RT vs. Spatial Hash Differential Parity
// =====================================================================
void test_rt_hash_parity(id<MTLDevice> device) {
  std::cout
      << "\n--- Test 5: Hardware RT vs. Spatial Hash Differential Parity (5k x 5k) ---\n";
  if (![device supportsRaytracing]) {
    std::cout << "  [SKIPPED] Hardware Ray Tracing not supported on this device\n";
    return;
  }

  const uint32_t N_BUILD = 5000;
  const uint32_t N_PROBE = 5000;

  std::mt19937 rng(999);
  std::uniform_real_distribution<float> pos_dist(0.0f, 500.0f);
  std::uniform_real_distribution<float> size_dist(0.5f, 5.0f);

  std::vector<BoundingBox> build(N_BUILD);
  for (uint32_t i = 0; i < N_BUILD; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    float w = size_dist(rng);
    float h = size_dist(rng);
    build[i] = {x, y, x + w, y + h};
  }

  std::vector<BoundingBox> probe(N_PROBE);
  for (uint32_t i = 0; i < N_PROBE; ++i) {
    float x = pos_dist(rng);
    float y = pos_dist(rng);
    probe[i] = {x, y, x, y};  // Points
  }

  // 1. Run Hardware RT
  MetalSpatialIndex rt_index(device);
  rt_index.set_index_type(IndexType::HardwareRT);
  rt_index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);
  rt_index.finish_building();
  std::vector<uint32_t> rt_build, rt_probe;
  rt_index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE, rt_build,
                 rt_probe);
  assert(rt_index.get_active_index_type() == IndexType::HardwareRT);

  std::set<std::pair<uint32_t, uint32_t>> rt_set;
  for (size_t i = 0; i < rt_build.size(); ++i) {
    rt_set.insert({rt_build[i], rt_probe[i]});
  }

  // 2. Run Spatial Hash
  MetalSpatialIndex hash_index(device);
  hash_index.set_index_type(IndexType::SpatialHash);
  hash_index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);
  hash_index.finish_building();
  std::vector<uint32_t> hash_build, hash_probe;
  hash_index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE, hash_build,
                   hash_probe);
  assert(hash_index.get_active_index_type() == IndexType::SpatialHash);

  std::set<std::pair<uint32_t, uint32_t>> hash_set;
  for (size_t i = 0; i < hash_build.size(); ++i) {
    hash_set.insert({hash_build[i], hash_probe[i]});
  }

  assert(rt_set.size() == hash_set.size());
  assert(rt_set == hash_set);
  std::cout << "  Parity between Hardware RT and Spatial Hash: 100% IDENTICAL ("
            << rt_set.size() << " pairs)\n";
}

// =====================================================================
// Test 6: Robust NaN, Inf & Inverted Bounds Handling (Item 1.2)
// =====================================================================
void test_nan_inf_cases(id<MTLDevice> device) {
  std::cout << "\n--- Test 6: NaN, Inf & Inverted Bounds Handling ---\n";

  float qnan = std::numeric_limits<float>::quiet_NaN();
  float pinf = std::numeric_limits<float>::infinity();
  float ninf = -std::numeric_limits<float>::infinity();

  // 11 build boxes: valid at index 0 and 10; various invalid boxes in 1..9
  std::vector<BoundingBox> build = {
      {0.0f, 0.0f, 10.0f, 10.0f},   // 0: valid
      {qnan, 0.0f, 10.0f, 10.0f},   // 1: NaN xmin
      {0.0f, qnan, 10.0f, 10.0f},   // 2: NaN ymin
      {0.0f, 0.0f, qnan, 10.0f},    // 3: NaN xmax
      {0.0f, 0.0f, 10.0f, qnan},    // 4: NaN ymax
      {pinf, 0.0f, 10.0f, 10.0f},   // 5: +Inf xmin
      {ninf, 0.0f, 10.0f, 10.0f},   // 6: -Inf xmin
      {0.0f, 0.0f, pinf, 10.0f},    // 7: +Inf xmax
      {10.0f, 0.0f, 5.0f, 10.0f},   // 8: inverted xmin > xmax
      {0.0f, 10.0f, 10.0f, 5.0f},   // 9: inverted ymin > ymax
      {20.0f, 20.0f, 30.0f, 30.0f}  // 10: valid
  };

  // 5 probe boxes (points): valid at index 0 and 3; invalid at 1, 2, 4
  std::vector<BoundingBox> probe = {
      {5.0f, 5.0f, 5.0f, 5.0f},      // 0: inside build[0]
      {qnan, 5.0f, qnan, 5.0f},      // 1: NaN probe
      {5.0f, pinf, 5.0f, pinf},      // 2: Inf probe
      {25.0f, 25.0f, 25.0f, 25.0f},  // 3: inside build[10]
      {15.0f, 0.0f, 5.0f, 0.0f}      // 4: inverted probe xmin > xmax
  };

  std::vector<IndexType> engines = {IndexType::SpatialHash};
  if ([device supportsRaytracing]) {
    engines.push_back(IndexType::HardwareRT);
  }

  for (auto engine : engines) {
    MetalSpatialIndex index(device);
    index.set_index_type(engine);
    index.push_build(reinterpret_cast<const float*>(build.data()),
                     static_cast<uint32_t>(build.size()));
    index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    index.probe(reinterpret_cast<const float*>(probe.data()),
                static_cast<uint32_t>(probe.size()), out_build, out_probe);

    // Positional index integrity check:
    // Must contain ONLY matches (build 0, probe 0) and (build 10, probe 3).
    assert(out_build.size() == 2);
    assert(out_probe.size() == 2);

    bool found_0_0 = false;
    bool found_10_3 = false;
    for (size_t i = 0; i < out_build.size(); ++i) {
      uint32_t b = out_build[i];
      uint32_t p = out_probe[i];
      assert(b == 0 || b == 10);
      assert(p == 0 || p == 3);
      if (b == 0 && p == 0) found_0_0 = true;
      if (b == 10 && p == 3) found_10_3 = true;
    }
    assert(found_0_0 && found_10_3);

    // Sub-test: All build boxes are NaN/Inf -> graceful empty result
    std::vector<BoundingBox> all_nan_build = {{qnan, qnan, qnan, qnan},
                                              {pinf, pinf, pinf, pinf}};
    MetalSpatialIndex empty_build_index(device);
    empty_build_index.set_index_type(engine);
    empty_build_index.push_build(reinterpret_cast<const float*>(all_nan_build.data()), 2);
    empty_build_index.finish_building();

    out_build.clear();
    out_probe.clear();
    empty_build_index.probe(reinterpret_cast<const float*>(probe.data()),
                            static_cast<uint32_t>(probe.size()), out_build, out_probe);
    assert(out_build.empty());
    assert(out_probe.empty());

    // Sub-test: All probe boxes are NaN/Inf -> graceful empty result
    std::vector<BoundingBox> all_nan_probe = {{qnan, qnan, qnan, qnan},
                                              {pinf, pinf, pinf, pinf}};
    out_build.clear();
    out_probe.clear();
    index.probe(reinterpret_cast<const float*>(all_nan_probe.data()), 2, out_build,
                out_probe);
    assert(out_build.empty());
    assert(out_probe.empty());
  }

  std::cout << "  NaN, Inf & Inverted Bounds Handling: PASS (all engines, positional "
               "integrity verified)\n";
}

// =====================================================================
// Test 7: Error Handling & Diagnostics (Item 1.3)
// =====================================================================
void test_error_handling(id<MTLDevice> device) {
  std::cout << "\n--- Test 7: Error Handling & Diagnostics (Item 1.3) ---\n";
  MetalSpatialIndex index(device);
  assert(index.is_valid());

  std::vector<uint32_t> b, p;
  bool ok = index.probe(nullptr, 10, b, p);
  assert(!ok);
  assert(strlen(index.get_last_error()) > 0);
  std::cout << "  Null probe error captured: " << index.get_last_error() << "\n";

  bool push_ok = index.push_build(nullptr, 5);
  assert(!push_ok);
  assert(strlen(index.get_last_error()) > 0);
  std::cout << "  Null push_build error captured: " << index.get_last_error() << "\n";
  std::cout << "  Error Handling & Diagnostics: PASS\n";
}

// =====================================================================
// Test 8: Hierarchical Grid Adversarial Cases (Item 1.4)
// =====================================================================
void test_hierarchical_grid_adversarial(id<MTLDevice> device) {
  std::cout << "\n--- Test 8: Hierarchical Grid Adversarial Cases (Item 1.4) ---\n";

  auto cpu_oracle = [](const std::vector<BoundingBox>& build,
                       const std::vector<BoundingBox>& probe) {
    std::vector<std::pair<uint32_t, uint32_t>> matches;
    for (uint32_t p = 0; p < probe.size(); ++p) {
      const auto& pb = probe[p];
      if (!cpu_is_valid_box(pb)) continue;
      for (uint32_t b = 0; b < build.size(); ++b) {
        const auto& bb = build[b];
        if (!cpu_is_valid_box(bb)) continue;
        if (cpu_boxes_intersect(bb, pb)) {
          matches.push_back({b, p});
        }
      }
    }
    std::sort(matches.begin(), matches.end());
    return matches;
  };

  auto verify_matches = [](const std::vector<uint32_t>& out_b,
                           const std::vector<uint32_t>& out_p,
                           const std::vector<std::pair<uint32_t, uint32_t>>& expected) {
    assert(out_b.size() == out_p.size());
    assert(out_b.size() == expected.size());
    std::vector<std::pair<uint32_t, uint32_t>> actual(out_b.size());
    for (size_t i = 0; i < out_b.size(); ++i) {
      actual[i] = {out_b[i], out_p[i]};
    }
    std::sort(actual.begin(), actual.end());
    assert(actual == expected);
  };

  // Sub-test 8.1: 20k build boxes that each span the full extent
  {
    const uint32_t N_BUILD = 20000;
    const uint32_t N_PROBE = 100;
    std::vector<BoundingBox> build(N_BUILD, BoundingBox{0.0f, 0.0f, 1000.0f, 1000.0f});

    std::mt19937 rng(1401);
    std::uniform_real_distribution<float> coord_dist(10.0f, 990.0f);
    std::uniform_real_distribution<float> size_dist(1.0f, 10.0f);
    std::vector<BoundingBox> probe(N_PROBE);
    for (uint32_t i = 0; i < N_PROBE; ++i) {
      float x = coord_dist(rng);
      float y = coord_dist(rng);
      float w = size_dist(rng);
      float h = size_dist(rng);
      probe[i] = BoundingBox{x, y, x + w, y + h};
    }

    auto expected = cpu_oracle(build, probe);
    assert(expected.size() == static_cast<size_t>(N_BUILD) * N_PROBE);

    MetalSpatialIndex index(device);
    index.set_index_type(IndexType::SpatialHash);
    index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);
    index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    bool ok = index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE,
                          out_build, out_probe);
    assert(ok);
    verify_matches(out_build, out_probe, expected);
    std::cout << "  20k Full-Extent Build Boxes vs Probes: PASS (" << out_build.size()
              << " matches, verified vs CPU oracle)\n";
  }

  // Sub-test 8.2: Heavily clustered data (95% of 20k boxes in 1% of extent)
  {
    const uint32_t N_BUILD = 20000;
    const uint32_t N_PROBE = 200;
    std::vector<BoundingBox> build(N_BUILD);
    std::mt19937 rng(1402);
    std::uniform_real_distribution<float> cluster_dist(0.0f,
                                                       100.0f);  // 1% area of 1000x1000
    std::uniform_real_distribution<float> world_dist(0.0f, 1000.0f);
    std::uniform_real_distribution<float> size_dist(0.1f, 2.0f);

    uint32_t n_cluster = static_cast<uint32_t>(N_BUILD * 0.95);
    for (uint32_t i = 0; i < n_cluster; ++i) {
      float x = cluster_dist(rng);
      float y = cluster_dist(rng);
      build[i] = BoundingBox{x, y, x + size_dist(rng), y + size_dist(rng)};
    }
    for (uint32_t i = n_cluster; i < N_BUILD; ++i) {
      float x = world_dist(rng);
      float y = world_dist(rng);
      build[i] = BoundingBox{x, y, x + size_dist(rng), y + size_dist(rng)};
    }

    std::vector<BoundingBox> probe(N_PROBE);
    for (uint32_t i = 0; i < N_PROBE; ++i) {
      // Half inside cluster, half outside
      float x = (i % 2 == 0) ? cluster_dist(rng) : world_dist(rng);
      float y = (i % 2 == 0) ? cluster_dist(rng) : world_dist(rng);
      probe[i] = BoundingBox{x, y, x + size_dist(rng), y + size_dist(rng)};
    }

    auto expected = cpu_oracle(build, probe);

    MetalSpatialIndex index(device);
    index.set_index_type(IndexType::SpatialHash);
    index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);
    index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    bool ok = index.probe(reinterpret_cast<const float*>(probe.data()), N_PROBE,
                          out_build, out_probe);
    assert(ok);
    verify_matches(out_build, out_probe, expected);
    std::cout << "  95% Clustered in 1% Extent: PASS (" << out_build.size()
              << " matches, verified vs CPU oracle)\n";
  }

  // Sub-test 8.3: One probe box covering everything
  {
    const uint32_t N_BUILD = 10000;
    std::vector<BoundingBox> build(N_BUILD);
    std::mt19937 rng(1403);
    std::uniform_real_distribution<float> world_dist(0.0f, 1000.0f);
    std::uniform_real_distribution<float> size_dist(0.5f, 5.0f);
    for (uint32_t i = 0; i < N_BUILD; ++i) {
      float x = world_dist(rng);
      float y = world_dist(rng);
      build[i] = BoundingBox{x, y, x + size_dist(rng), y + size_dist(rng)};
    }

    std::vector<BoundingBox> probe = {
        BoundingBox{-50.0f, -50.0f, 1050.0f, 1050.0f}  // Covers entire extent
    };

    auto expected = cpu_oracle(build, probe);
    assert(expected.size() == N_BUILD);

    MetalSpatialIndex index(device);
    index.set_index_type(IndexType::SpatialHash);
    index.push_build(reinterpret_cast<const float*>(build.data()), N_BUILD);
    index.finish_building();

    std::vector<uint32_t> out_build, out_probe;
    bool ok = index.probe(reinterpret_cast<const float*>(probe.data()), 1, out_build,
                          out_probe);
    assert(ok);
    verify_matches(out_build, out_probe, expected);
    std::cout << "  One Probe Box Covering Everything: PASS (" << out_build.size()
              << " matches, verified vs CPU oracle)\n";
  }
}

int main() {
  @autoreleasepool {
    std::cout << "========================================================\n";
    std::cout << " SedonaDB Metal Spatial Index Test Suite (Phase 2)\n";
    std::cout << "========================================================\n";

    id<MTLDevice> device = MTLCreateSystemDefaultDevice();
    if (!device) {
      std::cerr << "Fatal: No Metal-compatible GPU found.\n";
      return 1;
    }

    std::cout << "Device Name:       " << [[device name] UTF8String] << "\n";
    std::cout << "Unified Memory:    " << (device.hasUnifiedMemory ? "YES" : "NO")
              << "\n";
    std::cout << "Hardware RT Cores: " << ([device supportsRaytracing] ? "YES" : "NO")
              << "\n";

    auto suite_t0 = std::chrono::high_resolution_clock::now();

    test_sanity_cases(device);
    test_rt_scale(device);
    test_spatial_hash_scale(device);
    test_edge_cases(device);
    test_rt_hash_parity(device);
    test_nan_inf_cases(device);
    test_error_handling(device);
    test_hierarchical_grid_adversarial(device);

    auto suite_t1 = std::chrono::high_resolution_clock::now();
    double total_suite_s = std::chrono::duration<double>(suite_t1 - suite_t0).count();

    std::cout << "\n========================================================\n";
    std::cout << " ALL PHASE 2 TESTS PASSED SUCCESSFULLY in " << total_suite_s << " s\n";
    std::cout << "========================================================\n";
  }
  return 0;
}
