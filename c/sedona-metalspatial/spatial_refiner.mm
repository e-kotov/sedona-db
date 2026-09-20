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
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#import "metal_shaders.h"

namespace {

// ---------------------------------------------------------------------------
// Ray-traced edge index constants. The soundness argument lives next to
// evaluate_ring_rt() in refine.metal; the invariants it needs from this file are
// marked [RT-I1]..[RT-I6] below.
// ---------------------------------------------------------------------------

// Must match struct RtRingInfo in refine.metal.
struct RtRingInfo {
  uint32_t first_prim;
  uint32_t prim_count;
  uint32_t segs_per_box;
  float z;
  float scale;
  float eta_limit;
  float ray_back;
  uint32_t _padding;
};

constexpr uint32_t kRtNoSlot = 0xFFFFFFFFu;
// Must match RT_MAX_BOXES_PER_RING in refine.metal (capacity of the duplicate filter).
constexpr uint32_t kRtMaxBoxesPerRing = 4096;
// z slabs are [slot - 0.25, slot + 0.25] around the integer slot. At 2^18 one f32 ulp is
// 2^-5, so the slab half-thickness is at least 8 ulps of z and adjacent slabs are at
// least 16 ulps apart. Rings beyond the limit keep the linear scan.
constexpr uint32_t kRtMaxSlots = 1u << 18;
// Clearance (in scaled units, where ring coordinates lie in [-1, 1]) added on top of the
// analytically required padding to absorb the undocumented precision of traversal.
constexpr double kRtTraversalSlack = 1.0 / 8192.0;
// Rings whose total padding would exceed this are not indexed (the index would not
// prune).
constexpr double kRtMaxPad = 1.0 / 64.0;
constexpr uint32_t kRtStatCount = 8;

float round_down_f32(double v) {
  float f = static_cast<float>(v);
  if (static_cast<double>(f) > v)
    f = std::nextafter(f, -std::numeric_limits<float>::infinity());
  return f;
}

float round_up_f32(double v) {
  float f = static_cast<float>(v);
  if (static_cast<double>(f) < v)
    f = std::nextafter(f, std::numeric_limits<float>::infinity());
  return f;
}

uint32_t env_u32(const char* name, uint32_t fallback) {
  const char* v = std::getenv(name);
  if (!v || !*v) return fallback;
  char* end = nullptr;
  unsigned long parsed = std::strtoul(v, &end, 10);
  if (end == v) return fallback;
  return static_cast<uint32_t>(parsed);
}

id<MTLComputePipelineState> compile_refine_pipeline(id<MTLDevice> device, int bound_mode,
                                                    bool rt_enable, bool rt_stats,
                                                    std::string* err) {
  MTLCompileOptions* options = [[MTLCompileOptions alloc] init];
  options.fastMathEnabled = NO;
  if (@available(macOS 15.0, *)) {
    options.mathMode = MTLMathModeSafe;
  }
  options.preprocessorMacros = @{
    @"BOUND_MODE" : @(bound_mode),
    @"RT_ENABLE" : @(rt_enable ? 1 : 0),
    @"RT_STATS" : @(rt_stats ? 1 : 0)
  };

  NSError* error = nil;
  NSString* src = [NSString stringWithUTF8String:REFINE_METAL_SOURCE];
  id<MTLLibrary> library = [device newLibraryWithSource:src options:options error:&error];
  if (!library) {
    *err = "Failed to compile refine.metal: " +
           std::string(error ? [[error localizedDescription] UTF8String]
                             : "Unknown shader error");
    return nil;
  }

  id<MTLFunction> kernel_fn = [library newFunctionWithName:@"point_in_polygon_refine"];
  if (!kernel_fn) {
    *err = "Function point_in_polygon_refine not found in library";
    return nil;
  }

  id<MTLComputePipelineState> pso = [device newComputePipelineStateWithFunction:kernel_fn
                                                                          error:&error];
  if (!pso) {
    *err = "Failed to create pipeline state: " +
           std::string(error ? [[error localizedDescription] UTF8String]
                             : "Unknown pipeline error");
    return nil;
  }
  return pso;
}

}  // namespace

RtConfig RtConfig::from_env() {
  RtConfig cfg;
  cfg.enabled = env_u32("SEDONA_METAL_RT_REFINE", 1);
  cfg.min_ring_vertices = env_u32("SEDONA_METAL_RT_MIN_RING", 256);
  cfg.segs_per_box = env_u32("SEDONA_METAL_RT_SEGS", 16);
  cfg.slot_base = 0;
  cfg.collect_stats = env_u32("SEDONA_METAL_RT_STATS", 0);
  return cfg;
}

#ifdef ENABLE_TEST_INTERNALS
MetalSpatialRefiner::MetalSpatialRefiner(id device, int bound_mode,
                                         const RtConfig* rt_config)
#else
MetalSpatialRefiner::MetalSpatialRefiner(id device, const RtConfig* rt_config)
#endif
    : device_(device),
      command_queue_(nil),
      pipeline_state_(nil),
      buf_polygons_(nil),
      buf_parts_(nil),
      buf_rings_(nil),
      buf_vertices_(nil),
      rt_pipeline_state_(nil),
      rt_accel_(nil),
      buf_ring_rt_slots_(nil),
      buf_rt_infos_(nil),
      buf_rt_stats_(nil),
      rt_config_(rt_config ? *rt_config : RtConfig::from_env()),
#ifdef ENABLE_TEST_INTERNALS
      bound_mode_(bound_mode),
#else
      bound_mode_(0),
#endif
      rt_info_{},
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

    // Linear-scan pipeline: always available, and the only one used when no ring is
    // indexed. The ray-traced variant is compiled lazily by build_rt_index().
    std::string err;
    pipeline_state_ = compile_refine_pipeline(device_, bound_mode_, false, false, &err);
    if (!pipeline_state_) {
      set_error(err);
      throw std::runtime_error(err);
    }
  }
}

void MetalSpatialRefiner::get_rt_info(uint64_t* out) const {
  for (uint32_t i = 0; i < 8; ++i) out[i] = rt_info_[i];
  for (uint32_t i = 0; i < kRtStatCount; ++i) out[8 + i] = 0;
  if (buf_rt_stats_) {
    const uint32_t* stats = static_cast<const uint32_t*>(buf_rt_stats_.contents);
    for (uint32_t i = 0; i < kRtStatCount; ++i) out[8 + i] = stats[i];
  }
}

// Builds ONE primitive acceleration structure of padded bounding boxes over groups of
// consecutive ring segments, for every ring with at least min_ring_vertices vertices.
// Must run while the host geometry vectors are still populated.
void MetalSpatialRefiner::build_rt_index() {
  if (!rt_config_.enabled || bound_mode_ != 0) return;
  if (![device_ supportsRaytracing]) return;

  auto t_host0 = std::chrono::steady_clock::now();

  const double u = 5.9604644775390625e-8;           // 2^-24
  const double two_neg_48 = 3.552713678800501e-15;  // 2^-48
  const uint32_t min_verts = std::max<uint32_t>(rt_config_.min_ring_vertices, 3);
  const uint32_t base_segs = std::max<uint32_t>(rt_config_.segs_per_box, 1);
  const size_t num_rings = host_rings_.size();
  const size_t num_verts = host_vertices_.size();

  std::vector<uint32_t> ring_slots(num_rings, kRtNoSlot);
  std::vector<RtRingInfo> infos;
  std::vector<double> pads;  // per slot, scaled units
  uint64_t total_boxes = 0;
  uint64_t skipped_slot_limit = 0;
  uint64_t skipped_numeric = 0;

  for (const PolygonRecord& poly : host_polygons_) {
    if (!poly.is_valid) continue;
    if ((uint64_t)poly.part_start + poly.part_count > host_parts_.size()) continue;

    // Pass 1 over the polygon: is any ring large enough, and what is the largest
    // |local coordinate| R_v of the polygon (bounds |delta| for probes inside the bbox).
    bool any_large = false;
    bool sane = true;
    for (uint32_t p = 0; p < poly.part_count && sane; ++p) {
      const PartRecord& part = host_parts_[poly.part_start + p];
      if ((uint64_t)part.ring_start + part.ring_count > num_rings) {
        sane = false;
        break;
      }
      for (uint32_t r = 0; r < part.ring_count; ++r) {
        const RingRecord& ring = host_rings_[part.ring_start + r];
        if ((uint64_t)ring.vertex_start + ring.vertex_count > num_verts) {
          sane = false;
          break;
        }
        if (ring.vertex_count >= min_verts) any_large = true;
      }
    }
    if (!sane || !any_large) continue;

    double r_v = 0.0;
    for (uint32_t p = 0; p < poly.part_count; ++p) {
      const PartRecord& part = host_parts_[poly.part_start + p];
      for (uint32_t r = 0; r < part.ring_count; ++r) {
        const RingRecord& ring = host_rings_[part.ring_start + r];
        for (uint32_t i = 0; i < ring.vertex_count; ++i) {
          const Point2D& v = host_vertices_[ring.vertex_start + i];
          r_v = std::max(r_v, (double)std::max(std::abs(v.x), std::abs(v.y)));
        }
      }
    }
    // Largest eta_k the kernel computes for a probe inside the polygon's bounding box:
    // eta_k = 2 * (eta_poly + 3u|delta| + 2^-48 |p_hi|), |delta| <~ R_v, |p| <= |o| +
    // R_v. This is a performance estimate only: [RT-I1] the kernel compares its own eta_k
    // with eta_limit and takes the linear scan when it is larger, so an underestimate is
    // safe.
    double o_norm =
        std::max(std::abs((double)poly.origin_hi_x), std::abs((double)poly.origin_hi_y));
    double eta_limit_d = 2.0 *
                         ((double)poly.eta_poly + 3.0 * u * 1.5 * r_v +
                          two_neg_48 * (o_norm + 2.0 * r_v)) *
                         1.01;
    float eta_limit = round_up_f32(eta_limit_d);
    if (!std::isfinite(eta_limit) || !(eta_limit > 0.0f)) {
      continue;
    }

    for (uint32_t p = 0; p < poly.part_count; ++p) {
      const PartRecord& part = host_parts_[poly.part_start + p];
      for (uint32_t r = 0; r < part.ring_count; ++r) {
        uint32_t ring_idx = part.ring_start + r;
        const RingRecord& ring = host_rings_[ring_idx];
        uint32_t n = ring.vertex_count;
        if (n < min_verts) continue;
        // A ring referenced twice keeps its first slot.
        if (ring_slots[ring_idx] != kRtNoSlot) continue;

        // [RT-I2] z slot limit: beyond it the slab thickness is too few ulps of z.
        uint64_t slot_z = (uint64_t)rt_config_.slot_base + infos.size();
        if (slot_z >= kRtMaxSlots) {
          skipped_slot_limit++;
          continue;
        }

        double m_ring = 0.0;
        bool finite = true;
        for (uint32_t i = 0; i < n; ++i) {
          const Point2D& v = host_vertices_[ring.vertex_start + i];
          if (!std::isfinite(v.x) || !std::isfinite(v.y)) {
            finite = false;
            break;
          }
          m_ring = std::max(m_ring, (double)std::max(std::abs(v.x), std::abs(v.y)));
        }
        if (!finite || !(m_ring > 0.0)) {
          skipped_numeric++;
          continue;
        }

        // [RT-I3] scale is an exact power of two with scale * m_ring in [0.5, 1), so
        // scaling vertices and delta in f32 is exact (no underflow: |exponent| <= 40).
        int exp2 = 0;
        std::frexp(m_ring, &exp2);
        if (exp2 < -40 || exp2 > 40) {
          skipped_numeric++;
          continue;
        }
        double scale = std::ldexp(1.0, -exp2);

        // [RT-I4] pad covers e' = eta_limit * (1 + 2u) in scaled units plus the traversal
        // slack; the 1.001 factor absorbs the rounding of this very expression.
        double pad =
            scale * (double)eta_limit * (1.0 + 2.0 * u) * 1.001 + kRtTraversalSlack;
        if (!(pad <= kRtMaxPad)) {
          skipped_numeric++;
          continue;
        }

        // [RT-I5] at most kRtMaxBoxesPerRing boxes per ring (duplicate filter capacity).
        uint32_t segs =
            std::max(base_segs, (n + kRtMaxBoxesPerRing - 1) / kRtMaxBoxesPerRing);
        uint32_t prim_count = (n + segs - 1) / segs;
        if (total_boxes + prim_count > 0x7FFFFFFFull) {
          skipped_slot_limit++;
          continue;
        }

        RtRingInfo info;
        info.first_prim = (uint32_t)total_boxes;
        info.prim_count = prim_count;
        info.segs_per_box = segs;
        info.z = (float)slot_z;  // exact: slot_z < 2^18
        info.scale = (float)scale;
        info.eta_limit = eta_limit;
        info.ray_back = (float)(2.0 * kRtTraversalSlack);
        info._padding = 0;

        ring_slots[ring_idx] = (uint32_t)infos.size();
        infos.push_back(info);
        pads.push_back(pad);
        total_boxes += prim_count;
      }
    }
  }

  rt_info_[6] = skipped_slot_limit;
  rt_info_[7] = skipped_numeric;
  if (infos.empty()) return;

  @autoreleasepool {
    size_t box_bytes = (size_t)total_boxes * sizeof(MTLAxisAlignedBoundingBox);
    id<MTLBuffer> buf_boxes = [device_ newBufferWithLength:box_bytes
                                                   options:MTLResourceStorageModeShared];
    if (!buf_boxes) {
      set_error("Failed to allocate RT edge index bounding box buffer");
      throw std::runtime_error("RT bounding box buffer allocation failed");
    }
    auto* boxes = static_cast<MTLAxisAlignedBoundingBox*>(buf_boxes.contents);

    for (size_t ring_idx = 0; ring_idx < num_rings; ++ring_idx) {
      uint32_t slot = ring_slots[ring_idx];
      if (slot == kRtNoSlot) continue;
      const RtRingInfo& info = infos[slot];
      const RingRecord& ring = host_rings_[ring_idx];
      const Point2D* verts = host_vertices_.data() + ring.vertex_start;
      const uint32_t n = ring.vertex_count;
      const double scale = (double)info.scale;
      const double pad = pads[slot];
      for (uint32_t g = 0; g < info.prim_count; ++g) {
        uint32_t e0 = g * info.segs_per_box;
        uint32_t e1 = std::min(e0 + info.segs_per_box, n);
        float min_x = verts[e0].x, max_x = verts[e0].x;
        float min_y = verts[e0].y, max_y = verts[e0].y;
        // Segment i joins vertex i and vertex (i + 1) % n, exactly as the kernel does.
        for (uint32_t i = e0; i < e1; ++i) {
          const Point2D& w = verts[(i + 1) % n];
          min_x = std::min(min_x, std::min(verts[i].x, w.x));
          max_x = std::max(max_x, std::max(verts[i].x, w.x));
          min_y = std::min(min_y, std::min(verts[i].y, w.y));
          max_y = std::max(max_y, std::max(verts[i].y, w.y));
        }
        // [RT-I6] outward rounding; thickness is nonzero in all three dimensions.
        MTLAxisAlignedBoundingBox& b = boxes[info.first_prim + g];
        b.min = MTLPackedFloat3Make(round_down_f32((double)min_x * scale - pad),
                                    round_down_f32((double)min_y * scale - pad),
                                    info.z - 0.25f);
        b.max = MTLPackedFloat3Make(round_up_f32((double)max_x * scale + pad),
                                    round_up_f32((double)max_y * scale + pad),
                                    info.z + 0.25f);
      }
    }
    auto t_host1 = std::chrono::steady_clock::now();

    MTLAccelerationStructureBoundingBoxGeometryDescriptor* geom_desc =
        [MTLAccelerationStructureBoundingBoxGeometryDescriptor descriptor];
    geom_desc.boundingBoxBuffer = buf_boxes;
    geom_desc.boundingBoxBufferOffset = 0;
    geom_desc.boundingBoxCount = (NSUInteger)total_boxes;
    geom_desc.boundingBoxStride = sizeof(MTLAxisAlignedBoundingBox);

    MTLPrimitiveAccelerationStructureDescriptor* accel_desc =
        [MTLPrimitiveAccelerationStructureDescriptor descriptor];
    accel_desc.geometryDescriptors = @[ geom_desc ];

    MTLAccelerationStructureSizes sizes =
        [device_ accelerationStructureSizesWithDescriptor:accel_desc];
    NSUInteger accel_size = std::max(sizes.accelerationStructureSize, (NSUInteger)256);
    NSUInteger scratch_size = std::max(sizes.buildScratchBufferSize, (NSUInteger)256);

    rt_accel_ = [device_ newAccelerationStructureWithSize:accel_size];
    id<MTLBuffer> scratch = [device_ newBufferWithLength:scratch_size
                                                 options:MTLResourceStorageModePrivate];
    if (!rt_accel_ || !scratch) {
      rt_accel_ = nil;
      set_error("Failed to allocate RT edge index acceleration structure");
      throw std::runtime_error("RT acceleration structure allocation failed");
    }

    id<MTLCommandBuffer> cmd = [command_queue_ commandBuffer];
    id<MTLAccelerationStructureCommandEncoder> enc =
        [cmd accelerationStructureCommandEncoder];
    [enc buildAccelerationStructure:rt_accel_
                         descriptor:accel_desc
                      scratchBuffer:scratch
                scratchBufferOffset:0];
    [enc endEncoding];
    [cmd commit];
    [cmd waitUntilCompleted];
    if (cmd.status != MTLCommandBufferStatusCompleted) {
      rt_accel_ = nil;
      std::string err_desc =
          cmd.error ? [[cmd.error localizedDescription] UTF8String] : "Unknown GPU error";
      set_error("RT edge index build failed: " + err_desc);
      throw std::runtime_error("RT edge index build failed");
    }
    auto t_build1 = std::chrono::steady_clock::now();

    buf_ring_rt_slots_ = [device_ newBufferWithBytes:ring_slots.data()
                                              length:ring_slots.size() * sizeof(uint32_t)
                                             options:MTLResourceStorageModeShared];
    buf_rt_infos_ = [device_ newBufferWithBytes:infos.data()
                                         length:infos.size() * sizeof(RtRingInfo)
                                        options:MTLResourceStorageModeShared];
    if (rt_config_.collect_stats) {
      buf_rt_stats_ = [device_ newBufferWithLength:kRtStatCount * sizeof(uint32_t)
                                           options:MTLResourceStorageModeShared];
      if (buf_rt_stats_) std::memset(buf_rt_stats_.contents, 0, kRtStatCount * 4);
    }
    std::string err;
    rt_pipeline_state_ = compile_refine_pipeline(device_, bound_mode_, true,
                                                 rt_config_.collect_stats != 0, &err);
    if (!buf_ring_rt_slots_ || !buf_rt_infos_ || !rt_pipeline_state_ ||
        (rt_config_.collect_stats && !buf_rt_stats_)) {
      rt_accel_ = nil;
      rt_pipeline_state_ = nil;
      set_error(err.empty() ? "Failed to allocate RT edge index buffers" : err);
      throw std::runtime_error("RT edge index setup failed");
    }

    using us = std::chrono::microseconds;
    rt_info_[0] = infos.size();
    rt_info_[1] = total_boxes;
    rt_info_[2] = accel_size;
    rt_info_[3] = scratch_size;
    rt_info_[4] = (uint64_t)std::chrono::duration_cast<us>(t_build1 - t_host1).count();
    rt_info_[5] = (uint64_t)std::chrono::duration_cast<us>(t_host1 - t_host0).count();
    allocated_bytes_ += accel_size + ring_slots.size() * sizeof(uint32_t) +
                        infos.size() * sizeof(RtRingInfo);

    if (env_u32("SEDONA_METAL_RT_LOG", 0)) {
      std::fprintf(
          stderr,
          "[sedona-metal rt] rings=%llu boxes=%llu as_bytes=%llu scratch_bytes=%llu "
          "box_buffer_bytes=%zu gpu_build_ms=%.1f host_prep_ms=%.1f "
          "skipped_slot_limit=%llu skipped_numeric=%llu (total rings=%zu verts=%zu)\n",
          rt_info_[0], rt_info_[1], rt_info_[2], rt_info_[3], box_bytes,
          rt_info_[4] / 1000.0, rt_info_[5] / 1000.0, rt_info_[6], rt_info_[7], num_rings,
          num_verts);
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

    if (buf_rt_stats_ && env_u32("SEDONA_METAL_RT_LOG", 0)) {
      const uint32_t* st = static_cast<const uint32_t*>(buf_rt_stats_.contents);
      std::fprintf(
          stderr,
          "[sedona-metal rt] stats ring_evals_x=%u ring_evals_y=%u box_reports=%u "
          "duplicate_reports=%u foreign_reports=%u eta_fallbacks=%u "
          "edges_visited=%u pairs_with_rt=%u\n",
          st[0], st[1], st[2], st[3], st[4], st[5], st[6], st[7]);
    }
    rt_pipeline_state_ = nil;
    rt_accel_ = nil;
    buf_ring_rt_slots_ = nil;
    buf_rt_infos_ = nil;
    buf_rt_stats_ = nil;
    std::memset(rt_info_, 0, sizeof(rt_info_));

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

    // Needs the host geometry vectors, so it runs before they are released.
    // The index is an optimisation only: if it cannot be built (e.g. out of GPU memory)
    // every ring keeps the linear scan.
    try {
      build_rt_index();
    } catch (const std::exception& e) {
      rt_pipeline_state_ = nil;
      rt_accel_ = nil;
      buf_ring_rt_slots_ = nil;
      buf_rt_infos_ = nil;
      buf_rt_stats_ = nil;
      std::fprintf(stderr, "[sedona-metal rt] edge index disabled: %s\n", e.what());
    }

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

        const bool use_rt = (rt_pipeline_state_ != nil) && (rt_accel_ != nil);
        id<MTLComputePipelineState> pso = use_rt ? rt_pipeline_state_ : pipeline_state_;

        [encoder setComputePipelineState:pso];
        [encoder setBuffer:buf_candidates offset:0 atIndex:0];
        [encoder setBuffer:buf_polygons_ offset:0 atIndex:1];
        [encoder setBuffer:buf_parts_ offset:0 atIndex:2];
        [encoder setBuffer:buf_rings_ offset:0 atIndex:3];
        [encoder setBuffer:buf_vertices_ offset:0 atIndex:4];
        [encoder setBuffer:buf_points offset:0 atIndex:5];
        [encoder setBuffer:buf_states offset:0 atIndex:6];
        [encoder setBytes:&chunk_len length:sizeof(uint32_t) atIndex:7];
        [encoder setBytes:&num_polygons length:sizeof(uint32_t) atIndex:8];
        if (use_rt) {
          [encoder setAccelerationStructure:rt_accel_ atBufferIndex:9];
          [encoder setBuffer:buf_ring_rt_slots_ offset:0 atIndex:10];
          [encoder setBuffer:buf_rt_infos_ offset:0 atIndex:11];
          if (buf_rt_stats_) {
            [encoder setBuffer:buf_rt_stats_ offset:0 atIndex:12];
          }
        }

        NSUInteger max_threads = pso.maxTotalThreadsPerThreadgroup;
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
