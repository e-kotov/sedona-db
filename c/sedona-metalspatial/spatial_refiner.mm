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
#include <dispatch/dispatch.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#import "metal_shaders.h"

// ----------------------------------------------------------------------------
// Ring slab edge index builder (see the soundness note in refine.metal, select_slab)
// ----------------------------------------------------------------------------
namespace {

constexpr uint32_t kIndexMinVertices = 64;  // smaller rings keep the linear scan
constexpr uint32_t kEdgesPerSlab = 16;      // K ~ n / kEdgesPerSlab
constexpr uint32_t kMaxSlabs = 32768;       // 2^15: bounds the GPU slab rounding error
constexpr double kSlabSlack =
    1.0 / 32.0;  // covers GPU (<0.008) + CPU (<=0.01) error in t
constexpr double kMaxCpuSlabErr = 0.01;
constexpr double kMaxEntriesPerEdge = 3.0;  // memory budget; halve K until it holds
constexpr float kMinPad = 1e-30f;
constexpr float kMaxInvH = 1e30f;
constexpr uint64_t kChunkVertices = 65536;  // parallel work granule

struct AxisPlan {
  AxisIndex rec{};
  uint64_t entries = 0;
  uint64_t entry_start = 0;
};

inline float coord_of(const Point2D& v, bool use_y) { return use_y ? v.y : v.x; }

// Slab range of an edge whose stabbing-coordinate interval is [min(a,b), max(a,b)],
// widened by pad and by kSlabSlack slabs. Used identically by the count and fill passes.
inline void edge_slab_range(double a, double b, double pad, double lo, double inv_h,
                            uint32_t num_slabs, uint32_t& s0, uint32_t& s1) {
  const double mn = std::min(a, b);
  const double mx = std::max(a, b);
  const double t0 = ((mn - pad) - lo) * inv_h - kSlabSlack;
  const double t1 = ((mx + pad) - lo) * inv_h + kSlabSlack;
  const double last = static_cast<double>(num_slabs - 1);
  s0 = t0 <= 0.0 ? 0u : (t0 >= last ? num_slabs - 1 : static_cast<uint32_t>(t0));
  s1 = t1 <= 0.0 ? 0u : (t1 >= last ? num_slabs - 1 : static_cast<uint32_t>(t1));
}

// Decides grid parameters for one ring on one axis. Leaves rec.num_slabs == 0 when the
// ring must not be indexed (too small, non-finite, degenerate, or over budget).
void plan_axis(const Point2D* v, uint32_t n, bool use_y, float pad, AxisPlan& out) {
  out = AxisPlan{};
  if (n < kIndexMinVertices || !(pad >= kMinPad) || !std::isfinite(pad)) return;

  float cmin = std::numeric_limits<float>::infinity();
  float cmax = -std::numeric_limits<float>::infinity();
  for (uint32_t i = 0; i < n; ++i) {
    const float c = coord_of(v[i], use_y);
    if (!std::isfinite(c)) return;
    cmin = std::min(cmin, c);
    cmax = std::max(cmax, c);
  }

  // Outward-rounded float bounds: lo <= cmin - pad, hi >= cmax + pad.
  const double lo_d = static_cast<double>(cmin) - static_cast<double>(pad);
  const double hi_d = static_cast<double>(cmax) + static_cast<double>(pad);
  float lo = static_cast<float>(lo_d);
  if (static_cast<double>(lo) > lo_d) lo = std::nextafter(lo, -INFINITY);
  float hi = static_cast<float>(hi_d);
  if (static_cast<double>(hi) < hi_d) hi = std::nextafter(hi, INFINITY);
  if (!std::isfinite(lo) || !std::isfinite(hi)) return;
  const double extent = static_cast<double>(hi) - static_cast<double>(lo);
  if (!(extent > 0.0)) return;

  uint32_t num_slabs = std::min((n + kEdgesPerSlab - 1) / kEdgesPerSlab, kMaxSlabs);
  const double mag = std::max(std::fabs((double)cmin), std::fabs((double)cmax)) + pad +
                     std::fabs((double)lo);
  while (num_slabs >= 2) {
    const float inv_h = static_cast<float>(static_cast<double>(num_slabs) / extent);
    if (!std::isfinite(inv_h) || !(inv_h > 0.0f) || inv_h > kMaxInvH) return;
    // f64 rounding error of t = ((c -/+ pad) - lo) * inv_h, three operations.
    const double cpu_err = 4.0 * 0x1p-53 * mag * static_cast<double>(inv_h);
    if (!(cpu_err <= kMaxCpuSlabErr)) return;

    uint64_t entries = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const Point2D& a = v[i];
      const Point2D& b = v[i + 1 == n ? 0 : i + 1];
      if (a.x == b.x && a.y == b.y) continue;  // the kernel skips zero-length edges
      uint32_t s0, s1;
      edge_slab_range(coord_of(a, use_y), coord_of(b, use_y), pad, lo, inv_h, num_slabs,
                      s0, s1);
      entries += (s1 - s0 + 1);
    }
    if (static_cast<double>(entries) <= kMaxEntriesPerEdge * n) {
      out.rec = AxisIndex{lo, hi, inv_h, pad, 0, num_slabs};
      out.entries = entries;
      return;
    }
    num_slabs /= 2;
  }
}

void fill_axis(const Point2D* v, uint32_t n, bool use_y, const AxisPlan& plan,
               uint32_t* slab_offsets, uint32_t* edge_ids,
               std::vector<uint32_t>& cursor) {
  const AxisIndex& r = plan.rec;
  if (r.num_slabs == 0) return;
  uint32_t* off = slab_offsets + r.slab_start;
  std::fill(off, off + r.num_slabs + 1, 0u);
  for (int pass = 0; pass < 2; ++pass) {
    if (pass == 1) {
      off[0] = static_cast<uint32_t>(plan.entry_start);
      for (uint32_t s = 0; s < r.num_slabs; ++s) off[s + 1] += off[s];
      cursor.assign(off, off + r.num_slabs);
    }
    for (uint32_t i = 0; i < n; ++i) {
      const Point2D& a = v[i];
      const Point2D& b = v[i + 1 == n ? 0 : i + 1];
      if (a.x == b.x && a.y == b.y) continue;
      uint32_t s0, s1;
      edge_slab_range(coord_of(a, use_y), coord_of(b, use_y), r.pad, r.lo, r.inv_h,
                      r.num_slabs, s0, s1);
      for (uint32_t sl = s0; sl <= s1; ++sl) {
        if (pass == 0) {
          off[sl + 1]++;
        } else {
          edge_ids[cursor[sl]++] = i;
        }
      }
    }
  }
}

// Heuristic upper estimate of the kernel's eta_k for probes that pass the bbox filter,
// doubled for headroom. Soundness never depends on it: the kernel re-checks eta_k <= pad
// per pair and falls back to the linear scan otherwise.
float polygon_pad(const PolygonRecord& p) {
  const double u = 0x1p-24;
  const double ox = (double)p.origin_hi_x + (double)p.origin_lo_x;
  const double oy = (double)p.origin_hi_y + (double)p.origin_lo_y;
  const double d = std::max(std::max(std::fabs(p.min_x - ox), std::fabs(p.max_x - ox)),
                            std::max(std::fabs(p.min_y - oy), std::fabs(p.max_y - oy)));
  const double pn =
      std::max(std::max(std::fabs((double)p.min_x), std::fabs((double)p.max_x)),
               std::max(std::fabs((double)p.min_y), std::fabs((double)p.max_y)));
  const double eta =
      p.eta_poly + 3.0 * u * (d + p.eta_poly) + 0x1p-48 * (pn + p.eta_poly);
  return static_cast<float>(4.0 * eta);  // 2 (S_eta) * 2 (headroom)
}

std::vector<std::pair<size_t, size_t>> chunk_rings(const std::vector<RingRecord>& rings) {
  std::vector<std::pair<size_t, size_t>> chunks;
  size_t begin = 0;
  uint64_t acc = 0;
  for (size_t i = 0; i < rings.size(); ++i) {
    acc += rings[i].vertex_count;
    if (acc >= kChunkVertices || i + 1 == rings.size()) {
      chunks.emplace_back(begin, i + 1);
      begin = i + 1;
      acc = 0;
    }
  }
  return chunks;
}

}  // namespace

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
      buf_ring_index_(nil),
      buf_slab_offsets_(nil),
      buf_edge_ids_(nil),
      index_mode_(2),
      print_stats_(false),
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

    if (const char* env = std::getenv("SEDONA_METAL_REFINE_INDEX")) {
      if (std::strcmp(env, "off") == 0) index_mode_ = 0;
      if (std::strcmp(env, "x") == 0) index_mode_ = 1;
      if (std::strcmp(env, "xy") == 0) index_mode_ = 2;
    }
    if (const char* env = std::getenv("SEDONA_METAL_REFINE_STATS")) {
      print_stats_ = std::strcmp(env, "1") == 0;
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

MetalSpatialRefiner::~MetalSpatialRefiner() {
  if (print_stats_ && stats_[5] > 0) {
    fprintf(
        stderr,
        "[metal refine stats] rings_indexed_y=%llu rings_indexed_x=%llu entries=%llu "
        "index_bytes=%llu build_us=%llu vertices=%llu indexed_vertices=%llu pairs=%llu "
        "x_indexed=%llu y_ray=%llu y_indexed=%llu pad_fallback=%llu\n",
        stats_[0], stats_[1], stats_[2], stats_[3], stats_[4], stats_[11], stats_[10],
        stats_[5], stats_[6], stats_[7], stats_[8], stats_[9]);
  }
  clear();
}

void MetalSpatialRefiner::set_index_mode(int mode) {
  if (is_built_) {
    set_error("set_index_mode() must be called before finish_building()");
    throw std::runtime_error("Refiner already built");
  }
  index_mode_ = std::clamp(mode, 0, 2);
}

void MetalSpatialRefiner::get_stats(uint64_t* out, uint32_t n) const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  for (uint32_t i = 0; i < n; ++i) out[i] = i < kNumStats ? stats_[i] : 0;
}

void MetalSpatialRefiner::build_ring_index() {
  const auto t_start = std::chrono::steady_clock::now();
  const size_t num_rings = host_rings_.size();
  const size_t num_verts = host_vertices_.size();
  const Point2D* verts = host_vertices_.data();
  const RingRecord* rings = host_rings_.data();

  // One record per ring, zero-initialised (num_slabs == 0 => linear scan).
  size_t rec_size = std::max(num_rings * sizeof(RingIndexRecord), (size_t)16);
  buf_ring_index_ = [device_ newBufferWithLength:rec_size
                                         options:MTLResourceStorageModeShared];
  if (!buf_ring_index_) throw std::runtime_error("Metal buffer allocation failed");
  std::memset(buf_ring_index_.contents, 0, rec_size);
  RingIndexRecord* recs = static_cast<RingIndexRecord*>(buf_ring_index_.contents);

  std::vector<AxisPlan> plan_y(num_rings), plan_x(num_rings);
  AxisPlan* py = plan_y.data();
  AxisPlan* px = plan_x.data();
  uint64_t total_offsets = 0, total_entries = 0;
  const auto chunks = chunk_rings(host_rings_);
  const auto* chunk_ptr = chunks.data();
  dispatch_queue_t queue = dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0);

  if (index_mode_ > 0 && num_rings > 0) {
    // Per-ring pad from the owning (valid) polygon; rings of invalid polygons stay at 0.
    std::vector<float> pads(num_rings, 0.0f);
    for (const PolygonRecord& poly : host_polygons_) {
      if (!poly.is_valid) continue;
      const float pad = polygon_pad(poly) * index_pad_scale_;
      for (uint32_t p = 0; p < poly.part_count; ++p) {
        const size_t pi = (size_t)poly.part_start + p;
        if (pi >= host_parts_.size()) break;
        const PartRecord& part = host_parts_[pi];
        for (uint32_t r = 0; r < part.ring_count; ++r) {
          const size_t ri = (size_t)part.ring_start + r;
          if (ri < num_rings) pads[ri] = pad;
        }
      }
    }
    const float* pad_ptr = pads.data();
    const bool want_x = index_mode_ >= 2;

    dispatch_apply(chunks.size(), queue, ^(size_t c) {
      for (size_t i = chunk_ptr[c].first; i < chunk_ptr[c].second; ++i) {
        const RingRecord& ring = rings[i];
        if ((uint64_t)ring.vertex_start + ring.vertex_count > num_verts) continue;
        plan_axis(verts + ring.vertex_start, ring.vertex_count, true, pad_ptr[i], py[i]);
        if (want_x) {
          plan_axis(verts + ring.vertex_start, ring.vertex_count, false, pad_ptr[i],
                    px[i]);
        }
      }
    });

    for (size_t i = 0; i < num_rings; ++i) {
      for (AxisPlan* plan : {&plan_y[i], &plan_x[i]}) {
        if (plan->rec.num_slabs == 0) continue;
        plan->rec.slab_start = static_cast<uint32_t>(total_offsets);
        plan->entry_start = total_entries;
        total_offsets += (uint64_t)plan->rec.num_slabs + 1;
        total_entries += plan->entries;
      }
    }
    const uint64_t max_len = std::min<uint64_t>(device_.maxBufferLength, UINT32_MAX);
    if (total_offsets * 4 > max_len || total_entries * 4 > max_len) {
      // Too large to address with u32 offsets: keep the linear scan everywhere.
      total_offsets = 0;
      total_entries = 0;
    }
  }

  size_t off_size = std::max<size_t>(total_offsets * sizeof(uint32_t), 16);
  size_t ids_size = std::max<size_t>(total_entries * sizeof(uint32_t), 16);
  buf_slab_offsets_ = [device_ newBufferWithLength:off_size
                                           options:MTLResourceStorageModeShared];
  buf_edge_ids_ = [device_ newBufferWithLength:ids_size
                                       options:MTLResourceStorageModeShared];
  if (!buf_slab_offsets_ || !buf_edge_ids_) {
    throw std::runtime_error("Metal buffer allocation failed");
  }

  uint64_t rings_y = 0, rings_x = 0, indexed_verts = 0;
  if (total_entries > 0) {
    uint32_t* slab_offsets = static_cast<uint32_t*>(buf_slab_offsets_.contents);
    uint32_t* edge_ids = static_cast<uint32_t*>(buf_edge_ids_.contents);
    dispatch_apply(chunks.size(), queue, ^(size_t c) {
      std::vector<uint32_t> cursor;
      for (size_t i = chunk_ptr[c].first; i < chunk_ptr[c].second; ++i) {
        const RingRecord& ring = rings[i];
        fill_axis(verts + ring.vertex_start, ring.vertex_count, true, py[i], slab_offsets,
                  edge_ids, cursor);
        fill_axis(verts + ring.vertex_start, ring.vertex_count, false, px[i],
                  slab_offsets, edge_ids, cursor);
      }
    });
    // Publish the records only after their slabs are filled.
    for (size_t i = 0; i < num_rings; ++i) {
      recs[i].y_slabs = plan_y[i].rec;
      recs[i].x_slabs = plan_x[i].rec;
      if (plan_y[i].rec.num_slabs) {
        rings_y++;
        indexed_verts += rings[i].vertex_count;
      }
      if (plan_x[i].rec.num_slabs) rings_x++;
    }
  }

  allocated_bytes_ += rec_size + off_size + ids_size;
  const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t_start)
                      .count();
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_[0] = rings_y;
  stats_[1] = rings_x;
  stats_[2] = total_entries;
  stats_[3] = rec_size + off_size + ids_size;
  stats_[4] = static_cast<uint64_t>(us);
  stats_[10] = indexed_verts;
  stats_[11] = num_verts;
}

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
    buf_ring_index_ = nil;
    buf_slab_offsets_ = nil;
    buf_edge_ids_ = nil;
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      std::fill(std::begin(stats_), std::end(stats_), 0);
    }

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

    // Build the per-ring slab edge index while the host vectors are still alive.
    build_ring_index();

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
        [encoder setBuffer:buf_ring_index_ offset:0 atIndex:9];
        [encoder setBuffer:buf_slab_offsets_ offset:0 atIndex:10];
        [encoder setBuffer:buf_edge_ids_ offset:0 atIndex:11];

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
        // Low 2 bits: state. Bits 2..5: kernel path diagnostics (FLAG_* in refine.metal).
        uint64_t tally[4] = {0, 0, 0, 0};
        for (uint32_t i = 0; i < chunk_len; ++i) {
          const uint8_t raw = states_ptr[i];
          out_states[offset + i] = raw & 3u;
          tally[0] += (raw >> 2) & 1u;
          tally[1] += (raw >> 3) & 1u;
          tally[2] += (raw >> 4) & 1u;
          tally[3] += (raw >> 5) & 1u;
        }
        {
          std::lock_guard<std::mutex> lock(stats_mutex_);
          stats_[5] += chunk_len;
          for (int k = 0; k < 4; ++k) stats_[6 + k] += tally[k];
        }
      }
    }
  }
}
