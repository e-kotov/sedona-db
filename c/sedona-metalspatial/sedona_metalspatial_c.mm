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

#import "sedona_metalspatial_c.h"
#include <cstdlib>
#include <cstring>
#include <exception>
#include <vector>
#import "spatial_index.hpp"

extern "C" {

int SedonaMetalIndexCreate(void** out_index) {
  if (!out_index) return -1;
  try {
    auto* idx = new MetalSpatialIndex();
    if (!idx->is_valid()) {
      delete idx;
      *out_index = nullptr;
      return -2;
    }
    *out_index = static_cast<void*>(idx);
    return 0;
  } catch (...) {
    *out_index = nullptr;
    return -1;
  }
}

int SedonaMetalIndexPushBuild(void* index, const float* rects, uint32_t count) {
  if (!index) return -1;
  if (count > 0 && !rects) return -2;
  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    if (!idx->push_build(rects, count)) {
      return -3;
    }
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(e.what());
    return -4;
  } catch (...) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(
        "Unknown C++ exception occurred");
    return -4;
  }
}

int SedonaMetalIndexFinish(void* index) {
  if (!index) return -1;
  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    if (!idx->finish_building()) {
      return -2;
    }
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(e.what());
    return -3;
  } catch (...) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(
        "Unknown C++ exception occurred");
    return -3;
  }
}

int SedonaMetalIndexProbe(void* index, const float* rects, uint32_t count,
                          uint32_t** out_build, uint32_t** out_probe, uint32_t* out_len) {
  if (!index || !out_build || !out_probe || !out_len) return -1;
  if (count > 0 && !rects) return -2;

  *out_build = nullptr;
  *out_probe = nullptr;
  *out_len = 0;

  if (count == 0) {
    return 0;
  }

  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    std::vector<uint32_t> build_res;
    std::vector<uint32_t> probe_res;
    if (!idx->probe(rects, count, build_res, probe_res)) {
      return -5;
    }

    uint32_t num_matches = static_cast<uint32_t>(build_res.size());
    *out_len = num_matches;
    if (num_matches > 0) {
      auto* b_buf = static_cast<uint32_t*>(std::malloc(num_matches * sizeof(uint32_t)));
      auto* p_buf = static_cast<uint32_t*>(std::malloc(num_matches * sizeof(uint32_t)));
      if (!b_buf || !p_buf) {
        std::free(b_buf);
        std::free(p_buf);
        *out_len = 0;
        return -3;
      }
      std::memcpy(b_buf, build_res.data(), num_matches * sizeof(uint32_t));
      std::memcpy(p_buf, probe_res.data(), num_matches * sizeof(uint32_t));
      *out_build = b_buf;
      *out_probe = p_buf;
    }
    return 0;
  } catch (const std::exception& e) {
    if (out_build) *out_build = nullptr;
    if (out_probe) *out_probe = nullptr;
    if (out_len) *out_len = 0;
    static_cast<MetalSpatialIndex*>(index)->set_last_error(e.what());
    return -4;
  } catch (...) {
    if (out_build) *out_build = nullptr;
    if (out_probe) *out_probe = nullptr;
    if (out_len) *out_len = 0;
    static_cast<MetalSpatialIndex*>(index)->set_last_error(
        "Unknown C++ exception occurred");
    return -4;
  }
}

void SedonaMetalIndexFreeResults(uint32_t* out_build, uint32_t* out_probe) {
  try {
    if (out_build) std::free(out_build);
    if (out_probe) std::free(out_probe);
  } catch (...) {
  }
}

void SedonaMetalIndexFree(void* index) {
  if (index) {
    try {
      delete static_cast<MetalSpatialIndex*>(index);
    } catch (...) {
    }
  }
}

int SedonaMetalIndexClear(void* index) {
  if (!index) return -1;
  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    idx->clear();
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(e.what());
    return -2;
  } catch (...) {
    static_cast<MetalSpatialIndex*>(index)->set_last_error(
        "Unknown C++ exception occurred");
    return -2;
  }
}

const char* SedonaMetalIndexGetLastError(void* index) {
  if (!index) return "Index handle is null";
  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    return idx->get_last_error();
  } catch (...) {
    return "Internal exception reading last error";
  }
}

uint64_t SedonaMetalIndexGetMemUsage(void* index) {
  if (!index) return 0;
  try {
    auto* idx = static_cast<MetalSpatialIndex*>(index);
    return idx->get_memory_usage();
  } catch (...) {
    return 0;
  }
}

// ============================================================================
// Metal Spatial Refiner C-ABI Implementations
// ============================================================================

#import "spatial_refiner.hpp"

static thread_local std::string g_last_refiner_error;

int SedonaMetalRefinerCreate(void** out_refiner) {
  if (!out_refiner) return -1;
  try {
    @autoreleasepool {
#ifdef ENABLE_TEST_INTERNALS
      auto* refiner = new MetalSpatialRefiner(nil, 0);
#else
      auto* refiner = new MetalSpatialRefiner(nil);
#endif
      *out_refiner = static_cast<void*>(refiner);
      return 0;
    }
  } catch (const std::exception& e) {
    g_last_refiner_error = e.what();
    *out_refiner = nullptr;
    return -1;
  } catch (...) {
    g_last_refiner_error = "Unknown exception in refiner constructor";
    *out_refiner = nullptr;
    return -1;
  }
}

#ifdef ENABLE_TEST_INTERNALS
int SedonaMetalRefinerCreateWithMode(void** out_refiner, int bound_mode) {
  if (!out_refiner) return -1;
  try {
    @autoreleasepool {
      auto* refiner = new MetalSpatialRefiner(nil, bound_mode);
      *out_refiner = static_cast<void*>(refiner);
      return 0;
    }
  } catch (const std::exception& e) {
    g_last_refiner_error = e.what();
    *out_refiner = nullptr;
    return -1;
  } catch (...) {
    g_last_refiner_error = "Unknown exception in refiner constructor";
    *out_refiner = nullptr;
    return -1;
  }
}

int SedonaMetalRefinerCreateWithRtConfig(void** out_refiner, int bound_mode,
                                         const SedonaMetalRtConfig* rt_config) {
  if (!out_refiner || !rt_config) return -1;
  try {
    @autoreleasepool {
      RtConfig cfg;
      cfg.enabled = rt_config->enabled;
      cfg.min_ring_vertices = rt_config->min_ring_vertices;
      cfg.segs_per_box = rt_config->segs_per_box;
      cfg.slot_base = rt_config->slot_base;
      cfg.collect_stats = rt_config->collect_stats;
      auto* refiner = new MetalSpatialRefiner(nil, bound_mode, &cfg);
      *out_refiner = static_cast<void*>(refiner);
      return 0;
    }
  } catch (const std::exception& e) {
    g_last_refiner_error = e.what();
    *out_refiner = nullptr;
    return -1;
  } catch (...) {
    g_last_refiner_error = "Unknown exception in refiner constructor";
    *out_refiner = nullptr;
    return -1;
  }
}
#endif

int SedonaMetalRefinerGetRtInfo(void* refiner, uint64_t* out_info) {
  if (!refiner || !out_info) return -1;
  try {
    static_cast<MetalSpatialRefiner*>(refiner)->get_rt_info(out_info);
    return 0;
  } catch (...) {
    return -1;
  }
}

int SedonaMetalRefinerPushPolygons(void* refiner, const void* polys, uint32_t poly_count,
                                   const void* parts, uint32_t part_count,
                                   const void* rings, uint32_t ring_count,
                                   const void* vertices, uint32_t vertex_count) {
  if (!refiner) return -1;
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    ref->push_polygons(static_cast<const PolygonRecord*>(polys), poly_count,
                       static_cast<const PartRecord*>(parts), part_count,
                       static_cast<const RingRecord*>(rings), ring_count,
                       static_cast<const Point2D*>(vertices), vertex_count);
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(e.what());
    return -2;
  } catch (...) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(
        "Unknown C++ exception occurred");
    return -2;
  }
}

int SedonaMetalRefinerFinish(void* refiner) {
  if (!refiner) return -1;
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    ref->finish_building();
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(e.what());
    return -2;
  } catch (...) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(
        "Unknown C++ exception occurred");
    return -2;
  }
}

int SedonaMetalRefinerRefine(void* refiner, const void* points, uint32_t point_count,
                             const uint32_t* candidate_build_indices,
                             const uint32_t* candidate_probe_indices,
                             uint32_t candidate_count, uint8_t* out_states) {
  if (!refiner) return -1;
  if (candidate_count > 0 &&
      (!candidate_build_indices || !candidate_probe_indices || !out_states))
    return -2;
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    ref->refine(static_cast<const DecomposedPoint*>(points), point_count,
                candidate_build_indices, candidate_probe_indices, candidate_count,
                out_states);
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(e.what());
    return -3;
  } catch (...) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(
        "Unknown C++ exception occurred");
    return -3;
  }
}

int SedonaMetalRefinerClear(void* refiner) {
  if (!refiner) return -1;
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    ref->clear();
    return 0;
  } catch (const std::exception& e) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(e.what());
    return -2;
  } catch (...) {
    static_cast<MetalSpatialRefiner*>(refiner)->set_last_error(
        "Unknown C++ exception occurred");
    return -2;
  }
}

void SedonaMetalRefinerFree(void* refiner) {
  if (refiner) {
    try {
      delete static_cast<MetalSpatialRefiner*>(refiner);
    } catch (...) {
    }
  }
}

const char* SedonaMetalRefinerGetLastError(void* refiner) {
  if (!refiner) {
    if (!g_last_refiner_error.empty()) {
      return g_last_refiner_error.c_str();
    }
    return "Refiner handle is null";
  }
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    return ref->get_last_error();
  } catch (...) {
    return "Internal exception reading last error";
  }
}

const char* SedonaMetalRefinerGetDeviceName(void* refiner) {
  if (!refiner) return "None";
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    return ref->get_device_name();
  } catch (...) {
    return "Unknown";
  }
}

uint64_t SedonaMetalRefinerGetMemUsage(void* refiner) {
  if (!refiner) return 0;
  try {
    auto* ref = static_cast<MetalSpatialRefiner*>(refiner);
    return ref->get_memory_usage();
  } catch (...) {
    return 0;
  }
}
}
