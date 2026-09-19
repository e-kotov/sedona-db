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

#ifdef __METAL_VERSION__
#include <metal_stdlib>
using namespace metal;
#else
#include <cstdint>
#include <cmath>
#include <algorithm>
#endif

// Contiguous 2D point coordinate (GeoArrow flat coordinates)
struct Point2D {
    float x;
    float y;

#ifndef __METAL_VERSION__
    bool operator==(const Point2D& o) const {
        return x == o.x && y == o.y;
    }
#endif
};

#ifndef SEDONA_BOUNDING_BOX_DEFINED
#define SEDONA_BOUNDING_BOX_DEFINED
// 2D Axis-Aligned Bounding Box
struct BoundingBox {
    float xmin;
    float ymin;
    float xmax;
    float ymax;
};
#endif

// Ring descriptor referencing a contiguous slice of Point2D vertices
struct PolygonRing {
    uint32_t start_idx;   // Offset into the vertices buffer
    uint32_t num_points;  // Number of vertices in the ring
};

// Polygon descriptor matching GeoArrow nested list offsets
// Outer ring is at rings[outer_ring_idx]
// Interior rings (holes) are at rings[outer_ring_idx + 1 .. outer_ring_idx + num_interior_rings]
struct PolygonGeom {
    uint32_t outer_ring_idx;
    uint32_t num_interior_rings;
};

// Candidate match pair (from Stage 1 AABB bounding box filter)
struct CandidatePair {
    uint32_t polygon_idx;
    uint32_t point_idx;

#ifndef __METAL_VERSION__
    bool operator==(const CandidatePair& o) const {
        return polygon_idx == o.polygon_idx && point_idx == o.point_idx;
    }
    bool operator<(const CandidatePair& o) const {
        if (polygon_idx != o.polygon_idx) return polygon_idx < o.polygon_idx;
        return point_idx < o.point_idx;
    }
#endif
};

// Point Location relative to a polygon or ring
enum PointLocation : uint32_t {
    kPointInside = 0,
    kPointBoundary = 1,
    kPointOutside = 2
};

// 3-state classification for robust geometric refiner
#define STATE_OUTSIDE 0
#define STATE_INSIDE 1
#define STATE_UNCERTAIN 2

// Flat multi-polygon hierarchy records matching design note v2
struct PolygonRecord {
    float min_x;
    float min_y;
    float max_x;
    float max_y;
    float origin_hi_x;
    float origin_hi_y;
    float origin_lo_x;
    float origin_lo_y;
    float eta_poly;
    uint32_t part_start;
    uint32_t part_count;
    uint32_t is_valid; // 1 if polygon is valid and parsable, 0 if empty/NaN/invalid
};

struct PartRecord {
    uint32_t ring_start;
    uint32_t ring_count;
};

struct RingRecord {
    uint32_t vertex_start;
    uint32_t vertex_count;
};

struct DecomposedPoint {
    float hi_x;
    float hi_y;
    float lo_x;
    float lo_y;
    uint32_t is_valid; // 1 if point is valid, 0 if empty/NaN/invalid
    uint32_t _padding;
};

