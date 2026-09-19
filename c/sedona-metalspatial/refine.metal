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

#include <metal_stdlib>
#include "geom_types.hpp"
using namespace metal;

// 2D Line segment orientation:
// Returns:
//   1: q is strictly to the right of p1->p2
//  -1: q is strictly to the left of p1->p2
//   0: q is collinear with p1->p2
inline int get_orientation(Point2D p1, Point2D p2, Point2D q) {
    float d_x = q.x - p1.x;
    float d_y = q.y - p1.y;
    if (metal::abs(d_x) <= 1e-7f && metal::abs(d_y) <= 1e-7f) {
        return 0;
    }
    float v1 = d_x * (p2.y - p1.y);
    float v2 = (p2.x - p1.x) * d_y;
    if (metal::abs(v1 - v2) <= 1e-7f) {
        return 0;
    }
    return (v1 - v2 < 0.0f) ? -1 : 1;
}

// Check whether collinear point q lies within bounding box of segment (p1, p2)
inline bool segment_covers(Point2D p1, Point2D p2, Point2D q) {
    float min_x = metal::min(p1.x, p2.x) - 1e-6f;
    float max_x = metal::max(p1.x, p2.x) + 1e-6f;
    float min_y = metal::min(p1.y, p2.y) - 1e-6f;
    float max_y = metal::max(p1.y, p2.y) + 1e-6f;
    return (q.x >= min_x && q.x <= max_x && q.y >= min_y && q.y <= max_y);
}

// Locate point in a single linear ring using winding number algorithm
inline PointLocation locate_point_in_ring(
    Point2D p,
    device const Point2D* vertices,
    uint32_t start_idx,
    uint32_t num_points)
{
    if (num_points < 3) return kPointOutside;

    Point2D first_pt = vertices[start_idx];
    Point2D last_pt = vertices[start_idx + num_points - 1];
    bool is_closed = (first_pt.x == last_pt.x && first_pt.y == last_pt.y);
    uint32_t num_segments = is_closed ? (num_points - 1) : num_points;

    int wn = 0;

    for (uint32_t i = 0; i < num_segments; ++i) {
        Point2D p1 = vertices[start_idx + i];
        Point2D p2 = vertices[start_idx + ((i + 1) % num_points)];

        // Zero-length segments are ignored
        if (p1.x == p2.x && p1.y == p2.y) continue;

        int side = get_orientation(p1, p2, p);
        if (side == 0) {
            if (segment_covers(p1, p2, p)) {
                return kPointBoundary;
            }
        }

        bool is_rising = (p1.y <= p.y) && (p.y < p2.y) && (side == 1);
        bool is_falling = (p2.y <= p.y) && (p.y < p1.y) && (side == -1);
        wn += (is_rising ? 1 : 0) - (is_falling ? 1 : 0);
    }

    if (wn == 0) return kPointOutside;
    return kPointInside;
}

// Exact point location in a polygon (outer ring + interior hole rings)
inline PointLocation locate_point_in_polygon(
    Point2D p,
    PolygonGeom poly,
    device const PolygonRing* rings,
    device const Point2D* vertices)
{
    // 1. Check outer ring
    PolygonRing outer = rings[poly.outer_ring_idx];
    PointLocation outer_loc = locate_point_in_ring(p, vertices, outer.start_idx, outer.num_points);

    if (outer_loc == kPointOutside) {
        return kPointOutside;
    }

    PointLocation rloc = outer_loc;

    // 2. Check interior rings (holes)
    for (uint32_t h = 0; h < poly.num_interior_rings; ++h) {
        PolygonRing hole = rings[poly.outer_ring_idx + 1 + h];
        PointLocation hole_loc = locate_point_in_ring(p, vertices, hole.start_idx, hole.num_points);

        if (hole_loc == kPointInside) {
            // Inside a hole -> outside the polygon
            return kPointOutside;
        }
        if (hole_loc == kPointBoundary) {
            rloc = kPointBoundary;
        }
    }

    return rloc;
}

// Stage 2 Geometric Refinement Kernel:
// Dispatched with 1 thread per CandidatePair.
// Points located inside outer ring and outside all holes are retained.
kernel void point_in_polygon_refine(
    device const CandidatePair* candidate_pairs  [[buffer(0)]],
    device const PolygonGeom*   polygons         [[buffer(1)]],
    device const PolygonRing*   rings            [[buffer(2)]],
    device const Point2D*       vertices         [[buffer(3)]],
    device const Point2D*       points           [[buffer(4)]],
    device CandidatePair*       output_refined   [[buffer(5)]],
    device atomic_uint*         refined_count    [[buffer(6)]],
    constant uint&              num_candidates   [[buffer(7)]],
    constant uint&              max_results      [[buffer(8)]],
    uint                        thread_id        [[thread_position_in_grid]])
{
    if (thread_id >= num_candidates) return;

    CandidatePair pair = candidate_pairs[thread_id];
    PolygonGeom poly = polygons[pair.polygon_idx];
    Point2D pt = points[pair.point_idx];

    PointLocation loc = locate_point_in_polygon(pt, poly, rings, vertices);

    if (loc != kPointOutside) {
        uint slot = atomic_fetch_add_explicit(refined_count, 1, memory_order_relaxed);
        if (slot < max_results) {
            output_refined[slot] = pair;
        }
    }
}
