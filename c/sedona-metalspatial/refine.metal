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
using namespace metal;

#ifndef BOUND_MODE
#define BOUND_MODE 0 // 0: certified v2 (default), 1: v1 det bound only, 2: band off only, 3: naive delta only
#endif

#define STATE_OUTSIDE 0
#define STATE_INSIDE 1
#define STATE_UNCERTAIN 2

struct Point2D {
    float x;
    float y;
};

struct CandidatePair {
    uint32_t polygon_idx;
    uint32_t point_idx;
};

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
    uint32_t is_valid;
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
    uint32_t is_valid;
    uint32_t _padding;
};

// Helper to evaluate a single linear ring against a probe point.
// Uses ray-casting along the positive x-axis.
// Adheres strictly to Section 2.3, 2.4, and 4.2 of design note v2.
inline uint32_t evaluate_ring(
    RingRecord ring,
    device const Point2D* vertices,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt)
{
    if (ring.vertex_count < 3) {
        return STATE_UNCERTAIN;
    }

    uint32_t crossings = 0;
    uint32_t n = ring.vertex_count;
    uint32_t start = ring.vertex_start;

    for (uint32_t i = 0; i < n; ++i) {
        Point2D v1 = vertices[start + i];
        Point2D v2 = vertices[start + ((i + 1) % n)];

        // Skip degenerate zero-length edges
        if (v1.x == v2.x && v1.y == v2.y) {
            continue;
        }

        // Relative coordinates: vector from probe point to vertex
        float x1 = v1.x - delta_x;
        float y1 = v1.y - delta_y;
        float x2 = v2.x - delta_x;
        float y2 = v2.y - delta_y;

#if BOUND_MODE == 2
        // Mode 2: band off only (no eta-band vertex trap)
#else
        // Section 2.4 Ray Straddle Vertex Protection:
        // Conservative eta_k-band rule:
        // If |y1| <= eta_k or |y2| <= eta_k, a vertex lies within the ambiguity band
        // of the ray. Traps potential apex/vertex grazing to avoid false outside results.
        if (metal::abs(y1) <= eta_k || metal::abs(y2) <= eta_k) {
            return STATE_UNCERTAIN;
        }
#endif

        // Certified straddle check:
        // Because |y1| > eta_k and |y2| > eta_k, signs are certified.
        // Edge straddles y = 0 iff y1 and y2 have opposite signs.
        bool straddles = (y1 > 0.0f) != (y2 > 0.0f);
        if (!straddles) {
            continue;
        }

        // Determinant
        float det = x1 * y2 - x2 * y1;

#if BOUND_MODE == 1
        // Mode 1: Flawed v1 determinant bound only:
        float max_coord = metal::max(metal::max(metal::abs(x1), metal::abs(x2)), metal::max(metal::abs(y1), metal::abs(y2)));
        float bound_det = 3.5f * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1)) + u_flt * max_coord;
#else
        // Section 2.3 Determinant and Forward Error Bound (v2 approved):
        // Shewchuk's A-bound: eps_arith = (3 + 16u) * u * (|x1*y2| + |x2*y1|)
        float eps_arith = (3.0f + 16.0f * u_flt) * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1));

        // Input perturbation: eps_input = eta_k * (|x1| + |x2| + |y1| + |y2|) + 2 * eta_k^2
        float eps_input = eta_k * (metal::abs(x1) + metal::abs(x2) + metal::abs(y1) + metal::abs(y2)) + 2.0f * eta_k * eta_k;

        // Total forward error bound with safety factor S = 2.0 (Section 2.3 D)
        float bound_det = 2.0f * (eps_arith + eps_input);
#endif

        // Ambiguous orientation check
        if (metal::abs(det) <= bound_det) {
            return STATE_UNCERTAIN;
        }

        // Section 4.2 (R5) Division-Free Certified Crossing Formula:
        // Edge crosses positive x-axis iff sign(det) == sign(y2 - y1)
        bool det_positive = (det > 0.0f);
        bool dy_positive = ((y2 - y1) > 0.0f);
        if (det_positive == dy_positive) {
            crossings++;
        }
    }

    return (crossings & 1) ? STATE_INSIDE : STATE_OUTSIDE;
}

// Stage 2 Robust Geometric Refinement Kernel:
// Evaluates point-in-polygon containment for candidate pairs into 3 states:
//   STATE_OUTSIDE (0), STATE_INSIDE (1), STATE_UNCERTAIN (2)
kernel void point_in_polygon_refine(
    device const CandidatePair*   candidate_pairs  [[buffer(0)]],
    device const PolygonRecord*   polygons         [[buffer(1)]],
    device const PartRecord*      parts            [[buffer(2)]],
    device const RingRecord*      rings            [[buffer(3)]],
    device const Point2D*         vertices         [[buffer(4)]],
    device const DecomposedPoint* points           [[buffer(5)]],
    device uint8_t*               out_states       [[buffer(6)]],
    constant uint32_t&            num_candidates   [[buffer(7)]],
    constant uint32_t&            num_polygons     [[buffer(8)]],
    uint                          thread_id        [[thread_position_in_grid]])
{
    if (thread_id >= num_candidates) return;

    CandidatePair pair = candidate_pairs[thread_id];
    if (pair.polygon_idx >= num_polygons) {
        out_states[thread_id] = STATE_UNCERTAIN;
        return;
    }

    PolygonRecord poly = polygons[pair.polygon_idx];
    DecomposedPoint pt = points[pair.point_idx];

    // Invalid/empty/NaN geometries are routed to uncertain
    if (poly.is_valid == 0 || pt.is_valid == 0) {
        out_states[thread_id] = STATE_UNCERTAIN;
        return;
    }

    // Bounding box filter check
    // Justification: Rounding is monotone, so p in [min, max] implies fl(p) in [fl(min), fl(max)].
    // The +/- eta_poly margin provides additional numerical safety.
    float px = pt.hi_x + pt.lo_x;
    float py = pt.hi_y + pt.lo_y;
    if (px < poly.min_x - poly.eta_poly || px > poly.max_x + poly.eta_poly ||
        py < poly.min_y - poly.eta_poly || py > poly.max_y + poly.eta_poly)
    {
        out_states[thread_id] = STATE_OUTSIDE;
        return;
    }

#if BOUND_MODE == 3
    // Mode 3: Naive single-precision subtraction only
    float delta_x = pt.hi_x - poly.origin_hi_x;
    float delta_y = pt.hi_y - poly.origin_hi_y;
#else
    // Section 2.2 step 4: Relative probe displacement Delta_tilde (double-single)
    float delta_hi_x = pt.hi_x - poly.origin_hi_x;
    float delta_lo_x = pt.lo_x - poly.origin_lo_x;
    float delta_x = delta_hi_x + delta_lo_x;

    float delta_hi_y = pt.hi_y - poly.origin_hi_y;
    float delta_lo_y = pt.lo_y - poly.origin_lo_y;
    float delta_y = delta_hi_y + delta_lo_y;
#endif

    // Section 2.3 & 2.4: Conservative eta_k calculation with S_eta = 2.0
    float delta_norm_inf = metal::max(metal::abs(delta_x), metal::abs(delta_y));
    float pt_norm_inf = metal::max(metal::abs(pt.hi_x), metal::abs(pt.hi_y));

    constexpr float u_flt = 5.9604645e-8f;             // 2^-24
    constexpr float two_neg_48_flt = 3.5527137e-15f;    // 2^-48

    float eta = poly.eta_poly + 3.0f * u_flt * delta_norm_inf + two_neg_48_flt * pt_norm_inf;
    float eta_k = 2.0f * eta; // S_eta = 2.0

    // Section 4.2 MultiPolygon & Part combination rules:
    bool any_part_inside = false;
    bool any_part_uncertain = false;

    for (uint32_t p = 0; p < poly.part_count; ++p) {
        PartRecord part = parts[poly.part_start + p];
        if (part.ring_count == 0) {
            any_part_uncertain = true;
            continue;
        }

        // Exterior ring (index 0 of part)
        RingRecord ext_ring = rings[part.ring_start];
        uint32_t ext_state = evaluate_ring(ext_ring, vertices, delta_x, delta_y, eta_k, u_flt);

        if (ext_state == STATE_OUTSIDE) {
            continue;
        }
        if (ext_state == STATE_UNCERTAIN) {
            any_part_uncertain = true;
            continue;
        }

        // ext_state == STATE_INSIDE: Check interior rings (holes)
        bool in_hole = false;
        bool hole_uncertain = false;

        for (uint32_t h = 1; h < part.ring_count; ++h) {
            RingRecord hole_ring = rings[part.ring_start + h];
            uint32_t hole_state = evaluate_ring(hole_ring, vertices, delta_x, delta_y, eta_k, u_flt);
            if (hole_state == STATE_INSIDE) {
                in_hole = true;
                break;
            }
            if (hole_state == STATE_UNCERTAIN) {
                hole_uncertain = true;
            }
        }

        if (in_hole) {
            continue;
        }
        if (hole_uncertain) {
            any_part_uncertain = true;
        } else {
            any_part_inside = true;
            break;
        }
    }

    if (any_part_inside) {
        out_states[thread_id] = STATE_INSIDE;
    } else if (any_part_uncertain) {
        out_states[thread_id] = STATE_UNCERTAIN;
    } else {
        out_states[thread_id] = STATE_OUTSIDE;
    }
}
