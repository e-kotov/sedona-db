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

// ----------------------------------------------------------------------------
// Per-ring slab edge index (1D stabbing index along one axis)
// ----------------------------------------------------------------------------
// A ring with `num_slabs > 0` on an axis owns a uniform grid of slabs over
// [lo, hi] on that axis. `slab_offsets[slab_start + s] .. slab_offsets[slab_start + s + 1]`
// delimits the ring-local edge ids stored in `edge_ids` for slab s. The builder
// (spatial_refiner.mm, build_ring_index) guarantees, for every probe displacement d
// that select_slab() maps to slab s, that slab s lists EVERY edge that the linear
// scan would not skip as a no-op, provided eta_k (padded) <= pad. See the soundness
// note above select_slab().
struct AxisIndex {
    float lo;            // float <= (ring min on axis) - pad
    float hi;            // float >= (ring max on axis) + pad
    float inv_h;         // slabs per unit length; the grid is DEFINED by (lo, inv_h)
    float pad;           // edges were inserted with their interval widened by pad
    uint32_t slab_start; // offset into slab_offsets (num_slabs + 1 entries)
    uint32_t num_slabs;  // 0: ring is not indexed on this axis
};

struct RingIndexRecord {
    AxisIndex y_slabs; // serves the +x ray (stabbing query on y)
    AxisIndex x_slabs; // serves the +y ray (stabbing query on x)
};

#define FLAG_X_INDEXED 1u    // some ring evaluated the +x ray through the slab index
#define FLAG_Y_RAY 2u        // the +y retry ray ran on some ring
#define FLAG_Y_INDEXED 4u    // the +y retry ray used the slab index
#define FLAG_PAD_FALLBACK 8u // an indexed ring fell back to the linear scan (eta_k > pad etc.)

#define SLAB_LINEAR 0u
#define SLAB_INDEXED 1u

// Chooses the edge subset for a ray whose stabbing coordinate (relative to the polygon
// origin) is d. Returns SLAB_LINEAR when the caller must scan all edges, or
// SLAB_INDEXED with [begin, end) into edge_ids (possibly empty).
//
// Soundness (design_refiner.md 2.3-2.4 frame). The per-edge body forms
// c_i = fl(v_i.c - d) for the stabbing coordinate c and is a no-op (no trap, no
// crossing) whenever both c_1, c_2 > eta_k or both < -eta_k. So an edge can matter only
// if fl(cmin - d) <= eta_k and fl(cmax - d) >= -eta_k. Rounding is monotone and
// succ(eta_k) is a normal float, so this implies cmin - d < succ(eta_k) and
// cmax - d > -succ(eta_k) in exact arithmetic. q = fl(eta_k * (1 + 2^-20)) >= succ(eta_k),
// and we only use the index when q <= pad, hence an edge that matters satisfies
//     cmin - pad < d < cmax + pad.                                              (*)
// (1) d < lo or d > hi: no edge satisfies (*) because lo <= ring_min - pad and
//     hi >= ring_max + pad; zero crossings, no trap: identical to the linear scan.
// (2) otherwise s = trunc(min(fl(fl(d - lo) * inv_h), K - 1)). The exact value
//     t* = (d - lo) * inv_h obeys |t - t*| <= 2.0001 u t* + 1e-8 (the last term covers
//     GPUs that flush subnormals; the builder enforces inv_h <= 1e30). With K <= 2^15
//     this is < 0.008 for t* <= 2K; the builder lists each edge in slabs
//     floor(t_lo - 1/32) .. floor(t_hi + 1/32) (clamped), where t_lo/t_hi are t* at the
//     ends of (*) computed in f64 with a checked error <= 0.01. For t* > 2K both sides
//     clamp to K - 1. Hence slab s lists every edge satisfying (*).
// Each edge appears at most once per slab and exactly one slab is visited, so crossing
// parity cannot double count. NaN / inf / tiny eta_k all route to SLAB_LINEAR.
inline uint32_t select_slab(
    AxisIndex ax,
    float d,
    float eta_k,
    device const uint32_t* slab_offsets,
    thread uint32_t& begin,
    thread uint32_t& end,
    thread uint32_t& flags)
{
    begin = 0;
    end = 0;
    if (ax.num_slabs == 0) {
        return SLAB_LINEAR;
    }
    float q = eta_k * 1.00000095367431640625f; // 1 + 2^-20
    if (!(q <= ax.pad) || !(eta_k >= 1e-30f) || !(d == d)) {
        flags |= FLAG_PAD_FALLBACK;
        return SLAB_LINEAR;
    }
    if (d < ax.lo || d > ax.hi) {
        return SLAB_INDEXED; // empty range
    }
    float t = (d - ax.lo) * ax.inv_h;
    t = metal::min(t, float(ax.num_slabs - 1));
    uint32_t s = uint32_t(t);
    begin = slab_offsets[ax.slab_start + s];
    end = slab_offsets[ax.slab_start + s + 1];
    return SLAB_INDEXED;
}

// Helper to evaluate a single linear ring against a probe point.
// Uses ray-casting along the positive x-axis.
// Adheres strictly to Section 2.3, 2.4, and 4.2 of design note v2.
// Primary ray along positive x-axis: { (x, 0) : x >= 0 }
//
// The linear scan and the slab-indexed scan share this single loop body: they differ
// only in how the edge id `i` is obtained, so the per-edge certified logic cannot drift.
// The result is order independent (any trap -> Uncertain, else crossing parity).
inline uint32_t evaluate_ring_x(
    RingRecord ring,
    AxisIndex ax,
    device const Point2D* vertices,
    device const uint32_t* slab_offsets,
    device const uint32_t* edge_ids,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt,
    thread uint32_t& flags)
{
    if (ring.vertex_count < 3) {
        return STATE_UNCERTAIN;
    }

    uint32_t crossings = 0;
    uint32_t n = ring.vertex_count;
    uint32_t start = ring.vertex_start;

    uint32_t begin, end;
    bool indexed = select_slab(ax, delta_y, eta_k, slab_offsets, begin, end, flags) == SLAB_INDEXED;
    uint32_t count = indexed ? (end - begin) : n;
    if (indexed) {
        flags |= FLAG_X_INDEXED;
    }

    for (uint32_t k = 0; k < count; ++k) {
        uint32_t i = indexed ? edge_ids[begin + k] : k;
        uint32_t j = (i + 1 == n) ? 0 : i + 1;
        Point2D v1 = vertices[start + i];
        Point2D v2 = vertices[start + j];

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
#elif BOUND_MODE == 4
        // Mode 4: Legacy unrestricted band rule without edge cull
        if (metal::abs(y1) <= eta_k || metal::abs(y2) <= eta_k) {
            return STATE_UNCERTAIN;
        }
#elif BOUND_MODE == 5
        // Mode 5: Unsound trap (skips vertex trap if x < -eta_k on spanning edges)
        if (metal::max(x1, x2) < -eta_k) {
            continue;
        }
        if ((metal::abs(y1) <= eta_k && x1 >= -eta_k) ||
            (metal::abs(y2) <= eta_k && x2 >= -eta_k)) {
            return STATE_UNCERTAIN;
        }
#else
        // Edge cull: An edge lying entirely to the left of the positive x-ray (max(x1, x2) < -eta_k)
        // cannot cross or graze the ray { (x, 0) : x >= 0 }.
        if (metal::max(x1, x2) < -eta_k) {
            continue;
        }

        // Section 2.4 Ray Straddle Vertex Protection:
        // Unconditional eta_k-band trap on all non-culled edges (including spanning edges).
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

        // Determinant: x1 * y2 - x2 * y1
        float det = x1 * y2 - x2 * y1;

#if BOUND_MODE == 1
        // Mode 1: Flawed v1 determinant bound only
        float max_coord = metal::max(metal::max(metal::abs(x1), metal::abs(x2)), metal::max(metal::abs(y1), metal::abs(y2)));
        float bound_det = 3.5f * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1)) + u_flt * max_coord;
#else
        // Section 2.3 Determinant and Forward Error Bound (v2 approved)
        float eps_arith = (3.0f + 16.0f * u_flt) * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1));
        float eps_input = eta_k * (metal::abs(x1) + metal::abs(x2) + metal::abs(y1) + metal::abs(y2)) + 2.0f * eta_k * eta_k;
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

// Secondary ray along positive y-axis: { (0, y) : y >= 0 }
inline uint32_t evaluate_ring_y(
    RingRecord ring,
    AxisIndex ax,
    device const Point2D* vertices,
    device const uint32_t* slab_offsets,
    device const uint32_t* edge_ids,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt,
    thread uint32_t& flags)
{
    if (ring.vertex_count < 3) {
        return STATE_UNCERTAIN;
    }

    uint32_t crossings = 0;
    uint32_t n = ring.vertex_count;
    uint32_t start = ring.vertex_start;

    uint32_t begin, end;
    bool indexed = select_slab(ax, delta_x, eta_k, slab_offsets, begin, end, flags) == SLAB_INDEXED;
    uint32_t count = indexed ? (end - begin) : n;
    flags |= FLAG_Y_RAY;
    if (indexed) {
        flags |= FLAG_Y_INDEXED;
    }

    for (uint32_t k = 0; k < count; ++k) {
        uint32_t i = indexed ? edge_ids[begin + k] : k;
        uint32_t j = (i + 1 == n) ? 0 : i + 1;
        Point2D v1 = vertices[start + i];
        Point2D v2 = vertices[start + j];

        if (v1.x == v2.x && v1.y == v2.y) {
            continue;
        }

        float x1 = v1.x - delta_x;
        float y1 = v1.y - delta_y;
        float x2 = v2.x - delta_x;
        float y2 = v2.y - delta_y;

        // Edge cull: An edge lying entirely below the positive y-ray (max(y1, y2) < -eta_k)
        // cannot cross or graze the ray { (0, y) : y >= 0 }.
        if (metal::max(y1, y2) < -eta_k) {
            continue;
        }

        // Vertex protection for positive y-ray: trap vertices with |x| <= eta_k
        if (metal::abs(x1) <= eta_k || metal::abs(x2) <= eta_k) {
            return STATE_UNCERTAIN;
        }

        // Certified straddle check across vertical axis x = 0:
        bool straddles = (x1 > 0.0f) != (x2 > 0.0f);
        if (!straddles) {
            continue;
        }

        // Intercept with x = 0 is y* = (x2 * y1 - x1 * y2) / (x2 - x1)
        float det_y = x2 * y1 - x1 * y2;

        float eps_arith = (3.0f + 16.0f * u_flt) * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1));
        float eps_input = eta_k * (metal::abs(x1) + metal::abs(x2) + metal::abs(y1) + metal::abs(y2)) + 2.0f * eta_k * eta_k;
        float bound_det = 2.0f * (eps_arith + eps_input);

        if (metal::abs(det_y) <= bound_det) {
            return STATE_UNCERTAIN;
        }

        // Edge crosses positive y-axis iff sign(det_y) == sign(x2 - x1)
        bool det_positive = (det_y > 0.0f);
        bool dx_positive = ((x2 - x1) > 0.0f);
        if (det_positive == dx_positive) {
            crossings++;
        }
    }

    return (crossings & 1) ? STATE_INSIDE : STATE_OUTSIDE;
}

// Two-ray certified ring evaluator:
// Evaluates primary +x ray; on uncertainty, retries with orthogonal +y ray.
inline uint32_t evaluate_ring(
    RingRecord ring,
    RingIndexRecord ring_index,
    device const Point2D* vertices,
    device const uint32_t* slab_offsets,
    device const uint32_t* edge_ids,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt,
    thread uint32_t& flags)
{
    uint32_t state_x = evaluate_ring_x(ring, ring_index.y_slabs, vertices, slab_offsets, edge_ids,
                                       delta_x, delta_y, eta_k, u_flt, flags);
    if (state_x != STATE_UNCERTAIN) {
        return state_x;
    }

#if BOUND_MODE == 4
    // Mode 4: Legacy single-ray baseline
    return STATE_UNCERTAIN;
#else
    // Retry with orthogonal +y ray to eliminate horizontal ray direction artifacts
    return evaluate_ring_y(ring, ring_index.x_slabs, vertices, slab_offsets, edge_ids,
                           delta_x, delta_y, eta_k, u_flt, flags);
#endif
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
    device const RingIndexRecord* ring_index       [[buffer(9)]],
    device const uint32_t*        slab_offsets     [[buffer(10)]],
    device const uint32_t*        edge_ids         [[buffer(11)]],
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
    uint32_t flags = 0;

    for (uint32_t p = 0; p < poly.part_count; ++p) {
        PartRecord part = parts[poly.part_start + p];
        if (part.ring_count == 0) {
            any_part_uncertain = true;
            continue;
        }

        // Exterior ring (index 0 of part)
        RingRecord ext_ring = rings[part.ring_start];
        uint32_t ext_state = evaluate_ring(ext_ring, ring_index[part.ring_start], vertices,
                                           slab_offsets, edge_ids, delta_x, delta_y, eta_k, u_flt,
                                           flags);

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
            uint32_t hole_state = evaluate_ring(hole_ring, ring_index[part.ring_start + h], vertices,
                                                slab_offsets, edge_ids, delta_x, delta_y, eta_k,
                                                u_flt, flags);
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

    // The low 2 bits carry the state; bits 2..5 carry path diagnostics (FLAG_*), which the
    // host strips (and optionally tallies) before states leave spatial_refiner.mm.
    uint32_t state = STATE_OUTSIDE;
    if (any_part_inside) {
        state = STATE_INSIDE;
    } else if (any_part_uncertain) {
        state = STATE_UNCERTAIN;
    }
    out_states[thread_id] = uint8_t(state | (flags << 2));
}
