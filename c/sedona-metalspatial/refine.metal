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
#ifndef RT_ENABLE
#define RT_ENABLE 0 // 1: large rings are evaluated through the ray-traced edge index
#endif
#ifndef RT_STATS
#define RT_STATS 0 // 1: accumulate diagnostic counters (tests and tuning only)
#endif
#if RT_ENABLE
#include <metal_raytracing>
#endif
using namespace metal;
#if RT_ENABLE
using namespace metal::raytracing;
#endif

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

#define EDGE_NONE 0
#define EDGE_CROSSING 1
#define EDGE_UNCERTAIN 2

// Per-edge certified classification against the primary ray along the positive
// x-axis: { (x, 0) : x >= 0 }. Shared by the linear scan and the ray-traced edge
// index path so the two cannot drift.
// Adheres strictly to Section 2.3, 2.4, and 4.2 of design note v2.
// Returns EDGE_NONE (no contribution), EDGE_CROSSING, or EDGE_UNCERTAIN.
inline uint32_t classify_edge_x(
    Point2D v1,
    Point2D v2,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt)
{
    // Skip degenerate zero-length edges
    if (v1.x == v2.x && v1.y == v2.y) {
        return EDGE_NONE;
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
        return EDGE_UNCERTAIN;
    }
#elif BOUND_MODE == 5
    // Mode 5: Unsound trap (skips vertex trap if x < -eta_k on spanning edges)
    if (metal::max(x1, x2) < -eta_k) {
        return EDGE_NONE;
    }
    if ((metal::abs(y1) <= eta_k && x1 >= -eta_k) ||
        (metal::abs(y2) <= eta_k && x2 >= -eta_k)) {
        return EDGE_UNCERTAIN;
    }
#else
    // Edge cull: An edge lying entirely to the left of the positive x-ray (max(x1, x2) < -eta_k)
    // cannot cross or graze the ray { (x, 0) : x >= 0 }.
    if (metal::max(x1, x2) < -eta_k) {
        return EDGE_NONE;
    }

    // Section 2.4 Ray Straddle Vertex Protection:
    // Unconditional eta_k-band trap on all non-culled edges (including spanning edges).
    if (metal::abs(y1) <= eta_k || metal::abs(y2) <= eta_k) {
        return EDGE_UNCERTAIN;
    }
#endif

    // Certified straddle check:
    // Because |y1| > eta_k and |y2| > eta_k, signs are certified.
    // Edge straddles y = 0 iff y1 and y2 have opposite signs.
    bool straddles = (y1 > 0.0f) != (y2 > 0.0f);
    if (!straddles) {
        return EDGE_NONE;
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
        return EDGE_UNCERTAIN;
    }

    // Section 4.2 (R5) Division-Free Certified Crossing Formula:
    // Edge crosses positive x-axis iff sign(det) == sign(y2 - y1)
    bool det_positive = (det > 0.0f);
    bool dy_positive = ((y2 - y1) > 0.0f);
    return (det_positive == dy_positive) ? EDGE_CROSSING : EDGE_NONE;
}

// Per-edge certified classification against the secondary ray along the positive
// y-axis: { (0, y) : y >= 0 }. Shared by the linear scan and the ray-traced path.
inline uint32_t classify_edge_y(
    Point2D v1,
    Point2D v2,
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt)
{
    if (v1.x == v2.x && v1.y == v2.y) {
        return EDGE_NONE;
    }

    float x1 = v1.x - delta_x;
    float y1 = v1.y - delta_y;
    float x2 = v2.x - delta_x;
    float y2 = v2.y - delta_y;

    // Edge cull: An edge lying entirely below the positive y-ray (max(y1, y2) < -eta_k)
    // cannot cross or graze the ray { (0, y) : y >= 0 }.
    if (metal::max(y1, y2) < -eta_k) {
        return EDGE_NONE;
    }

    // Vertex protection for positive y-ray: trap vertices with |x| <= eta_k
    if (metal::abs(x1) <= eta_k || metal::abs(x2) <= eta_k) {
        return EDGE_UNCERTAIN;
    }

    // Certified straddle check across vertical axis x = 0:
    bool straddles = (x1 > 0.0f) != (x2 > 0.0f);
    if (!straddles) {
        return EDGE_NONE;
    }

    // Intercept with x = 0 is y* = (x2 * y1 - x1 * y2) / (x2 - x1)
    float det_y = x2 * y1 - x1 * y2;

    float eps_arith = (3.0f + 16.0f * u_flt) * u_flt * (metal::abs(x1 * y2) + metal::abs(x2 * y1));
    float eps_input = eta_k * (metal::abs(x1) + metal::abs(x2) + metal::abs(y1) + metal::abs(y2)) + 2.0f * eta_k * eta_k;
    float bound_det = 2.0f * (eps_arith + eps_input);

    if (metal::abs(det_y) <= bound_det) {
        return EDGE_UNCERTAIN;
    }

    // Edge crosses positive y-axis iff sign(det_y) == sign(x2 - x1)
    bool det_positive = (det_y > 0.0f);
    bool dx_positive = ((x2 - x1) > 0.0f);
    return (det_positive == dx_positive) ? EDGE_CROSSING : EDGE_NONE;
}

// Linear scan of every edge of a ring against the +x ray.
inline uint32_t evaluate_ring_x(
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
        uint32_t edge = classify_edge_x(v1, v2, delta_x, delta_y, eta_k, u_flt);
        if (edge == EDGE_UNCERTAIN) {
            return STATE_UNCERTAIN;
        }
        crossings += edge;
    }

    return (crossings & 1) ? STATE_INSIDE : STATE_OUTSIDE;
}

// Linear scan of every edge of a ring against the +y ray.
inline uint32_t evaluate_ring_y(
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
        uint32_t edge = classify_edge_y(v1, v2, delta_x, delta_y, eta_k, u_flt);
        if (edge == EDGE_UNCERTAIN) {
            return STATE_UNCERTAIN;
        }
        crossings += edge;
    }

    return (crossings & 1) ? STATE_INSIDE : STATE_OUTSIDE;
}

#if RT_ENABLE
#define RT_NO_SLOT 0xFFFFFFFFu
// Capacity of the per-thread duplicate-report filter. The host guarantees
// prim_count <= RT_MAX_BOXES_PER_RING for every indexed ring by widening
// segs_per_box for very large rings.
#define RT_MAX_BOXES_PER_RING 4096u
#define RT_SEEN_WORDS (RT_MAX_BOXES_PER_RING / 32u)

// Ray-traced edge index record of one indexed ring (see spatial_refiner.mm for how
// every field is derived and the DESIGN NOTE below for the soundness argument).
struct RtRingInfo {
    uint32_t first_prim;   // first bounding-box primitive id of this ring
    uint32_t prim_count;   // number of boxes (segment groups) of this ring
    uint32_t segs_per_box; // consecutive ring segments covered by each box
    float z;               // exact integer-valued z slab centre of this ring
    float scale;           // exact power of two mapping ring-local coords into [-1, 1]
    float eta_limit;       // largest eta_k the box padding of this ring covers
    float ray_back;        // how far behind the probe the ray starts (scaled units)
    uint32_t _padding;
};

#define RT_STAT_RING_EVALS_X 0
#define RT_STAT_RING_EVALS_Y 1
#define RT_STAT_BOX_REPORTS 2
#define RT_STAT_DUPLICATE_REPORTS 3
#define RT_STAT_FOREIGN_REPORTS 4
#define RT_STAT_ETA_FALLBACKS 5
#define RT_STAT_EDGES_VISITED 6
#define RT_STAT_PAIRS_WITH_RT 7

#if RT_STATS
#define RT_STAT_ADD(idx, val) atomic_fetch_add_explicit(&rt_stats[idx], (val), memory_order_relaxed)
#else
#define RT_STAT_ADD(idx, val)
#endif

// DESIGN NOTE (ray-traced edge index).
// The linear scan result depends only on the set of "contributing" edges: an edge
// contributes (crossing or uncertain) only if it is not culled and it is trapped or
// straddles. For the +x ray this implies, in ring-local f32 coordinates (ax, ay), (bx, by)
// and with e' = eta_k * (1 + 2u):
//     min(ay, by) - e' <= delta_y <= max(ay, by) + e'   and   max(ax, bx) + e' >= delta_x
// (fl(a - d) <= eta_k implies a - d <= eta_k + ulp(eta_k) / 2 because rounding is monotone).
// Symmetrically for the +y ray. Every box is the exact 2D bound of its segments padded on
// all four sides by pad >= scale * eta_limit * (1 + 2u) + traversal slack, and the kernel
// only takes this path when eta_k <= eta_limit. Hence the ray, which starts ray_back behind
// the probe, passes through the INTERIOR of the padded box of every contributing edge with a
// clearance of at least the traversal slack from the box faces in x and y, and 0.25 in z.
// Visiting extra boxes is harmless: every visited edge runs the same classify_edge_* body,
// and non-contributing edges return EDGE_NONE by construction. What is NOT documented by
// Apple, and is therefore an assumption, is that traversal reports every bounding-box
// primitive whose interior the ray crosses with that clearance.
// Duplicate reports of one primitive are filtered exactly by a per-thread bitset.
inline uint32_t evaluate_ring_rt(
    RingRecord ring,
    RtRingInfo info,
    bool y_ray,
    device const Point2D* vertices,
    primitive_acceleration_structure accel,
#if RT_STATS
    device atomic_uint* rt_stats,
#endif
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt)
{
    if (ring.vertex_count < 3) {
        return STATE_UNCERTAIN;
    }
    RT_STAT_ADD(y_ray ? RT_STAT_RING_EVALS_Y : RT_STAT_RING_EVALS_X, 1);

    uint32_t n = ring.vertex_count;
    uint32_t start = ring.vertex_start;
    uint32_t segs = info.segs_per_box;

    uint32_t seen[RT_SEEN_WORDS];
    uint32_t words = (metal::min(info.prim_count, RT_MAX_BOXES_PER_RING) + 31u) >> 5;
    for (uint32_t w = 0; w < words; ++w) {
        seen[w] = 0u;
    }

    // Scaling by a power of two is exact. Beyond |coordinate| = 2 no box of this ring
    // (all within [-1 - pad, 1 + pad], pad <= 1/64) can hold a contributing edge, so the
    // clamps only keep the ray finite and well-conditioned without changing the visited
    // set of contributing boxes.
    float sx = metal::clamp(delta_x * info.scale, -4.0f, 4.0f);
    float sy = metal::clamp(delta_y * info.scale, -4.0f, 4.0f);

    ray r;
    if (y_ray) {
        r.origin = float3(sx, metal::max(sy - info.ray_back, -2.0f), info.z);
        r.direction = float3(0.0f, 1.0f, 0.0f);
    } else {
        r.origin = float3(metal::max(sx - info.ray_back, -2.0f), sy, info.z);
        r.direction = float3(1.0f, 0.0f, 0.0f);
    }
    r.min_distance = 0.0f;
    r.max_distance = 16.0f;

    uint32_t crossings = 0;
    bool uncertain = false;

    intersection_query<> q(r, accel);
    // Candidates are never committed, so traversal is never shortened and visits every
    // box overlapped by the ray.
    while (q.next()) {
        if (q.get_candidate_intersection_type() != intersection_type::bounding_box) {
            continue;
        }
        RT_STAT_ADD(RT_STAT_BOX_REPORTS, 1);
        uint32_t g = q.get_candidate_primitive_id() - info.first_prim;
        if (g >= info.prim_count) {
            // Box of another ring (unsigned wrap covers smaller ids): a false positive of
            // traversal, never needed for this ring.
            RT_STAT_ADD(RT_STAT_FOREIGN_REPORTS, 1);
            continue;
        }
        uint32_t bit = 1u << (g & 31u);
        if (seen[g >> 5] & bit) {
            RT_STAT_ADD(RT_STAT_DUPLICATE_REPORTS, 1);
            continue;
        }
        seen[g >> 5] |= bit;

        uint32_t e0 = g * segs;
        uint32_t e1 = metal::min(e0 + segs, n);
        RT_STAT_ADD(RT_STAT_EDGES_VISITED, e1 - e0);
        for (uint32_t i = e0; i < e1; ++i) {
            Point2D v1 = vertices[start + i];
            Point2D v2 = vertices[start + ((i + 1) % n)];
            uint32_t edge = y_ray ? classify_edge_y(v1, v2, delta_x, delta_y, eta_k, u_flt)
                                  : classify_edge_x(v1, v2, delta_x, delta_y, eta_k, u_flt);
            if (edge == EDGE_UNCERTAIN) {
                uncertain = true;
                break;
            }
            crossings += edge;
        }
        if (uncertain) {
            break;
        }
    }

    if (uncertain) {
        return STATE_UNCERTAIN;
    }
    return (crossings & 1) ? STATE_INSIDE : STATE_OUTSIDE;
}
#endif // RT_ENABLE

#if RT_ENABLE
#if RT_STATS
#define RT_RING_ARGS(idx) (idx), ring_rt_slots, rt_infos, accel, used_rt, rt_stats,
#define RT_EVAL_ARGS accel, rt_stats,
#else
#define RT_RING_ARGS(idx) (idx), ring_rt_slots, rt_infos, accel, used_rt,
#define RT_EVAL_ARGS accel,
#endif
#else
#define RT_RING_ARGS(idx)
#endif

// Two-ray certified ring evaluator:
// Evaluates primary +x ray; on uncertainty, retries with orthogonal +y ray.
inline uint32_t evaluate_ring(
    RingRecord ring,
    device const Point2D* vertices,
#if RT_ENABLE
    uint32_t ring_idx,
    device const uint32_t* ring_rt_slots,
    device const RtRingInfo* rt_infos,
    primitive_acceleration_structure accel,
    thread bool& used_rt,
#if RT_STATS
    device atomic_uint* rt_stats,
#endif
#endif
    float delta_x,
    float delta_y,
    float eta_k,
    float u_flt)
{
#if RT_ENABLE && BOUND_MODE == 0
    uint32_t slot = ring_rt_slots[ring_idx];
    if (slot != RT_NO_SLOT) {
        RtRingInfo info = rt_infos[slot];
        // The box padding covers eta_k only up to eta_limit (NaN-safe comparison);
        // anything beyond takes the linear scan below.
        if (eta_k <= info.eta_limit) {
            used_rt = true;
            uint32_t rt_x = evaluate_ring_rt(ring, info, false, vertices, RT_EVAL_ARGS
                                             delta_x, delta_y, eta_k, u_flt);
            if (rt_x != STATE_UNCERTAIN) {
                return rt_x;
            }
            return evaluate_ring_rt(ring, info, true, vertices, RT_EVAL_ARGS
                                    delta_x, delta_y, eta_k, u_flt);
        }
        RT_STAT_ADD(RT_STAT_ETA_FALLBACKS, 1);
    }
#endif

    uint32_t state_x = evaluate_ring_x(ring, vertices, delta_x, delta_y, eta_k, u_flt);
    if (state_x != STATE_UNCERTAIN) {
        return state_x;
    }

#if BOUND_MODE == 4
    // Mode 4: Legacy single-ray baseline
    return STATE_UNCERTAIN;
#else
    // Retry with orthogonal +y ray to eliminate horizontal ray direction artifacts
    return evaluate_ring_y(ring, vertices, delta_x, delta_y, eta_k, u_flt);
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
#if RT_ENABLE
    primitive_acceleration_structure accel         [[buffer(9)]],
    device const uint32_t*        ring_rt_slots    [[buffer(10)]],
    device const RtRingInfo*      rt_infos         [[buffer(11)]],
#if RT_STATS
    device atomic_uint*           rt_stats         [[buffer(12)]],
#endif
#endif
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

#if RT_ENABLE
    bool used_rt = false;
#endif

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
        uint32_t ext_state = evaluate_ring(ext_ring, vertices, RT_RING_ARGS(part.ring_start)
                                           delta_x, delta_y, eta_k, u_flt);

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
            uint32_t hole_state = evaluate_ring(hole_ring, vertices, RT_RING_ARGS(part.ring_start + h)
                                                delta_x, delta_y, eta_k, u_flt);
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

#if RT_ENABLE
    if (used_rt) {
        RT_STAT_ADD(RT_STAT_PAIRS_WITH_RT, 1);
    }
#endif

    if (any_part_inside) {
        out_states[thread_id] = STATE_INSIDE;
    } else if (any_part_uncertain) {
        out_states[thread_id] = STATE_UNCERTAIN;
    } else {
        out_states[thread_id] = STATE_OUTSIDE;
    }
}
