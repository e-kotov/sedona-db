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

// PROTOTYPE (measurement only, not a production path).
//
// Second-stage exact point-in-polygon resolver for the uncertain residue of
// point_in_polygon_refine. Every coordinate arrives as an exact 128-bit
// two's-complement fixed-point integer at a batch-global binary scale 2^L,
// produced on the CPU from the original f64 inputs with no rounding.
// All arithmetic below is integer-only: no float is touched, so the result is
// independent of fast-math, fma contraction, reassociation and rounding mode.
//
// Output states: 0 outside, 1 inside, 3 boundary, 4 not-decided (inputs did not
// fit the fixed-point frame exactly; see f64_to_fixed on the host side).

#include <metal_stdlib>
using namespace metal;

#define EXACT_OUTSIDE 0
#define EXACT_INSIDE 1
#define EXACT_BOUNDARY 3
#define EXACT_NOT_DECIDED 4

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

// Exact vertex coordinate pair, 128-bit fixed point at scale 2^L (32 bytes).
struct FixedVertex {
    uint32_t x[4];
    uint32_t y[4];
};

// Exact probe point, 128-bit fixed point at scale 2^L (40 bytes).
struct FixedProbe {
    uint32_t x[4];
    uint32_t y[4];
    uint32_t is_exact;
    uint32_t _padding;
};

// 128-bit two's-complement integer, little-endian limbs.
struct I128 {
    uint32_t w[4];
};

inline I128 i128_load(const thread uint32_t (&src)[4]) {
    I128 r;
    r.w[0] = src[0];
    r.w[1] = src[1];
    r.w[2] = src[2];
    r.w[3] = src[3];
    return r;
}

inline bool i128_is_zero(I128 a) { return (a.w[0] | a.w[1] | a.w[2] | a.w[3]) == 0u; }

inline bool i128_is_neg(I128 a) { return (a.w[3] >> 31) != 0u; }

inline int i128_sign(I128 a) {
    if (i128_is_zero(a)) return 0;
    return i128_is_neg(a) ? -1 : 1;
}

inline bool i128_eq(I128 a, I128 b) {
    return a.w[0] == b.w[0] && a.w[1] == b.w[1] && a.w[2] == b.w[2] && a.w[3] == b.w[3];
}

// Two's-complement negation.
inline I128 i128_neg(I128 a) {
    I128 r;
    uint32_t carry = 1u;
#pragma unroll
    for (uint32_t i = 0; i < 4; ++i) {
        uint32_t v = ~a.w[i];
        uint32_t s = v + carry;
        carry = (s < v) ? 1u : 0u;
        r.w[i] = s;
    }
    return r;
}

inline I128 i128_abs(I128 a) { return i128_is_neg(a) ? i128_neg(a) : a; }

// a - b with borrow propagation. Callers guarantee |a|,|b| <= 2^121 so no wrap.
inline I128 i128_sub(I128 a, I128 b) {
    I128 r;
    uint32_t borrow = 0u;
#pragma unroll
    for (uint32_t i = 0; i < 4; ++i) {
        uint32_t ai = a.w[i];
        uint32_t bi = b.w[i];
        uint32_t t = ai - bi;
        uint32_t b1 = (ai < bi) ? 1u : 0u;
        uint32_t t2 = t - borrow;
        uint32_t b2 = (t < borrow) ? 1u : 0u;
        r.w[i] = t2;
        borrow = b1 + b2;  // provably <= 1
    }
    return r;
}

// Signed comparison: -1 if a < b, 0 if equal, 1 if a > b.
inline int i128_cmp(I128 a, I128 b) {
    bool na = i128_is_neg(a);
    bool nb = i128_is_neg(b);
    if (na != nb) return na ? -1 : 1;
    if (a.w[3] != b.w[3]) return (a.w[3] < b.w[3]) ? -1 : 1;
    if (a.w[2] != b.w[2]) return (a.w[2] < b.w[2]) ? -1 : 1;
    if (a.w[1] != b.w[1]) return (a.w[1] < b.w[1]) ? -1 : 1;
    if (a.w[0] != b.w[0]) return (a.w[0] < b.w[0]) ? -1 : 1;
    return 0;
}

// Unsigned 128 x 128 -> 256 schoolbook product. The running carry provably
// stays below 2^32 because a[i]*b[j] + r + carry <= 2^64 - 1 at every step.
inline void u128_mul(I128 a, I128 b, thread uint32_t (&r)[8]) {
#pragma unroll
    for (uint32_t i = 0; i < 8; ++i) r[i] = 0u;
#pragma unroll
    for (uint32_t i = 0; i < 4; ++i) {
        uint32_t carry = 0u;
#pragma unroll
        for (uint32_t j = 0; j < 4; ++j) {
            uint32_t lo = a.w[i] * b.w[j];
            uint32_t hi = metal::mulhi(a.w[i], b.w[j]);
            uint32_t s1 = lo + r[i + j];
            uint32_t c1 = (s1 < lo) ? 1u : 0u;
            uint32_t s2 = s1 + carry;
            uint32_t c2 = (s2 < carry) ? 1u : 0u;
            r[i + j] = s2;
            carry = hi + c1 + c2;
        }
        r[i + 4] = carry;
    }
}

inline int u256_cmp(const thread uint32_t (&a)[8], const thread uint32_t (&b)[8]) {
#pragma unroll
    for (int i = 7; i >= 0; --i) {
        if (a[i] != b[i]) return (a[i] < b[i]) ? -1 : 1;
    }
    return 0;
}

// Exact sign of the orientation determinant X1*Y2 - X2*Y1.
// Only the magnitudes are multiplied; the difference of the two 256-bit
// products never has to be formed, a comparison is enough.
inline int exact_orient_sign(I128 X1, I128 Y1, I128 X2, I128 Y2) {
    int sA = i128_sign(X1) * i128_sign(Y2);
    int sB = i128_sign(X2) * i128_sign(Y1);
    if (sA == 0 && sB == 0) return 0;
    if (sB == 0) return sA;
    if (sA == 0) return -sB;
    if (sA != sB) return sA;

    uint32_t m1[8];
    uint32_t m2[8];
    u128_mul(i128_abs(X1), i128_abs(Y2), m1);
    u128_mul(i128_abs(X2), i128_abs(Y1), m2);
    return sA * u256_cmp(m1, m2);
}

// Even-odd crossing parity for one ring with exact on-edge detection.
// Uses the half-open straddle rule (y1 <= 0 < y2 or y2 <= 0 < y1) so that a ray
// passing exactly through a vertex is counted once, matching the CPU oracle.
inline uint32_t eval_ring_exact(
    RingRecord ring,
    device const FixedVertex* verts,
    I128 px,
    I128 py,
    thread bool& boundary)
{
    uint32_t crossings = 0;
    uint32_t n = ring.vertex_count;
    uint32_t start = ring.vertex_start;
    if (n < 3) {
        boundary = false;
        return 0;
    }

    // Carry the previous vertex so each vertex is fetched once per ring.
    FixedVertex v0 = verts[start];
    I128 X1 = i128_sub(i128_load(v0.x), px);
    I128 Y1 = i128_sub(i128_load(v0.y), py);
    int sx1 = i128_sign(X1);
    int sy1 = i128_sign(Y1);

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t j = (i + 1 == n) ? 0u : (i + 1);
        FixedVertex vb = verts[start + j];
        I128 X2 = i128_sub(i128_load(vb.x), px);
        I128 Y2 = i128_sub(i128_load(vb.y), py);
        int sx2 = i128_sign(X2);
        int sy2 = i128_sign(Y2);

        I128 nX1 = X2;
        I128 nY1 = Y2;
        int nsx1 = sx2;
        int nsy1 = sy2;

        bool degenerate = i128_eq(X1, X2) && i128_eq(Y1, Y2);
        if (degenerate) {
            X1 = nX1;
            Y1 = nY1;
            sx1 = nsx1;
            sy1 = nsy1;
            continue;
        }

        // The probe lies inside the edge's closed bounding box.
        bool in_bbox = ((sx1 <= 0 && sx2 >= 0) || (sx2 <= 0 && sx1 >= 0)) &&
                       ((sy1 <= 0 && sy2 >= 0) || (sy2 <= 0 && sy1 >= 0));
        bool straddle = (sy1 <= 0 && sy2 > 0) || (sy2 <= 0 && sy1 > 0);

        if (in_bbox || straddle) {
            int d = exact_orient_sign(X1, Y1, X2, Y2);

            if (in_bbox && d == 0) {
                boundary = true;
                return 0;
            }

            if (straddle) {
                int cy = i128_cmp(Y1, Y2);  // < 0 means ay < by
                if ((cy < 0 && d > 0) || (cy > 0 && d < 0)) crossings++;
            }
        }

        X1 = nX1;
        Y1 = nY1;
        sx1 = nsx1;
        sy1 = nsy1;
    }

    boundary = false;
    return crossings & 1u;
}

kernel void point_in_polygon_exact(
    device const CandidatePair* candidate_pairs [[buffer(0)]],
    device const PolygonRecord* polygons        [[buffer(1)]],
    device const PartRecord*    parts           [[buffer(2)]],
    device const RingRecord*    rings           [[buffer(3)]],
    device const FixedVertex*   vertices        [[buffer(4)]],
    device const FixedProbe*    points          [[buffer(5)]],
    device const uint32_t*      poly_exact_ok   [[buffer(6)]],
    device uint8_t*             out_states      [[buffer(7)]],
    constant uint32_t&          num_candidates  [[buffer(8)]],
    constant uint32_t&          num_polygons    [[buffer(9)]],
    uint                        thread_id       [[thread_position_in_grid]])
{
    if (thread_id >= num_candidates) return;

    CandidatePair pair = candidate_pairs[thread_id];
    if (pair.polygon_idx >= num_polygons) {
        out_states[thread_id] = EXACT_NOT_DECIDED;
        return;
    }

    PolygonRecord poly = polygons[pair.polygon_idx];
    FixedProbe pt = points[pair.point_idx];

    if (poly.is_valid == 0 || pt.is_exact == 0 || poly_exact_ok[pair.polygon_idx] == 0) {
        out_states[thread_id] = EXACT_NOT_DECIDED;
        return;
    }

    I128 px = i128_load(pt.x);
    I128 py = i128_load(pt.y);

    bool any_part_inside = false;

    for (uint32_t p = 0; p < poly.part_count; ++p) {
        PartRecord part = parts[poly.part_start + p];
        if (part.ring_count == 0) continue;

        bool boundary = false;
        uint32_t ext = eval_ring_exact(rings[part.ring_start], vertices, px, py, boundary);
        if (boundary) {
            out_states[thread_id] = EXACT_BOUNDARY;
            return;
        }

        bool in_hole = false;
        for (uint32_t h = 1; h < part.ring_count; ++h) {
            uint32_t hole = eval_ring_exact(rings[part.ring_start + h], vertices, px, py, boundary);
            if (boundary) {
                out_states[thread_id] = EXACT_BOUNDARY;
                return;
            }
            if (hole == 1u) in_hole = true;
        }

        if (ext == 1u && !in_hole) any_part_inside = true;
    }

    out_states[thread_id] = any_part_inside ? EXACT_INSIDE : EXACT_OUTSIDE;
}
