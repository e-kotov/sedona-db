#include <metal_stdlib>
#include <metal_raytracing>

using namespace metal;
using namespace metal::raytracing;

struct BoundingBox {
    float xmin;
    float ymin;
    float xmax;
    float ymax;
};

struct MatchPair {
    uint build_idx;
    uint probe_idx;
};

// =====================================================================
// Apple Silicon Metal 3 Hardware Ray Tracing Index Shader
// Uses primitive_acceleration_structure containing MTLAxisAlignedBoundingBox
// and inline intersection_query to traverse hardware BVH on RT cores.
// Exact geometric validation is performed to guarantee 100% precision.
// =====================================================================

kernel void rt_probe_points(
    primitive_acceleration_structure accel        [[buffer(0)]],
    device const BoundingBox*        build_boxes  [[buffer(1)]],
    device const BoundingBox*        probe_boxes  [[buffer(2)]],
    device MatchPair*                output_pairs [[buffer(3)]],
    device atomic_uint*              match_count  [[buffer(4)]],
    constant uint&                   num_probes   [[buffer(5)]],
    constant uint&                   max_results  [[buffer(6)]],
    uint                             tid          [[thread_position_in_grid]])
{
    if (tid >= num_probes) return;

    BoundingBox probe = probe_boxes[tid];
    float px = probe.xmin;
    float py = probe.ymin;

    // Cast vertical ray along Z-axis through (px, py)
    ray r;
    r.origin = float3(px, py, -1.0f);
    r.direction = float3(0.0f, 0.0f, 1.0f);
    r.min_distance = 0.0f;
    r.max_distance = 2.0f;

    // Inline ray query traversing the hardware BVH
    intersection_query<> q(r, accel);
    while (q.next()) {
        if (q.get_candidate_intersection_type() == intersection_type::bounding_box) {
            uint build_id = q.get_candidate_primitive_id();
            BoundingBox b = build_boxes[build_id];
            // Exact geometric inclusion check (inclusive boundaries matching SedonaDB)
            if (px >= b.xmin && px <= b.xmax && py >= b.ymin && py <= b.ymax) {
                uint slot = atomic_fetch_add_explicit(match_count, 1, memory_order_relaxed);
                if (slot < max_results) {
                    output_pairs[slot] = MatchPair{build_id, tid};
                }
            }
        }
    }
}
