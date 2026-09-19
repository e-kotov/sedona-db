#include <metal_stdlib>
using namespace metal;

// 2D Axis-Aligned Bounding Box (matching SedonaDB's [xmin, ymin, xmax, ymax] layout)
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

// Check if two 2D boxes intersect (inclusive boundaries)
inline bool boxes_intersect(BoundingBox a, BoundingBox b) {
    return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
}

// Stage 1 Spatial Filter Kernel:
// Each thread processes one probe box against all build boxes (or a tiled subset).
// When an overlap is detected, an atomic counter reserves a slot in the results buffer.
kernel void box_intersection_filter(
    device const BoundingBox* build_boxes  [[buffer(0)]],
    device const BoundingBox* probe_boxes  [[buffer(1)]],
    device MatchPair*         output_pairs [[buffer(2)]],
    device atomic_uint*       match_count  [[buffer(3)]],
    constant uint&            num_build    [[buffer(4)]],
    constant uint&            max_results  [[buffer(5)]],
    uint                      probe_id     [[thread_position_in_grid]])
{
    BoundingBox probe = probe_boxes[probe_id];

    for (uint build_id = 0; build_id < num_build; ++build_id) {
        if (boxes_intersect(build_boxes[build_id], probe)) {
            uint slot = atomic_fetch_add_explicit(match_count, 1, memory_order_relaxed);
            if (slot < max_results) {
                output_pairs[slot] = MatchPair{build_id, probe_id};
            }
        }
    }
}
