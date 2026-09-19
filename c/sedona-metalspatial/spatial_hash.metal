#include <metal_stdlib>
using namespace metal;

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

struct GridParams {
    float min_x;
    float min_y;
    float cell_w;
    float cell_h;
    uint  grid_dim_x;
    uint  grid_dim_y;
    uint  num_build;
    uint  num_probe;
    uint  max_results;
};

inline bool is_valid_box(BoundingBox b) {
    return !(isnan(b.xmin) || isnan(b.ymin) || isnan(b.xmax) || isnan(b.ymax) ||
             isinf(b.xmin) || isinf(b.ymin) || isinf(b.xmax) || isinf(b.ymax) ||
             b.xmin > b.xmax || b.ymin > b.ymax);
}

// Check if two 2D boxes intersect (inclusive boundaries)
inline bool boxes_intersect(BoundingBox a, BoundingBox b) {
    if (!is_valid_box(a) || !is_valid_box(b)) return false;
    return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
}

// Compute the cell index range [min_cx..max_cx, min_cy..max_cy] for a box
inline void get_cell_range(BoundingBox b, constant GridParams& p,
                           thread int& min_cx, thread int& max_cx,
                           thread int& min_cy, thread int& max_cy)
{
    min_cx = clamp(int((b.xmin - p.min_x) / p.cell_w), 0, int(p.grid_dim_x - 1));
    max_cx = clamp(int((b.xmax - p.min_x) / p.cell_w), 0, int(p.grid_dim_x - 1));
    min_cy = clamp(int((b.ymin - p.min_y) / p.cell_h), 0, int(p.grid_dim_y - 1));
    max_cy = clamp(int((b.ymax - p.min_y) / p.cell_h), 0, int(p.grid_dim_y - 1));
}

// Pass 1: Count entries per cell
kernel void count_cell_entries(
    device const BoundingBox* boxes       [[buffer(0)]],
    device atomic_uint*       cell_counts [[buffer(1)]],
    constant GridParams&      p           [[buffer(2)]],
    uint                      tid         [[thread_position_in_grid]])
{
    if (tid >= p.num_build) return;
    BoundingBox b = boxes[tid];
    if (!is_valid_box(b)) return;

    int min_cx, max_cx, min_cy, max_cy;
    get_cell_range(b, p, min_cx, max_cx, min_cy, max_cy);

    for (int y = min_cy; y <= max_cy; ++y) {
        for (int x = min_cx; x <= max_cx; ++x) {
            uint cell_idx = y * p.grid_dim_x + x;
            atomic_fetch_add_explicit(&cell_counts[cell_idx], 1, memory_order_relaxed);
        }
    }
}

// Pass 2: Populate cell entries
kernel void populate_cells(
    device const BoundingBox* boxes        [[buffer(0)]],
    device atomic_uint*       cell_heads   [[buffer(1)]],
    device const uint*        cell_offsets [[buffer(2)]],
    device uint*              cell_entries [[buffer(3)]],
    constant GridParams&      p            [[buffer(4)]],
    uint                      tid          [[thread_position_in_grid]])
{
    if (tid >= p.num_build) return;
    BoundingBox b = boxes[tid];
    if (!is_valid_box(b)) return;

    int min_cx, max_cx, min_cy, max_cy;
    get_cell_range(b, p, min_cx, max_cx, min_cy, max_cy);

    for (int y = min_cy; y <= max_cy; ++y) {
        for (int x = min_cx; x <= max_cx; ++x) {
            uint cell_idx = y * p.grid_dim_x + x;
            uint slot = atomic_fetch_add_explicit(&cell_heads[cell_idx], 1, memory_order_relaxed);
            cell_entries[cell_offsets[cell_idx] + slot] = tid;
        }
    }
}

// Pass 3: Probe grid with probe boxes and deduplicate across cells
kernel void probe_grid(
    device const BoundingBox* build_boxes  [[buffer(0)]],
    device const BoundingBox* probe_boxes  [[buffer(1)]],
    device const uint*        cell_offsets [[buffer(2)]],
    device const uint*        cell_entries [[buffer(3)]],
    device MatchPair*         output_pairs [[buffer(4)]],
    device atomic_uint*       match_count  [[buffer(5)]],
    constant GridParams&      p            [[buffer(6)]],
    uint                      tid          [[thread_position_in_grid]])
{
    if (tid >= p.num_probe || p.num_build == 0) return;
    BoundingBox probe = probe_boxes[tid];
    if (!is_valid_box(probe)) return;

    float max_grid_x = p.min_x + p.cell_w * float(p.grid_dim_x);
    float max_grid_y = p.min_y + p.cell_h * float(p.grid_dim_y);
    if (probe.xmax < p.min_x || probe.xmin > max_grid_x ||
        probe.ymax < p.min_y || probe.ymin > max_grid_y) {
        return;
    }

    int p_min_cx, p_max_cx, p_min_cy, p_max_cy;
    get_cell_range(probe, p, p_min_cx, p_max_cx, p_min_cy, p_max_cy);

    for (int cy = p_min_cy; cy <= p_max_cy; ++cy) {
        for (int cx = p_min_cx; cx <= p_max_cx; ++cx) {
            uint cell_idx = cy * p.grid_dim_x + cx;
            uint start = cell_offsets[cell_idx];
            uint end = cell_offsets[cell_idx + 1];

            for (uint i = start; i < end; ++i) {
                uint build_id = cell_entries[i];
                BoundingBox build = build_boxes[build_id];
                if (boxes_intersect(build, probe)) {
                    int b_min_cx, b_max_cx, b_min_cy, b_max_cy;
                    get_cell_range(build, p, b_min_cx, b_max_cx, b_min_cy, b_max_cy);
                    // Deduplication rule: only emit in the minimum common cell
                    if (cx == max(p_min_cx, b_min_cx) && cy == max(p_min_cy, b_min_cy)) {
                        uint slot = atomic_fetch_add_explicit(match_count, 1, memory_order_relaxed);
                        if (slot < p.max_results) {
                            output_pairs[slot] = MatchPair{build_id, tid};
                        }
                    }
                }
            }
        }
    }
}
