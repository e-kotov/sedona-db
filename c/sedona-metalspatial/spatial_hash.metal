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

struct GridLevelParams {
    float cell_w;
    float cell_h;
    uint  grid_dim;
    uint  cell_offset;
};

struct HierarchicalGridParams {
    float min_x;
    float min_y;
    uint  num_levels;
    uint  num_build;
    uint  num_probe;
    uint  probe_offset;
    uint  max_results;
    GridLevelParams levels[4];
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

inline void get_level_cell_range(BoundingBox b, constant HierarchicalGridParams& p, uint level,
                                 thread int& min_cx, thread int& max_cx,
                                 thread int& min_cy, thread int& max_cy)
{
    constant GridLevelParams& lp = p.levels[level];
    min_cx = clamp(int((b.xmin - p.min_x) / lp.cell_w), 0, int(lp.grid_dim - 1));
    max_cx = clamp(int((b.xmax - p.min_x) / lp.cell_w), 0, int(lp.grid_dim - 1));
    min_cy = clamp(int((b.ymin - p.min_y) / lp.cell_h), 0, int(lp.grid_dim - 1));
    max_cy = clamp(int((b.ymax - p.min_y) / lp.cell_h), 0, int(lp.grid_dim - 1));
}

inline uint select_level_for_box(BoundingBox b, constant HierarchicalGridParams& p) {
    // Select the finest level where box covers at most 4 cells (from level num_levels-1 down to 0)
    for (int lvl = int(p.num_levels - 1); lvl > 0; --lvl) {
        int min_cx, max_cx, min_cy, max_cy;
        get_level_cell_range(b, p, lvl, min_cx, max_cx, min_cy, max_cy);
        int count = (max_cx - min_cx + 1) * (max_cy - min_cy + 1);
        if (count <= 4) {
            return lvl;
        }
    }
    return 0; // Coarsest level covers at most 4 cells
}

// Pass 1: Count entries per cell across all levels
kernel void count_cell_entries(
    device const BoundingBox*             boxes       [[buffer(0)]],
    device atomic_uint*                   cell_counts [[buffer(1)]],
    constant HierarchicalGridParams&      p           [[buffer(2)]],
    uint                                  tid         [[thread_position_in_grid]])
{
    if (tid >= p.num_build) return;
    BoundingBox b = boxes[tid];
    if (!is_valid_box(b)) return;

    uint lvl = select_level_for_box(b, p);
    constant GridLevelParams& lp = p.levels[lvl];

    int min_cx, max_cx, min_cy, max_cy;
    get_level_cell_range(b, p, lvl, min_cx, max_cx, min_cy, max_cy);

    for (int y = min_cy; y <= max_cy; ++y) {
        for (int x = min_cx; x <= max_cx; ++x) {
            uint cell_idx = lp.cell_offset + y * lp.grid_dim + x;
            atomic_fetch_add_explicit(&cell_counts[cell_idx], 1, memory_order_relaxed);
        }
    }
}

// Pass 2: Populate cell entries into hierarchical grid
kernel void populate_cells(
    device const BoundingBox*             boxes        [[buffer(0)]],
    device atomic_uint*                   cell_heads   [[buffer(1)]],
    device const uint*                    cell_offsets [[buffer(2)]],
    device uint*                          cell_entries [[buffer(3)]],
    constant HierarchicalGridParams&      p            [[buffer(4)]],
    uint                                  tid          [[thread_position_in_grid]])
{
    if (tid >= p.num_build) return;
    BoundingBox b = boxes[tid];
    if (!is_valid_box(b)) return;

    uint lvl = select_level_for_box(b, p);
    constant GridLevelParams& lp = p.levels[lvl];

    int min_cx, max_cx, min_cy, max_cy;
    get_level_cell_range(b, p, lvl, min_cx, max_cx, min_cy, max_cy);

    for (int y = min_cy; y <= max_cy; ++y) {
        for (int x = min_cx; x <= max_cx; ++x) {
            uint cell_idx = lp.cell_offset + y * lp.grid_dim + x;
            uint slot = atomic_fetch_add_explicit(&cell_heads[cell_idx], 1, memory_order_relaxed);
            cell_entries[cell_offsets[cell_idx] + slot] = tid;
        }
    }
}

// Pass 3: Probe hierarchical grid visiting every level
kernel void probe_grid(
    device const BoundingBox*             build_boxes  [[buffer(0)]],
    device const BoundingBox*             probe_boxes  [[buffer(1)]],
    device const uint*                    cell_offsets [[buffer(2)]],
    device const uint*                    cell_entries [[buffer(3)]],
    device MatchPair*                     output_pairs [[buffer(4)]],
    device atomic_uint*                   match_count  [[buffer(5)]],
    constant HierarchicalGridParams&      p            [[buffer(6)]],
    uint                                  tid          [[thread_position_in_grid]])
{
    if (tid >= p.num_probe || p.num_build == 0) return;
    uint probe_idx = p.probe_offset + tid;
    BoundingBox probe = probe_boxes[probe_idx];
    if (!is_valid_box(probe)) return;

    // A probe visits every level of the hierarchy
    for (uint lvl = 0; lvl < p.num_levels; ++lvl) {
        constant GridLevelParams& lp = p.levels[lvl];
        float max_grid_x = p.min_x + lp.cell_w * float(lp.grid_dim);
        float max_grid_y = p.min_y + lp.cell_h * float(lp.grid_dim);
        if (probe.xmax < p.min_x || probe.xmin > max_grid_x ||
            probe.ymax < p.min_y || probe.ymin > max_grid_y) {
            continue;
        }

        int p_min_cx, p_max_cx, p_min_cy, p_max_cy;
        get_level_cell_range(probe, p, lvl, p_min_cx, p_max_cx, p_min_cy, p_max_cy);

        for (int cy = p_min_cy; cy <= p_max_cy; ++cy) {
            for (int cx = p_min_cx; cx <= p_max_cx; ++cx) {
                uint cell_idx = lp.cell_offset + cy * lp.grid_dim + cx;
                uint start = cell_offsets[cell_idx];
                uint end = cell_offsets[cell_idx + 1];

                for (uint i = start; i < end; ++i) {
                    uint build_id = cell_entries[i];
                    BoundingBox build = build_boxes[build_id];
                    if (boxes_intersect(build, probe)) {
                        int b_min_cx, b_max_cx, b_min_cy, b_max_cy;
                        get_level_cell_range(build, p, lvl, b_min_cx, b_max_cx, b_min_cy, b_max_cy);
                        // Deduplication rule per level: emit only in the minimum common cell
                        if (cx == max(p_min_cx, b_min_cx) && cy == max(p_min_cy, b_min_cy)) {
                            uint slot = atomic_fetch_add_explicit(match_count, 1, memory_order_relaxed);
                            if (slot < p.max_results) {
                                output_pairs[slot] = MatchPair{build_id, probe_idx};
                            }
                        }
                    }
                }
            }
        }
    }
}
