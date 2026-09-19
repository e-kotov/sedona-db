#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <algorithm>
#include <cmath>
#include "geom_types.hpp"

// =====================================================================
// CPU Reference Winding-Number & Point-in-Polygon Implementation
// (Ported from SedonaDB's polygon.hpp)
// =====================================================================

inline int cpu_orientation(Point2D p1, Point2D p2, Point2D q) {
    float d_x = q.x - p1.x;
    float d_y = q.y - p1.y;
    if (std::abs(d_x) <= 1e-7f && std::abs(d_y) <= 1e-7f) {
        return 0;
    }
    float v1 = d_x * (p2.y - p1.y);
    float v2 = (p2.x - p1.x) * d_y;
    if (std::abs(v1 - v2) <= 1e-7f) {
        return 0;
    }
    return (v1 - v2 < 0.0f) ? -1 : 1;
}

inline bool cpu_segment_covers(Point2D p1, Point2D p2, Point2D q) {
    float min_x = std::min(p1.x, p2.x) - 1e-6f;
    float max_x = std::max(p1.x, p2.x) + 1e-6f;
    float min_y = std::min(p1.y, p2.y) - 1e-6f;
    float max_y = std::max(p1.y, p2.y) + 1e-6f;
    return (q.x >= min_x && q.x <= max_x && q.y >= min_y && q.y <= max_y);
}

inline PointLocation cpu_locate_point_in_ring(
    Point2D p,
    const std::vector<Point2D>& vertices,
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

        if (p1.x == p2.x && p1.y == p2.y) continue;

        int side = cpu_orientation(p1, p2, p);
        if (side == 0) {
            if (cpu_segment_covers(p1, p2, p)) {
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

inline PointLocation cpu_locate_point_in_polygon(
    Point2D p,
    const PolygonGeom& poly,
    const std::vector<PolygonRing>& rings,
    const std::vector<Point2D>& vertices)
{
    // 1. Outer ring test
    PolygonRing outer = rings[poly.outer_ring_idx];
    PointLocation outer_loc = cpu_locate_point_in_ring(p, vertices, outer.start_idx, outer.num_points);

    if (outer_loc == kPointOutside) {
        return kPointOutside;
    }

    PointLocation rloc = outer_loc;

    // 2. Interior rings (holes) test
    for (uint32_t h = 0; h < poly.num_interior_rings; ++h) {
        PolygonRing hole = rings[poly.outer_ring_idx + 1 + h];
        PointLocation hole_loc = cpu_locate_point_in_ring(p, vertices, hole.start_idx, hole.num_points);

        if (hole_loc == kPointInside) {
            return kPointOutside; // Point inside hole is outside polygon
        }
        if (hole_loc == kPointBoundary) {
            rloc = kPointBoundary;
        }
    }

    return rloc;
}

// Compute AABB for a polygon from its outer ring vertices
inline BoundingBox compute_polygon_aabb(
    const PolygonGeom& poly,
    const std::vector<PolygonRing>& rings,
    const std::vector<Point2D>& vertices)
{
    PolygonRing outer = rings[poly.outer_ring_idx];
    float xmin = 1e30f, ymin = 1e30f, xmax = -1e30f, ymax = -1e30f;
    for (uint32_t i = 0; i < outer.num_points; ++i) {
        Point2D pt = vertices[outer.start_idx + i];
        xmin = std::min(xmin, pt.x);
        ymin = std::min(ymin, pt.y);
        xmax = std::max(xmax, pt.x);
        ymax = std::max(ymax, pt.y);
    }
    return BoundingBox{xmin, ymin, xmax, ymax};
}

int main() {
    @autoreleasepool {
        std::cout << "=================================================================\n";
        std::cout << " SedonaDB Metal Spatial Refiner Test: Point-in-Polygon (MSL)\n";
        std::cout << " Phase 1 Differential Validation: CPU Reference vs. Metal GPU\n";
        std::cout << "=================================================================\n";

        // 1. Initialize Metal Device
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            std::cerr << "Error: No Metal GPU device found.\n";
            return 1;
        }
        std::cout << "Using GPU Device: " << [[device name] UTF8String] << "\n";
        std::cout << "Unified Memory:   " << (device.hasUnifiedMemory ? "YES" : "NO") << "\n\n";

        // 2. Load and compile Stage 1 & Stage 2 Metal Shaders
        NSError *error = nil;

        // Stage 1: box intersection filter
        NSString *boxShaderSource = [NSString stringWithContentsOfFile:@"box_intersection.metal"
                                                              encoding:NSUTF8StringEncoding
                                                                 error:&error];
        if (error) {
            std::cerr << "Error reading box_intersection.metal: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }
        id<MTLLibrary> boxLib = [device newLibraryWithSource:boxShaderSource options:nil error:&error];
        if (!boxLib) {
            std::cerr << "Box shader compilation error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }
        id<MTLFunction> boxFunc = [boxLib newFunctionWithName:@"box_intersection_filter"];
        id<MTLComputePipelineState> boxPipeline = [device newComputePipelineStateWithFunction:boxFunc error:&error];
        if (!boxPipeline) {
            std::cerr << "Box pipeline error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }

        // Stage 2: refine.metal (Winding Number PIP)
        NSString *refineShaderSource = [NSString stringWithContentsOfFile:@"refine.metal"
                                                                 encoding:NSUTF8StringEncoding
                                                                    error:&error];
        if (error) {
            std::cerr << "Error reading refine.metal: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }
        id<MTLLibrary> refineLib = [device newLibraryWithSource:refineShaderSource options:nil error:&error];
        if (!refineLib) {
            std::cerr << "Refine shader compilation error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }
        id<MTLFunction> refineFunc = [refineLib newFunctionWithName:@"point_in_polygon_refine"];
        id<MTLComputePipelineState> refinePipeline = [device newComputePipelineStateWithFunction:refineFunc error:&error];
        if (!refinePipeline) {
            std::cerr << "Refine pipeline error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }

        // 3. Build Complex Non-Convex Test Polygons
        // GeoArrow layout: flat vertices vector, flat rings vector, flat polygon headers
        std::vector<Point2D> vertices;
        std::vector<PolygonRing> rings;
        std::vector<PolygonGeom> polygons;

        auto add_ring = [&](const std::vector<Point2D>& pts) -> uint32_t {
            uint32_t ring_idx = static_cast<uint32_t>(rings.size());
            uint32_t start_idx = static_cast<uint32_t>(vertices.size());
            for (const auto& pt : pts) {
                vertices.push_back(pt);
            }
            rings.push_back(PolygonRing{start_idx, static_cast<uint32_t>(pts.size())});
            return ring_idx;
        };

        // Polygon 0: L-Shaped Polygon (Non-convex, 0 holes)
        // AABB: [0, 100] x [0, 100]. Empty corner: (40, 100) x (40, 100)
        {
            uint32_t outer_idx = add_ring({
                {0.0f, 0.0f}, {100.0f, 0.0f}, {100.0f, 40.0f},
                {40.0f, 40.0f}, {40.0f, 100.0f}, {0.0f, 100.0f}, {0.0f, 0.0f}
            });
            polygons.push_back(PolygonGeom{outer_idx, 0});
        }

        // Polygon 1: Concave Chevron / Arrow (Non-convex, 0 holes)
        // AABB: [150, 250] x [0, 100]. Rear notch: (170, 50) is inside AABB but outside arrow
        {
            uint32_t outer_idx = add_ring({
                {150.0f, 0.0f}, {250.0f, 50.0f}, {150.0f, 100.0f}, {190.0f, 50.0f}, {150.0f, 0.0f}
            });
            polygons.push_back(PolygonGeom{outer_idx, 0});
        }

        // Polygon 2: Donut with 1 Hole
        // Outer ring AABB: [300, 400] x [0, 100]
        // Hole: [330, 370] x [30, 70]
        {
            uint32_t outer_idx = add_ring({
                {300.0f, 0.0f}, {400.0f, 0.0f}, {400.0f, 100.0f}, {300.0f, 100.0f}, {300.0f, 0.0f}
            });
            // Interior hole ring
            add_ring({
                {330.0f, 30.0f}, {370.0f, 30.0f}, {370.0f, 70.0f}, {330.0f, 70.0f}, {330.0f, 30.0f}
            });
            polygons.push_back(PolygonGeom{outer_idx, 1});
        }

        // Polygon 3: Multi-hole Donut (2 interior holes)
        // Outer ring AABB: [500, 700] x [0, 200]
        // Hole 1: [530, 580] x [50, 150]
        // Hole 2: [620, 670] x [50, 150]
        {
            uint32_t outer_idx = add_ring({
                {500.0f, 0.0f}, {700.0f, 0.0f}, {700.0f, 200.0f}, {500.0f, 200.0f}, {500.0f, 0.0f}
            });
            add_ring({
                {530.0f, 50.0f}, {580.0f, 50.0f}, {580.0f, 150.0f}, {530.0f, 150.0f}, {530.0f, 50.0f}
            });
            add_ring({
                {620.0f, 50.0f}, {670.0f, 50.0f}, {670.0f, 150.0f}, {620.0f, 150.0f}, {620.0f, 50.0f}
            });
            polygons.push_back(PolygonGeom{outer_idx, 2});
        }

        // Polygon 4: Concave 8-Point Star (16 vertices, deep concave bays)
        // Center: (150, 400), R_outer = 90, R_inner = 35
        {
            const float cx = 150.0f, cy = 400.0f;
            const float r_out = 90.0f, r_in = 35.0f;
            std::vector<Point2D> star_pts;
            const int num_tips = 8;
            for (int i = 0; i < num_tips * 2; ++i) {
                float angle = static_cast<float>(i) * (M_PI / num_tips);
                float r = (i % 2 == 0) ? r_out : r_in;
                star_pts.push_back({cx + r * std::cos(angle), cy + r * std::sin(angle)});
            }
            star_pts.push_back(star_pts.front()); // Close ring
            uint32_t outer_idx = add_ring(star_pts);
            polygons.push_back(PolygonGeom{outer_idx, 0});
        }

        // Duplicate / translate polygons across the coordinate plane to create a multi-polygon dataset
        const uint32_t base_poly_count = static_cast<uint32_t>(polygons.size());
        for (uint32_t copy = 1; copy < 5; ++copy) {
            float dx = static_cast<float>(copy * 150.0f);
            float dy = static_cast<float>(copy * 120.0f);
            for (uint32_t p = 0; p < base_poly_count; ++p) {
                PolygonGeom src_poly = polygons[p];
                PolygonRing src_outer = rings[src_poly.outer_ring_idx];

                // Copy outer ring shifted
                std::vector<Point2D> shifted_outer;
                shifted_outer.reserve(src_outer.num_points);
                for (uint32_t i = 0; i < src_outer.num_points; ++i) {
                    Point2D pt = vertices[src_outer.start_idx + i];
                    shifted_outer.push_back({pt.x + dx, pt.y + dy});
                }
                uint32_t new_outer_idx = add_ring(shifted_outer);

                // Copy inner rings shifted
                for (uint32_t h = 0; h < src_poly.num_interior_rings; ++h) {
                    PolygonRing src_hole = rings[src_poly.outer_ring_idx + 1 + h];
                    std::vector<Point2D> shifted_hole;
                    shifted_hole.reserve(src_hole.num_points);
                    for (uint32_t i = 0; i < src_hole.num_points; ++i) {
                        Point2D pt = vertices[src_hole.start_idx + i];
                        shifted_hole.push_back({pt.x + dx, pt.y + dy});
                    }
                    add_ring(shifted_hole);
                }

                polygons.push_back(PolygonGeom{new_outer_idx, src_poly.num_interior_rings});
            }
        }

        const uint32_t total_polygons = static_cast<uint32_t>(polygons.size());
        std::cout << "Created " << total_polygons << " test polygons with "
                  << rings.size() << " rings and " << vertices.size() << " total vertices.\n";

        // Compute Bounding Boxes for Polygons
        std::vector<BoundingBox> poly_boxes(total_polygons);
        for (uint32_t i = 0; i < total_polygons; ++i) {
            poly_boxes[i] = compute_polygon_aabb(polygons[i], rings, vertices);
        }

        // 4. Generate 10,000 Test Points
        // Combining targeted probes (inside shapes, inside holes, inside empty corners)
        // and pseudo-random distributed points across the bounding boxes.
        const uint32_t TOTAL_POINTS = 10000;
        std::vector<Point2D> points;
        points.reserve(TOTAL_POINTS);

        // Targeted test points for Base Polygons:
        // Polygon 0 (L-shape):
        points.push_back({20.0f, 20.0f});   // Inside base
        points.push_back({20.0f, 80.0f});   // Inside stem
        points.push_back({80.0f, 20.0f});   // Inside foot
        points.push_back({70.0f, 70.0f});   // IN AABB, BUT IN EMPTY CORNER! (Stage 1 FP)
        points.push_back({90.0f, 90.0f});   // IN AABB, BUT IN EMPTY CORNER! (Stage 1 FP)

        // Polygon 1 (Arrow):
        points.push_back({220.0f, 50.0f});  // Inside arrow body
        points.push_back({170.0f, 50.0f});  // IN AABB, BUT IN REAR NOTCH! (Stage 1 FP)

        // Polygon 2 (Donut with 1 Hole):
        points.push_back({310.0f, 50.0f});  // Inside left donut wall
        points.push_back({390.0f, 50.0f});  // Inside right donut wall
        points.push_back({350.0f, 50.0f});  // IN AABB, BUT INSIDE HOLE! (Stage 1 FP)

        // Polygon 3 (Donut with 2 Holes):
        points.push_back({510.0f, 100.0f}); // Inside outer frame
        points.push_back({600.0f, 100.0f}); // Between hole 1 and hole 2 (inside polygon)
        points.push_back({550.0f, 100.0f}); // IN AABB, BUT INSIDE HOLE 1! (Stage 1 FP)
        points.push_back({650.0f, 100.0f}); // IN AABB, BUT INSIDE HOLE 2! (Stage 1 FP)

        // Polygon 4 (Concave Star):
        points.push_back({150.0f, 400.0f}); // Inside star center
        // Point in the concave bay between arms:
        points.push_back({static_cast<float>(150.0f + 60.0f * std::cos(M_PI / 8.0f)),
                          static_cast<float>(400.0f + 60.0f * std::sin(M_PI / 8.0f))}); // IN AABB, IN STAR BAY! (Stage 1 FP)

        // Deterministic pseudo-random points scattered across the domain
        uint32_t lcg = 987654321;
        auto next_float = [&lcg](float min_v, float max_v) {
            lcg = lcg * 1664525u + 1013904223u;
            float norm = (float)(lcg >> 8) / 16777216.0f;
            return min_v + norm * (max_v - min_v);
        };

        while (points.size() < TOTAL_POINTS) {
            // Pick a polygon and scatter near or in its AABB to ensure dense candidate pairs
            uint32_t target_poly = static_cast<uint32_t>(points.size() % total_polygons);
            BoundingBox box = poly_boxes[target_poly];
            // Expand slightly around the box so some points land inside and some outside
            float px = next_float(box.xmin - 20.0f, box.xmax + 20.0f);
            float py = next_float(box.ymin - 20.0f, box.ymax + 20.0f);
            points.push_back({px, py});
        }

        std::cout << "Generated " << points.size() << " test points for differential refinement.\n\n";

        // Convert points to BoundingBoxes for Stage 1 filter
        std::vector<BoundingBox> point_boxes(TOTAL_POINTS);
        for (uint32_t i = 0; i < TOTAL_POINTS; ++i) {
            point_boxes[i] = {points[i].x, points[i].y, points[i].x, points[i].y};
        }

        // =====================================================================
        // Stage 1: Bounding Box Intersection Filter (Generate Candidates)
        // =====================================================================
        std::cout << "--- Stage 1: Bounding Box Candidate Filter ---\n";

        // CPU Stage 1 baseline
        auto cpu_intersects = [](BoundingBox a, BoundingBox b) {
            return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
        };

        std::vector<CandidatePair> cpu_candidates;
        cpu_candidates.reserve(50000);
        for (uint32_t pt_id = 0; pt_id < TOTAL_POINTS; ++pt_id) {
            for (uint32_t poly_id = 0; poly_id < total_polygons; ++poly_id) {
                if (cpu_intersects(poly_boxes[poly_id], point_boxes[pt_id])) {
                    cpu_candidates.push_back({poly_id, pt_id});
                }
            }
        }
        std::cout << "Stage 1 (CPU): Generated " << cpu_candidates.size() << " candidate pairs.\n";

        // GPU Stage 1 dispatch
        const uint32_t MAX_CANDIDATES = 100000;
        id<MTLBuffer> gpu_poly_boxes = [device newBufferWithBytes:poly_boxes.data()
                                                           length:sizeof(BoundingBox) * total_polygons
                                                          options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_point_boxes = [device newBufferWithBytes:point_boxes.data()
                                                            length:sizeof(BoundingBox) * TOTAL_POINTS
                                                           options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_cand_buf = [device newBufferWithLength:sizeof(CandidatePair) * MAX_CANDIDATES
                                                         options:MTLResourceStorageModeShared];
        uint32_t zero_u32 = 0;
        id<MTLBuffer> gpu_cand_count = [device newBufferWithBytes:&zero_u32
                                                           length:sizeof(uint32_t)
                                                          options:MTLResourceStorageModeShared];
        uint32_t n_poly = total_polygons;
        id<MTLBuffer> gpu_npoly_buf = [device newBufferWithBytes:&n_poly
                                                          length:sizeof(uint32_t)
                                                         options:MTLResourceStorageModeShared];
        uint32_t max_cand_param = MAX_CANDIDATES;
        id<MTLBuffer> gpu_maxcand_buf = [device newBufferWithBytes:&max_cand_param
                                                            length:sizeof(uint32_t)
                                                           options:MTLResourceStorageModeShared];

        id<MTLCommandQueue> queue = [device newCommandQueue];
        id<MTLCommandBuffer> stage1Cmd = [queue commandBuffer];
        id<MTLComputeCommandEncoder> stage1Enc = [stage1Cmd computeCommandEncoder];

        [stage1Enc setComputePipelineState:boxPipeline];
        [stage1Enc setBuffer:gpu_poly_boxes offset:0 atIndex:0];
        [stage1Enc setBuffer:gpu_point_boxes offset:0 atIndex:1];
        [stage1Enc setBuffer:gpu_cand_buf offset:0 atIndex:2];
        [stage1Enc setBuffer:gpu_cand_count offset:0 atIndex:3];
        [stage1Enc setBuffer:gpu_npoly_buf offset:0 atIndex:4];
        [stage1Enc setBuffer:gpu_maxcand_buf offset:0 atIndex:5];

        MTLSize stage1Grid = MTLSizeMake(TOTAL_POINTS, 1, 1);
        NSUInteger stage1Tg = std::min((NSUInteger)TOTAL_POINTS, boxPipeline.maxTotalThreadsPerThreadgroup);
        [stage1Enc dispatchThreads:stage1Grid threadsPerThreadgroup:MTLSizeMake(stage1Tg, 1, 1)];
        [stage1Enc endEncoding];

        [stage1Cmd commit];
        [stage1Cmd waitUntilCompleted];

        uint32_t gpu_num_candidates = *(uint32_t*)[gpu_cand_count contents];
        std::cout << "Stage 1 (GPU): Generated " << gpu_num_candidates << " candidate pairs.\n";
        assert(gpu_num_candidates == cpu_candidates.size() && "Stage 1 candidate count mismatch!");

        // Sort and verify 100% parity on Stage 1 candidates
        CandidatePair* gpu_cand_raw = (CandidatePair*)[gpu_cand_buf contents];
        std::vector<CandidatePair> gpu_candidates(gpu_cand_raw, gpu_cand_raw + gpu_num_candidates);
        std::sort(cpu_candidates.begin(), cpu_candidates.end());
        std::sort(gpu_candidates.begin(), gpu_candidates.end());
        assert(cpu_candidates == gpu_candidates && "Stage 1 candidates mismatch between CPU and GPU!");
        std::cout << ">>> Stage 1 Parity Verified: Exact match on candidate pairs. <<<\n\n";

        // =====================================================================
        // Stage 2: Geometric Refinement (Exact Point-in-Polygon Winding Number)
        // =====================================================================
        std::cout << "--- Stage 2: Point-in-Polygon Geometric Refinement ---\n";

        // 1. CPU Reference Execution
        auto cpu_refine_start = std::chrono::high_resolution_clock::now();
        std::vector<CandidatePair> cpu_refined;
        cpu_refined.reserve(cpu_candidates.size());

        for (const auto& cand : cpu_candidates) {
            PointLocation loc = cpu_locate_point_in_polygon(
                points[cand.point_idx],
                polygons[cand.polygon_idx],
                rings,
                vertices
            );
            if (loc != kPointOutside) {
                cpu_refined.push_back(cand);
            }
        }
        auto cpu_refine_end = std::chrono::high_resolution_clock::now();
        double cpu_refine_ms = std::chrono::duration<double, std::milli>(cpu_refine_end - cpu_refine_start).count();
        std::cout << "CPU Reference Refiner: " << cpu_refined.size() << " exact matches from "
                  << cpu_candidates.size() << " candidates in " << cpu_refine_ms << " ms\n";

        // 2. Metal GPU Refiner Execution
        // Allocate Metal buffers for GeoArrow structures (contiguous memory layout)
        id<MTLBuffer> gpu_polygons = [device newBufferWithBytes:polygons.data()
                                                         length:sizeof(PolygonGeom) * polygons.size()
                                                        options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_rings = [device newBufferWithBytes:rings.data()
                                                      length:sizeof(PolygonRing) * rings.size()
                                                     options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_vertices = [device newBufferWithBytes:vertices.data()
                                                         length:sizeof(Point2D) * vertices.size()
                                                        options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_points = [device newBufferWithBytes:points.data()
                                                       length:sizeof(Point2D) * points.size()
                                                      options:MTLResourceStorageModeShared];

        const uint32_t MAX_REFINED = static_cast<uint32_t>(cpu_candidates.size() + 100);
        id<MTLBuffer> gpu_refined_buf = [device newBufferWithLength:sizeof(CandidatePair) * MAX_REFINED
                                                            options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_refined_count = [device newBufferWithBytes:&zero_u32
                                                              length:sizeof(uint32_t)
                                                             options:MTLResourceStorageModeShared];
        uint32_t num_candidates_param = gpu_num_candidates;
        id<MTLBuffer> gpu_ncand_buf = [device newBufferWithBytes:&num_candidates_param
                                                          length:sizeof(uint32_t)
                                                         options:MTLResourceStorageModeShared];
        uint32_t max_refined_param = MAX_REFINED;
        id<MTLBuffer> gpu_maxref_buf = [device newBufferWithBytes:&max_refined_param
                                                           length:sizeof(uint32_t)
                                                          options:MTLResourceStorageModeShared];

        auto gpu_refine_start = std::chrono::high_resolution_clock::now();
        id<MTLCommandBuffer> refineCmd = [queue commandBuffer];
        id<MTLComputeCommandEncoder> refineEnc = [refineCmd computeCommandEncoder];

        [refineEnc setComputePipelineState:refinePipeline];
        [refineEnc setBuffer:gpu_cand_buf offset:0 atIndex:0];       // candidate_pairs
        [refineEnc setBuffer:gpu_polygons offset:0 atIndex:1];       // polygons
        [refineEnc setBuffer:gpu_rings offset:0 atIndex:2];          // rings
        [refineEnc setBuffer:gpu_vertices offset:0 atIndex:3];       // vertices
        [refineEnc setBuffer:gpu_points offset:0 atIndex:4];         // points
        [refineEnc setBuffer:gpu_refined_buf offset:0 atIndex:5];    // output_refined
        [refineEnc setBuffer:gpu_refined_count offset:0 atIndex:6];  // refined_count
        [refineEnc setBuffer:gpu_ncand_buf offset:0 atIndex:7];      // num_candidates
        [refineEnc setBuffer:gpu_maxref_buf offset:0 atIndex:8];     // max_results

        MTLSize refineGrid = MTLSizeMake(gpu_num_candidates, 1, 1);
        NSUInteger refineTg = std::min((NSUInteger)gpu_num_candidates, refinePipeline.maxTotalThreadsPerThreadgroup);
        [refineEnc dispatchThreads:refineGrid threadsPerThreadgroup:MTLSizeMake(refineTg > 0 ? refineTg : 1, 1, 1)];
        [refineEnc endEncoding];

        [refineCmd commit];
        [refineCmd waitUntilCompleted];
        auto gpu_refine_end = std::chrono::high_resolution_clock::now();
        double gpu_refine_ms = std::chrono::duration<double, std::milli>(gpu_refine_end - gpu_refine_start).count();

        uint32_t gpu_refined_hits = *(uint32_t*)[gpu_refined_count contents];
        CandidatePair* gpu_ref_raw = (CandidatePair*)[gpu_refined_buf contents];
        std::vector<CandidatePair> gpu_refined(gpu_ref_raw, gpu_ref_raw + gpu_refined_hits);

        std::cout << "Metal GPU Refiner:     " << gpu_refined.size() << " exact matches from "
                  << gpu_num_candidates << " candidates in " << gpu_refine_ms << " ms (Kernel + Sync)\n";

        // =====================================================================
        // Differential Assertions: 100% Exact Match & Parity
        // =====================================================================
        std::cout << "\n--- Verification Results ---\n";
        std::cout << "Stage 1 Candidates:           " << cpu_candidates.size() << "\n";
        std::cout << "Stage 2 Refined (CPU):        " << cpu_refined.size() << "\n";
        std::cout << "Stage 2 Refined (GPU):        " << gpu_refined.size() << "\n";
        uint32_t false_positives_eliminated = static_cast<uint32_t>(cpu_candidates.size() - gpu_refined.size());
        std::cout << "False Positives Eliminated:   " << false_positives_eliminated
                  << " (" << (100.0 * false_positives_eliminated / cpu_candidates.size()) << "% pruned)\n";

        // 1. Assert that non-trivial false-positives from bounding boxes were indeed eliminated
        assert(false_positives_eliminated > 0 && "Expected Stage 2 to prune false-positive candidate pairs!");

        // 2. Assert match count parity between CPU and GPU
        assert(gpu_refined_hits == cpu_refined.size() && "Exact match count mismatch between CPU and GPU!");

        // 3. Assert zero false-positives and zero false-negatives (order-independent set equality)
        std::sort(cpu_refined.begin(), cpu_refined.end());
        std::sort(gpu_refined.begin(), gpu_refined.end());
        assert(cpu_refined == gpu_refined && "Mismatch in exact refined pairs between CPU and Metal GPU!");

        // 4. Assert targeted false-positive points were correctly filtered:
        // Poly 0: empty corner points (pt 3 & 4) must NOT be in refined matches
        auto contains_pair = [](const std::vector<CandidatePair>& list, uint32_t poly, uint32_t pt) {
            return std::binary_search(list.begin(), list.end(), CandidatePair{poly, pt});
        };
        assert(!contains_pair(gpu_refined, 0, 3) && "L-shape empty corner point 3 must be filtered!");
        assert(!contains_pair(gpu_refined, 0, 4) && "L-shape empty corner point 4 must be filtered!");
        assert(contains_pair(gpu_refined, 0, 0) && "L-shape interior point 0 must be matched!");

        // Poly 1: rear notch point (pt 6) must NOT be in refined matches
        assert(!contains_pair(gpu_refined, 1, 6) && "Arrow notch point 6 must be filtered!");
        assert(contains_pair(gpu_refined, 1, 5) && "Arrow interior point 5 must be matched!");

        // Poly 2: donut hole point (pt 9) must NOT be in refined matches
        assert(!contains_pair(gpu_refined, 2, 9) && "Donut hole point 9 must be filtered!");
        assert(contains_pair(gpu_refined, 2, 7) && "Donut wall point 7 must be matched!");

        // Poly 3: donut holes (pt 12 & 13) must NOT be in refined matches
        assert(!contains_pair(gpu_refined, 3, 12) && "Multi-donut hole 1 point 12 must be filtered!");
        assert(!contains_pair(gpu_refined, 3, 13) && "Multi-donut hole 2 point 13 must be filtered!");
        assert(contains_pair(gpu_refined, 3, 11) && "Multi-donut inter-hole bridge point 11 must be matched!");

        // Poly 4: star bay point (pt 15) must NOT be in refined matches
        assert(!contains_pair(gpu_refined, 4, 15) && "Star bay point 15 must be filtered!");
        assert(contains_pair(gpu_refined, 4, 14) && "Star center point 14 must be matched!");

        std::cout << "\n>>> ALL ASSERTIONS PASSED! <<<\n";
        std::cout << ">>> Targeted non-convex & hole test cases verified with zero false positives. <<<\n";
        std::cout << ">>> DIFFERENTIAL VALIDATION PASSED: 100% exact parity between CPU and Metal GPU! <<<\n";
        std::cout << "=================================================================\n";
    }
    return 0;
}
