#pragma once

#ifdef __METAL_VERSION__
#include <metal_stdlib>
using namespace metal;
#else
#include <cstdint>
#include <cmath>
#include <algorithm>
#endif

// Contiguous 2D point coordinate (GeoArrow flat coordinates)
struct Point2D {
    float x;
    float y;

#ifndef __METAL_VERSION__
    bool operator==(const Point2D& o) const {
        return x == o.x && y == o.y;
    }
#endif
};

#ifndef SEDONA_BOUNDING_BOX_DEFINED
#define SEDONA_BOUNDING_BOX_DEFINED
// 2D Axis-Aligned Bounding Box
struct BoundingBox {
    float xmin;
    float ymin;
    float xmax;
    float ymax;
};
#endif

// Ring descriptor referencing a contiguous slice of Point2D vertices
struct PolygonRing {
    uint32_t start_idx;   // Offset into the vertices buffer
    uint32_t num_points;  // Number of vertices in the ring
};

// Polygon descriptor matching GeoArrow nested list offsets
// Outer ring is at rings[outer_ring_idx]
// Interior rings (holes) are at rings[outer_ring_idx + 1 .. outer_ring_idx + num_interior_rings]
struct PolygonGeom {
    uint32_t outer_ring_idx;
    uint32_t num_interior_rings;
};

// Candidate match pair (from Stage 1 AABB bounding box filter)
struct CandidatePair {
    uint32_t polygon_idx;
    uint32_t point_idx;

#ifndef __METAL_VERSION__
    bool operator==(const CandidatePair& o) const {
        return polygon_idx == o.polygon_idx && point_idx == o.point_idx;
    }
    bool operator<(const CandidatePair& o) const {
        if (polygon_idx != o.polygon_idx) return polygon_idx < o.polygon_idx;
        return point_idx < o.point_idx;
    }
#endif
};

// Point Location relative to a polygon or ring
enum PointLocation : uint32_t {
    kPointInside = 0,
    kPointBoundary = 1,
    kPointOutside = 2
};
