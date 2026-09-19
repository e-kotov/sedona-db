#import "spatial_index.hpp"
#import <Metal/Metal.h>
#include <iostream>
#include <vector>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <mutex>

// MSL Source for Hardware Ray Tracing BVH
static const char* BVH_METAL_SOURCE = R"(
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

    ray r;
    r.origin = float3(px, py, -1.0f);
    r.direction = float3(0.0f, 0.0f, 1.0f);
    r.min_distance = 0.0f;
    r.max_distance = 2.0f;

    intersection_query<> q(r, accel);
    while (q.next()) {
        if (q.get_candidate_intersection_type() == intersection_type::bounding_box) {
            uint build_id = q.get_candidate_primitive_id();
            BoundingBox b = build_boxes[build_id];
            if (px >= b.xmin && px <= b.xmax && py >= b.ymin && py <= b.ymax) {
                uint slot = atomic_fetch_add_explicit(match_count, 1, memory_order_relaxed);
                if (slot < max_results) {
                    output_pairs[slot] = MatchPair{build_id, tid};
                }
            }
        }
    }
}
)";

// MSL Source for Fast Compute Spatial Hash (2D Uniform Grid)
static const char* SPATIAL_HASH_METAL_SOURCE = R"(
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

inline bool boxes_intersect(BoundingBox a, BoundingBox b) {
    return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
}

inline void get_cell_range(BoundingBox b, constant GridParams& p,
                           thread int& min_cx, thread int& max_cx,
                           thread int& min_cy, thread int& max_cy)
{
    min_cx = clamp(int((b.xmin - p.min_x) / p.cell_w), 0, int(p.grid_dim_x - 1));
    max_cx = clamp(int((b.xmax - p.min_x) / p.cell_w), 0, int(p.grid_dim_x - 1));
    min_cy = clamp(int((b.ymin - p.min_y) / p.cell_h), 0, int(p.grid_dim_y - 1));
    max_cy = clamp(int((b.ymax - p.min_y) / p.cell_h), 0, int(p.grid_dim_y - 1));
}

kernel void count_cell_entries(
    device const BoundingBox* boxes       [[buffer(0)]],
    device atomic_uint*       cell_counts [[buffer(1)]],
    constant GridParams&      p           [[buffer(2)]],
    uint                      tid         [[thread_position_in_grid]])
{
    if (tid >= p.num_build) return;
    BoundingBox b = boxes[tid];
    int min_cx, max_cx, min_cy, max_cy;
    get_cell_range(b, p, min_cx, max_cx, min_cy, max_cy);

    for (int y = min_cy; y <= max_cy; ++y) {
        for (int x = min_cx; x <= max_cx; ++x) {
            uint cell_idx = y * p.grid_dim_x + x;
            atomic_fetch_add_explicit(&cell_counts[cell_idx], 1, memory_order_relaxed);
        }
    }
}

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
    if (tid >= p.num_probe) return;
    BoundingBox probe = probe_boxes[tid];

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
)";

struct GridParamsInternal {
    float min_x;
    float min_y;
    float cell_w;
    float cell_h;
    uint32_t grid_dim_x;
    uint32_t grid_dim_y;
    uint32_t num_build;
    uint32_t num_probe;
    uint32_t max_results;
};

struct MetalSpatialIndex::Impl {
    id<MTLDevice> device = nil;
    id<MTLCommandQueue> queue = nil;

    IndexType requested_type = IndexType::Auto;
    std::atomic<IndexType> active_type{IndexType::SpatialHash};
    bool has_hw_rt = false;
    std::atomic<bool> is_built{false};
    std::mutex build_mutex;

    // Compiled pipelines
    id<MTLComputePipelineState> pso_rt_probe = nil;
    id<MTLComputePipelineState> pso_grid_count = nil;
    id<MTLComputePipelineState> pso_grid_populate = nil;
    id<MTLComputePipelineState> pso_grid_probe = nil;

    // Build data
    std::vector<BoundingBox> build_boxes;
    id<MTLBuffer> buf_build_boxes = nil;

    // Hardware RT Acceleration Structure
    id<MTLAccelerationStructure> rt_accel = nil;
    id<MTLBuffer> buf_rt_bboxes = nil;

    // Spatial Hash Grid structures
    GridParamsInternal grid_params{};
    id<MTLBuffer> buf_grid_offsets = nil;
    id<MTLBuffer> buf_grid_entries = nil;

    // Diagnostics
    double last_build_time_ms = 0.0;
    std::atomic<double> last_probe_time_ms{0.0};

    void init_pipelines() {
        @autoreleasepool {
            has_hw_rt = [device supportsRaytracing];

            NSError *err = nil;
            MTLCompileOptions *opts = [MTLCompileOptions new];
            opts.languageVersion = MTLLanguageVersion3_0;

            // 1. Compile Spatial Hash Shaders
            NSString *hashSrc = [NSString stringWithUTF8String:SPATIAL_HASH_METAL_SOURCE];
            id<MTLLibrary> hashLib = [device newLibraryWithSource:hashSrc options:opts error:&err];
            if (!hashLib) {
                std::cerr << "Error compiling Spatial Hash shaders: " << [[err localizedDescription] UTF8String] << "\n";
            } else {
                id<MTLFunction> fnCount = [hashLib newFunctionWithName:@"count_cell_entries"];
                id<MTLFunction> fnPop = [hashLib newFunctionWithName:@"populate_cells"];
                id<MTLFunction> fnProbe = [hashLib newFunctionWithName:@"probe_grid"];
                pso_grid_count = [device newComputePipelineStateWithFunction:fnCount error:&err];
                pso_grid_populate = [device newComputePipelineStateWithFunction:fnPop error:&err];
                pso_grid_probe = [device newComputePipelineStateWithFunction:fnProbe error:&err];
            }

            // 2. Compile Hardware Ray Tracing Shaders (if supported)
            if (has_hw_rt) {
                NSString *rtSrc = [NSString stringWithUTF8String:BVH_METAL_SOURCE];
                id<MTLLibrary> rtLib = [device newLibraryWithSource:rtSrc options:opts error:&err];
                if (!rtLib) {
                    std::cerr << "Hardware RT shader compilation notice: " << [[err localizedDescription] UTF8String] << "\n";
                    has_hw_rt = false;
                } else {
                    id<MTLFunction> fnRtProbe = [rtLib newFunctionWithName:@"rt_probe_points"];
                    pso_rt_probe = [device newComputePipelineStateWithFunction:fnRtProbe error:&err];
                    if (!pso_rt_probe) {
                        std::cerr << "Hardware RT PSO error: " << [[err localizedDescription] UTF8String] << "\n";
                        has_hw_rt = false;
                    }
                }
            }
        }
    }

    void build_rt_index() {
        @autoreleasepool {
            uint32_t n = static_cast<uint32_t>(build_boxes.size());
            if (n == 0) return;
            std::vector<MTLAxisAlignedBoundingBox> mtl_boxes(n);
            // Expand conservative bounds to prevent grazing-ray precision misses on boundaries across any coordinate scale
            for (uint32_t i = 0; i < n; ++i) {
                const auto& b = build_boxes[i];
                float dx = std::max(1e-4f, (b.xmax - b.xmin) * 1e-4f + std::abs(b.xmax) * 1e-6f);
                float dy = std::max(1e-4f, (b.ymax - b.ymin) * 1e-4f + std::abs(b.ymax) * 1e-6f);
                mtl_boxes[i].min = MTLPackedFloat3Make(b.xmin - dx, b.ymin - dy, -0.5f);
                mtl_boxes[i].max = MTLPackedFloat3Make(b.xmax + dx, b.ymax + dy, 0.5f);
            }

            buf_rt_bboxes = [device newBufferWithBytes:mtl_boxes.data()
                                                length:sizeof(MTLAxisAlignedBoundingBox) * n
                                               options:MTLResourceStorageModeShared];

            MTLAccelerationStructureBoundingBoxGeometryDescriptor *geomDesc =
                [MTLAccelerationStructureBoundingBoxGeometryDescriptor descriptor];
            geomDesc.boundingBoxBuffer = buf_rt_bboxes;
            geomDesc.boundingBoxBufferOffset = 0;
            geomDesc.boundingBoxCount = n;
            geomDesc.boundingBoxStride = sizeof(MTLAxisAlignedBoundingBox);

            MTLPrimitiveAccelerationStructureDescriptor *accelDesc =
                [MTLPrimitiveAccelerationStructureDescriptor descriptor];
            accelDesc.geometryDescriptors = @[geomDesc];

            MTLAccelerationStructureSizes sizes = [device accelerationStructureSizesWithDescriptor:accelDesc];
            NSUInteger accelSize = std::max(sizes.accelerationStructureSize, (NSUInteger)256);
            NSUInteger scratchSize = std::max(sizes.buildScratchBufferSize, (NSUInteger)256);

            rt_accel = [device newAccelerationStructureWithSize:accelSize];
            id<MTLBuffer> scratch = [device newBufferWithLength:scratchSize
                                                        options:MTLResourceStorageModePrivate];

            id<MTLCommandBuffer> cmd = [queue commandBuffer];
            id<MTLAccelerationStructureCommandEncoder> enc = [cmd accelerationStructureCommandEncoder];
            [enc buildAccelerationStructure:rt_accel descriptor:accelDesc scratchBuffer:scratch scratchBufferOffset:0];
            [enc endEncoding];
            [cmd commit];
            [cmd waitUntilCompleted];
        }
    }

    void build_grid_index() {
        @autoreleasepool {
            uint32_t n = static_cast<uint32_t>(build_boxes.size());
            if (n == 0) return;
            float min_x = 1e30f, min_y = 1e30f, max_x = -1e30f, max_y = -1e30f;
            for (uint32_t i = 0; i < n; ++i) {
                min_x = std::min(min_x, build_boxes[i].xmin);
                min_y = std::min(min_y, build_boxes[i].ymin);
                max_x = std::max(max_x, build_boxes[i].xmax);
                max_y = std::max(max_y, build_boxes[i].ymax);
            }

            // Grid resolution: heuristically balance cell occupancy
            uint32_t grid_dim = static_cast<uint32_t>(std::clamp(static_cast<float>(std::sqrt(n) * 1.2), 32.0f, 512.0f));
            uint32_t num_cells = grid_dim * grid_dim;

            float span_x = std::max(max_x - min_x, 1e-4f);
            float span_y = std::max(max_y - min_y, 1e-4f);

            grid_params.min_x = min_x - span_x * 0.001f;
            grid_params.min_y = min_y - span_y * 0.001f;
            grid_params.cell_w = (span_x * 1.002f) / grid_dim;
            grid_params.cell_h = (span_y * 1.002f) / grid_dim;
            grid_params.grid_dim_x = grid_dim;
            grid_params.grid_dim_y = grid_dim;
            grid_params.num_build = n;

            if (!buf_build_boxes) {
                buf_build_boxes = [device newBufferWithBytes:build_boxes.data()
                                                      length:sizeof(BoundingBox) * n
                                                     options:MTLResourceStorageModeShared];
            }

            id<MTLBuffer> buf_counts = [device newBufferWithLength:sizeof(uint32_t) * num_cells
                                                           options:MTLResourceStorageModeShared];
            memset([buf_counts contents], 0, sizeof(uint32_t) * num_cells);

            id<MTLBuffer> buf_params = [device newBufferWithBytes:&grid_params
                                                           length:sizeof(GridParamsInternal)
                                                          options:MTLResourceStorageModeShared];

            // 1. Count entries per cell
            id<MTLCommandBuffer> cmd1 = [queue commandBuffer];
            id<MTLComputeCommandEncoder> enc1 = [cmd1 computeCommandEncoder];
            [enc1 setComputePipelineState:pso_grid_count];
            [enc1 setBuffer:buf_build_boxes offset:0 atIndex:0];
            [enc1 setBuffer:buf_counts offset:0 atIndex:1];
            [enc1 setBuffer:buf_params offset:0 atIndex:2];
            [enc1 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
            [enc1 endEncoding];
            [cmd1 commit];
            [cmd1 waitUntilCompleted];

            // 2. Prefix sum for cell offsets
            uint32_t* counts = (uint32_t*)[buf_counts contents];
            std::vector<uint32_t> offsets(num_cells + 1, 0);
            for (uint32_t i = 0; i < num_cells; ++i) {
                offsets[i + 1] = offsets[i] + counts[i];
            }
            uint32_t total_entries = offsets[num_cells];

            buf_grid_offsets = [device newBufferWithBytes:offsets.data()
                                                   length:sizeof(uint32_t) * (num_cells + 1)
                                                  options:MTLResourceStorageModeShared];

            buf_grid_entries = [device newBufferWithLength:sizeof(uint32_t) * std::max(1u, total_entries)
                                                   options:MTLResourceStorageModeShared];

            id<MTLBuffer> buf_heads = [device newBufferWithLength:sizeof(uint32_t) * num_cells
                                                          options:MTLResourceStorageModeShared];
            memset([buf_heads contents], 0, sizeof(uint32_t) * num_cells);

            // 3. Populate cell entries
            id<MTLCommandBuffer> cmd2 = [queue commandBuffer];
            id<MTLComputeCommandEncoder> enc2 = [cmd2 computeCommandEncoder];
            [enc2 setComputePipelineState:pso_grid_populate];
            [enc2 setBuffer:buf_build_boxes offset:0 atIndex:0];
            [enc2 setBuffer:buf_heads offset:0 atIndex:1];
            [enc2 setBuffer:buf_grid_offsets offset:0 atIndex:2];
            [enc2 setBuffer:buf_grid_entries offset:0 atIndex:3];
            [enc2 setBuffer:buf_params offset:0 atIndex:4];
            [enc2 dispatchThreads:MTLSizeMake(n, 1, 1) threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
            [enc2 endEncoding];
            [cmd2 commit];
            [cmd2 waitUntilCompleted];
        }
    }
};

MetalSpatialIndex::MetalSpatialIndex(MetalDeviceHandle device)
    : impl_(std::make_unique<Impl>())
{
    @autoreleasepool {
        id<MTLDevice> mtl_dev = (id<MTLDevice>)device;
        if (!mtl_dev) {
            mtl_dev = MTLCreateSystemDefaultDevice();
        }
        impl_->device = mtl_dev;
        if (mtl_dev) {
            impl_->queue = [mtl_dev newCommandQueue];
            impl_->init_pipelines();
        }
    }
}

MetalSpatialIndex::~MetalSpatialIndex() = default;

void MetalSpatialIndex::set_index_type(IndexType type) {
    impl_->requested_type = type;
}

IndexType MetalSpatialIndex::get_index_type() const {
    return impl_->requested_type;
}

IndexType MetalSpatialIndex::get_active_index_type() const {
    return impl_->active_type.load();
}

bool MetalSpatialIndex::supports_hardware_rt() const {
    return impl_->has_hw_rt;
}

bool MetalSpatialIndex::is_valid() const {
    return impl_->device != nil && impl_->queue != nil;
}

void MetalSpatialIndex::push_build(const float* rects_flat, uint32_t count) {
    if (!rects_flat || count == 0) return;
    const auto* boxes = reinterpret_cast<const BoundingBox*>(rects_flat);
    impl_->build_boxes.insert(impl_->build_boxes.end(), boxes, boxes + count);
    impl_->is_built.store(false);
    impl_->buf_build_boxes = nil;
    impl_->rt_accel = nil;
    impl_->buf_rt_bboxes = nil;
    impl_->buf_grid_offsets = nil;
    impl_->buf_grid_entries = nil;
}

void MetalSpatialIndex::finish_building() {
    if (impl_->build_boxes.empty() || impl_->is_built.load()) return;

    auto t0 = std::chrono::high_resolution_clock::now();

    uint32_t n = static_cast<uint32_t>(impl_->build_boxes.size());
    impl_->buf_build_boxes = [impl_->device newBufferWithBytes:impl_->build_boxes.data()
                                                        length:sizeof(BoundingBox) * n
                                                       options:MTLResourceStorageModeShared];

    // Build Spatial Hash Grid (always available as universal engine / fast fallback)
    impl_->build_grid_index();

    // If Hardware RT is requested or Auto, build Hardware BVH
    if (impl_->has_hw_rt && (impl_->requested_type == IndexType::HardwareRT || impl_->requested_type == IndexType::Auto)) {
        impl_->build_rt_index();
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    impl_->last_build_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    impl_->is_built.store(true);
}

void MetalSpatialIndex::probe(const float* rects_flat, uint32_t count,
                              std::vector<uint32_t>& out_build, std::vector<uint32_t>& out_probe)
{
    out_build.clear();
    out_probe.clear();
    if (!rects_flat || count == 0 || impl_->build_boxes.empty()) return;
    if (!impl_->device || !impl_->queue) return;

    if (!impl_->is_built.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(impl_->build_mutex);
        if (!impl_->is_built.load(std::memory_order_relaxed)) {
            finish_building();
        }
    }

    @autoreleasepool {
        const auto* probe_boxes = reinterpret_cast<const BoundingBox*>(rects_flat);

        // Detect if all probe geometries are points (xmin == xmax && ymin == ymax)
        bool is_points = true;
        for (uint32_t i = 0; i < count; ++i) {
            if (probe_boxes[i].xmin != probe_boxes[i].xmax || probe_boxes[i].ymin != probe_boxes[i].ymax) {
                is_points = false;
                break;
            }
        }

        // Determine active engine
        bool use_rt = false;
        if (impl_->requested_type == IndexType::HardwareRT) {
            // Hardware RT traverses 1D rays along Z, perfectly suited for points.
            // If probe geometries are 2D boxes, seamlessly fall back to Spatial Hash
            // to ensure 100% geometric correctness without false negatives.
            use_rt = impl_->has_hw_rt && (impl_->rt_accel != nil) && is_points;
        } else if (impl_->requested_type == IndexType::Auto) {
            use_rt = impl_->has_hw_rt && (impl_->rt_accel != nil) && is_points;
        }

        impl_->active_type.store(use_rt ? IndexType::HardwareRT : IndexType::SpatialHash, std::memory_order_relaxed);

        auto t0 = std::chrono::high_resolution_clock::now();

        id<MTLBuffer> buf_probe = [impl_->device newBufferWithBytes:rects_flat
                                                             length:sizeof(BoundingBox) * count
                                                            options:MTLResourceStorageModeShared];
        if (!buf_probe) return;

        uint32_t max_results = std::max(count * 8u, 1000000u);
        __block id<MTLBuffer> buf_out = [impl_->device newBufferWithLength:sizeof(MatchPair) * max_results
                                                                   options:MTLResourceStorageModeShared];
        id<MTLBuffer> buf_count = [impl_->device newBufferWithLength:sizeof(uint32_t)
                                                             options:MTLResourceStorageModeShared];
        if (!buf_out || !buf_count || ![buf_count contents] || ![buf_out contents]) return;
        *((uint32_t*)[buf_count contents]) = 0;

        auto dispatch_query = ^(uint32_t curr_max) {
            if (use_rt) {
                id<MTLBuffer> buf_num_probes = [impl_->device newBufferWithBytes:&count length:sizeof(uint32_t) options:MTLResourceStorageModeShared];
                id<MTLBuffer> buf_max_results = [impl_->device newBufferWithBytes:&curr_max length:sizeof(uint32_t) options:MTLResourceStorageModeShared];

                id<MTLCommandBuffer> cmd = [impl_->queue commandBuffer];
                id<MTLComputeCommandEncoder> enc = [cmd computeCommandEncoder];
                [enc setComputePipelineState:impl_->pso_rt_probe];
                [enc setAccelerationStructure:impl_->rt_accel atBufferIndex:0];
                [enc setBuffer:impl_->buf_build_boxes offset:0 atIndex:1];
                [enc setBuffer:buf_probe offset:0 atIndex:2];
                [enc setBuffer:buf_out offset:0 atIndex:3];
                [enc setBuffer:buf_count offset:0 atIndex:4];
                [enc setBuffer:buf_num_probes offset:0 atIndex:5];
                [enc setBuffer:buf_max_results offset:0 atIndex:6];
                [enc dispatchThreads:MTLSizeMake(count, 1, 1) threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
                [enc endEncoding];
                [cmd commit];
                [cmd waitUntilCompleted];
            } else {
                GridParamsInternal params = impl_->grid_params;
                params.num_probe = count;
                params.max_results = curr_max;

                id<MTLBuffer> buf_params = [impl_->device newBufferWithBytes:&params
                                                                      length:sizeof(GridParamsInternal)
                                                                     options:MTLResourceStorageModeShared];

                id<MTLCommandBuffer> cmd = [impl_->queue commandBuffer];
                id<MTLComputeCommandEncoder> enc = [cmd computeCommandEncoder];
                [enc setComputePipelineState:impl_->pso_grid_probe];
                [enc setBuffer:impl_->buf_build_boxes offset:0 atIndex:0];
                [enc setBuffer:buf_probe offset:0 atIndex:1];
                [enc setBuffer:impl_->buf_grid_offsets offset:0 atIndex:2];
                [enc setBuffer:impl_->buf_grid_entries offset:0 atIndex:3];
                [enc setBuffer:buf_out offset:0 atIndex:4];
                [enc setBuffer:buf_count offset:0 atIndex:5];
                [enc setBuffer:buf_params offset:0 atIndex:6];
                [enc dispatchThreads:MTLSizeMake(count, 1, 1) threadsPerThreadgroup:MTLSizeMake(256, 1, 1)];
                [enc endEncoding];
                [cmd commit];
                [cmd waitUntilCompleted];
            }
        };

        dispatch_query(max_results);

        uint32_t total_matches = *((uint32_t*)[buf_count contents]);
        if (total_matches > max_results) {
            // Buffer capacity exceeded: dynamically reallocate exact needed capacity and re-run
            // to guarantee zero truncation and 100% completeness.
            max_results = total_matches;
            buf_out = [impl_->device newBufferWithLength:sizeof(MatchPair) * max_results
                                                 options:MTLResourceStorageModeShared];
            if (!buf_out || ![buf_out contents]) return;
            *((uint32_t*)[buf_count contents]) = 0;
            dispatch_query(max_results);
            total_matches = *((uint32_t*)[buf_count contents]);
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        impl_->last_probe_time_ms.store(std::chrono::duration<double, std::milli>(t1 - t0).count(), std::memory_order_relaxed);

        uint32_t valid_matches = std::min(total_matches, max_results);
        const auto* pairs = reinterpret_cast<const MatchPair*>([buf_out contents]);
        out_build.resize(valid_matches);
        out_probe.resize(valid_matches);

        for (uint32_t i = 0; i < valid_matches; ++i) {
            out_build[i] = pairs[i].build_idx;
            out_probe[i] = pairs[i].probe_idx;
        }
    }
}

double MetalSpatialIndex::get_last_build_time_ms() const {
    return impl_->last_build_time_ms;
}

double MetalSpatialIndex::get_last_probe_time_ms() const {
    return impl_->last_probe_time_ms.load();
}

uint32_t MetalSpatialIndex::get_build_count() const {
    return static_cast<uint32_t>(impl_->build_boxes.size());
}

void MetalSpatialIndex::clear() {
    impl_->build_boxes.clear();
    impl_->buf_build_boxes = nil;
    impl_->rt_accel = nil;
    impl_->buf_rt_bboxes = nil;
    impl_->buf_grid_offsets = nil;
    impl_->buf_grid_entries = nil;
    impl_->is_built.store(false);
    impl_->last_build_time_ms = 0.0;
    impl_->last_probe_time_ms.store(0.0);
}
