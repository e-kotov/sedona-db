#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <algorithm>

struct BoundingBox {
    float xmin;
    float ymin;
    float xmax;
    float ymax;
};

struct MatchPair {
    uint32_t build_idx;
    uint32_t probe_idx;
};

int main() {
    @autoreleasepool {
        std::cout << "========================================================\n";
        std::cout << " SedonaDB Metal Spatial Join POC (Apple Silicon)\n";
        std::cout << "========================================================\n";

        // 1. Initialize Metal Device
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) {
            std::cerr << "Error: No Metal device found.\n";
            return 1;
        }
        std::cout << "Using GPU Device: " << [[device name] UTF8String] << "\n";
        std::cout << "Unified Memory:   " << (device.hasUnifiedMemory ? "YES" : "NO") << "\n";

        // 2. Load and compile Metal shader from file
        NSError *error = nil;
        NSString *shaderPath = @"box_intersection.metal";
        NSString *shaderSource = [NSString stringWithContentsOfFile:shaderPath
                                                           encoding:NSUTF8StringEncoding
                                                              error:&error];
        if (error) {
            std::cerr << "Error reading shader file: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }

        id<MTLLibrary> library = [device newLibraryWithSource:shaderSource options:nil error:&error];
        if (!library) {
            std::cerr << "Shader compilation error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }

        id<MTLFunction> kernelFunc = [library newFunctionWithName:@"box_intersection_filter"];
        id<MTLComputePipelineState> pipelineState = [device newComputePipelineStateWithFunction:kernelFunc error:&error];
        if (!pipelineState) {
            std::cerr << "Pipeline state error: " << [[error localizedDescription] UTF8String] << "\n";
            return 1;
        }

        // 3. Define Test Geometries (Bounding Boxes)
        // Build side: 4 boxes
        std::vector<BoundingBox> build_boxes = {
            {0.0f, 0.0f, 10.0f, 10.0f},   // 0: [0,0] to [10,10]
            {20.0f, 20.0f, 30.0f, 30.0f}, // 1: [20,20] to [30,30]
            {40.0f, 40.0f, 50.0f, 50.0f}, // 2: [40,40] to [50,50]
            {5.0f, 5.0f, 25.0f, 25.0f}    // 3: Overlaps both Box 0 and Box 1
        };

        // Probe side: 3 boxes
        std::vector<BoundingBox> probe_boxes = {
            {2.0f, 2.0f, 8.0f, 8.0f},     // 0: Inside Box 0 and Box 3
            {22.0f, 22.0f, 28.0f, 28.0f}, // 1: Inside Box 1 and Box 3
            {100.0f, 100.0f, 110.0f, 110.0f} // 2: Disjoint from all
        };

        uint32_t num_build = static_cast<uint32_t>(build_boxes.size());
        uint32_t num_probe = static_cast<uint32_t>(probe_boxes.size());
        uint32_t max_results = 100;

        // 4. Create Metal Buffers (Shared memory on Apple Silicon)
        id<MTLBuffer> buildBuf = [device newBufferWithBytes:build_boxes.data()
                                                     length:sizeof(BoundingBox) * num_build
                                                    options:MTLResourceStorageModeShared];

        id<MTLBuffer> probeBuf = [device newBufferWithBytes:probe_boxes.data()
                                                     length:sizeof(BoundingBox) * num_probe
                                                    options:MTLResourceStorageModeShared];

        id<MTLBuffer> outputBuf = [device newBufferWithLength:sizeof(MatchPair) * max_results
                                                      options:MTLResourceStorageModeShared];

        uint32_t zero_count = 0;
        id<MTLBuffer> countBuf = [device newBufferWithBytes:&zero_count
                                                     length:sizeof(uint32_t)
                                                    options:MTLResourceStorageModeShared];

        id<MTLBuffer> numBuildBuf = [device newBufferWithBytes:&num_build
                                                        length:sizeof(uint32_t)
                                                       options:MTLResourceStorageModeShared];

        id<MTLBuffer> maxResultsBuf = [device newBufferWithBytes:&max_results
                                                          length:sizeof(uint32_t)
                                                         options:MTLResourceStorageModeShared];

        // 5. Encode and Dispatch GPU Command
        id<MTLCommandQueue> queue = [device newCommandQueue];
        id<MTLCommandBuffer> cmdBuffer = [queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [cmdBuffer computeCommandEncoder];

        [encoder setComputePipelineState:pipelineState];
        [encoder setBuffer:buildBuf offset:0 atIndex:0];
        [encoder setBuffer:probeBuf offset:0 atIndex:1];
        [encoder setBuffer:outputBuf offset:0 atIndex:2];
        [encoder setBuffer:countBuf offset:0 atIndex:3];
        [encoder setBuffer:numBuildBuf offset:0 atIndex:4];
        [encoder setBuffer:maxResultsBuf offset:0 atIndex:5];

        MTLSize gridSize = MTLSizeMake(num_probe, 1, 1);
        NSUInteger threadGroupSize = std::min((NSUInteger)num_probe, pipelineState.maxTotalThreadsPerThreadgroup);
        MTLSize threadsPerGroup = MTLSizeMake(threadGroupSize > 0 ? threadGroupSize : 1, 1, 1);

        [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadsPerGroup];
        [encoder endEncoding];

        // Commit and await GPU completion
        [cmdBuffer commit];
        [cmdBuffer waitUntilCompleted];

        // 6. Inspect Results
        uint32_t totalHits = *(uint32_t*)[countBuf contents];
        MatchPair* pairs = (MatchPair*)[outputBuf contents];

        std::cout << "GPU Execution Completed.\n";
        std::cout << "Total candidate match pairs found: " << totalHits << "\n";

        for (uint32_t i = 0; i < totalHits && i < max_results; ++i) {
            std::cout << "  Pair " << i << ": Build Box " << pairs[i].build_idx
                      << " <-> Probe Box " << pairs[i].probe_idx << "\n";
        }

        // Expected matches:
        // Probe 0 intersects Build 0 and Build 3 (2 matches)
        // Probe 1 intersects Build 1 and Build 3 (2 matches)
        // Probe 2 intersects nothing (0 matches)
        // Total expected = 4 matches
        assert(totalHits == 4 && "Expected exactly 4 candidate match pairs!");
        std::cout << "\n>>> Assertion PASSED: Small sanity test verified! <<<\n\n";

        // =====================================================================
        // Test 2: Differential Test: CPU Ground-Truth vs. Metal GPU Acceleration
        // =====================================================================
        std::cout << "--- Differential Test: CPU vs. Metal GPU (Randomized Dataset) ---\n";

        const uint32_t N_BUILD = 2000;
        const uint32_t N_PROBE = 2000;
        const uint32_t MAX_RESULTS = 500000;

        // Fixed seed pseudo-random generator for 100% reproducible tests across CI runs
        std::vector<BoundingBox> large_build(N_BUILD);
        std::vector<BoundingBox> large_probe(N_PROBE);

        uint32_t lcg = 123456789;
        auto next_float = [&lcg](float min_val, float max_val) {
            lcg = lcg * 1664525u + 1013904223u;
            float normalized = (float)(lcg >> 8) / 16777216.0f;
            return min_val + normalized * (max_val - min_val);
        };

        for (uint32_t i = 0; i < N_BUILD; ++i) {
            float x = next_float(0.0f, 1000.0f);
            float y = next_float(0.0f, 1000.0f);
            float w = next_float(1.0f, 30.0f);
            float h = next_float(1.0f, 30.0f);
            large_build[i] = {x, y, x + w, y + h};
        }

        for (uint32_t i = 0; i < N_PROBE; ++i) {
            float x = next_float(0.0f, 1000.0f);
            float y = next_float(0.0f, 1000.0f);
            float w = next_float(1.0f, 30.0f);
            float h = next_float(1.0f, 30.0f);
            large_probe[i] = {x, y, x + w, y + h};
        }

        // 1. Run CPU Reference Baseline
        auto cpu_start = std::chrono::high_resolution_clock::now();
        std::vector<std::pair<uint32_t, uint32_t>> cpu_pairs;
        cpu_pairs.reserve(10000);

        auto cpu_intersects = [](BoundingBox a, BoundingBox b) {
            return !(a.xmax < b.xmin || a.xmin > b.xmax || a.ymax < b.ymin || a.ymin > b.ymax);
        };

        for (uint32_t p = 0; p < N_PROBE; ++p) {
            for (uint32_t b = 0; b < N_BUILD; ++b) {
                if (cpu_intersects(large_build[b], large_probe[p])) {
                    cpu_pairs.push_back({b, p});
                }
            }
        }
        auto cpu_end = std::chrono::high_resolution_clock::now();
        double cpu_ms = std::chrono::duration<double, std::milli>(cpu_end - cpu_start).count();
        std::cout << "CPU Ground Truth: found " << cpu_pairs.size() << " matches in " << cpu_ms << " ms\n";

        // 2. Run Metal GPU Implementation
        id<MTLBuffer> gpu_build_buf = [device newBufferWithBytes:large_build.data()
                                                          length:sizeof(BoundingBox) * N_BUILD
                                                         options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_probe_buf = [device newBufferWithBytes:large_probe.data()
                                                          length:sizeof(BoundingBox) * N_PROBE
                                                         options:MTLResourceStorageModeShared];
        id<MTLBuffer> gpu_out_buf = [device newBufferWithLength:sizeof(MatchPair) * MAX_RESULTS
                                                        options:MTLResourceStorageModeShared];
        uint32_t zero = 0;
        id<MTLBuffer> gpu_count_buf = [device newBufferWithBytes:&zero
                                                          length:sizeof(uint32_t)
                                                         options:MTLResourceStorageModeShared];
        uint32_t build_count_param = N_BUILD;
        id<MTLBuffer> gpu_nbuild_buf = [device newBufferWithBytes:&build_count_param
                                                           length:sizeof(uint32_t)
                                                          options:MTLResourceStorageModeShared];
        uint32_t max_results_param = MAX_RESULTS;
        id<MTLBuffer> gpu_max_buf = [device newBufferWithBytes:&max_results_param
                                                        length:sizeof(uint32_t)
                                                       options:MTLResourceStorageModeShared];

        auto gpu_start = std::chrono::high_resolution_clock::now();
        id<MTLCommandBuffer> diffCmd = [queue commandBuffer];
        id<MTLComputeCommandEncoder> diffEnc = [diffCmd computeCommandEncoder];

        [diffEnc setComputePipelineState:pipelineState];
        [diffEnc setBuffer:gpu_build_buf offset:0 atIndex:0];
        [diffEnc setBuffer:gpu_probe_buf offset:0 atIndex:1];
        [diffEnc setBuffer:gpu_out_buf offset:0 atIndex:2];
        [diffEnc setBuffer:gpu_count_buf offset:0 atIndex:3];
        [diffEnc setBuffer:gpu_nbuild_buf offset:0 atIndex:4];
        [diffEnc setBuffer:gpu_max_buf offset:0 atIndex:5];

        MTLSize diffGridSize = MTLSizeMake(N_PROBE, 1, 1);
        NSUInteger diffThreadsPerGroup = std::min((NSUInteger)N_PROBE, pipelineState.maxTotalThreadsPerThreadgroup);
        [diffEnc dispatchThreads:diffGridSize threadsPerThreadgroup:MTLSizeMake(diffThreadsPerGroup, 1, 1)];
        [diffEnc endEncoding];

        [diffCmd commit];
        [diffCmd waitUntilCompleted];
        auto gpu_end = std::chrono::high_resolution_clock::now();
        double gpu_ms = std::chrono::duration<double, std::milli>(gpu_end - gpu_start).count();

        uint32_t gpu_hits = *(uint32_t*)[gpu_count_buf contents];
        MatchPair* gpu_pairs_raw = (MatchPair*)[gpu_out_buf contents];
        std::cout << "Metal GPU:        found " << gpu_hits << " matches in " << gpu_ms << " ms (Kernel + Sync)\n";

        // 3. Exact Verification: GPU results must match CPU results 1:1
        assert(gpu_hits == cpu_pairs.size() && "Match count mismatch between CPU and GPU!");

        // Sort both collections to ensure order-independent 1:1 match
        std::vector<std::pair<uint32_t, uint32_t>> gpu_pairs;
        gpu_pairs.reserve(gpu_hits);
        for (uint32_t i = 0; i < gpu_hits; ++i) {
            gpu_pairs.push_back({gpu_pairs_raw[i].build_idx, gpu_pairs_raw[i].probe_idx});
        }
        std::sort(cpu_pairs.begin(), cpu_pairs.end());
        std::sort(gpu_pairs.begin(), gpu_pairs.end());

        assert(cpu_pairs == gpu_pairs && "Mismatch in matched pairs between CPU and GPU!");

        std::cout << "\n>>> DIFFERENTIAL VALIDATION PASSED: 100% exact parity between CPU and Metal GPU! <<<\n";
    }
    return 0;
}
