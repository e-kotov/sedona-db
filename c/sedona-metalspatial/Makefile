CXX = clang++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra -fobjc-arc
LDFLAGS = -framework Metal -framework Foundation -framework MetalKit

POC_TARGET = poc_metal
REFINE_BIN = test_refine_bin
INDEX_SCALE_BIN = test_index_scale_bin

all: $(POC_TARGET) $(REFINE_BIN) $(INDEX_SCALE_BIN)

$(POC_TARGET): main.mm box_intersection.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) main.mm -o $(POC_TARGET)

$(REFINE_BIN): test_refine.mm refine.metal geom_types.hpp box_intersection.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) test_refine.mm -o $(REFINE_BIN)

$(INDEX_SCALE_BIN): test_index_scale.mm spatial_index.mm spatial_index.hpp bvh.metal spatial_hash.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) test_index_scale.mm spatial_index.mm -o $(INDEX_SCALE_BIN)

BENCH_HEAVY_BIN = test_large_scale_bench_bin
$(BENCH_HEAVY_BIN): test_large_scale_bench.mm spatial_index.mm spatial_index.hpp refine.metal spatial_hash.metal bvh.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) test_large_scale_bench.mm spatial_index.mm -o $(BENCH_HEAVY_BIN)

test_refine: $(REFINE_BIN)
	./$(REFINE_BIN)

test_index_scale: $(INDEX_SCALE_BIN)
	./$(INDEX_SCALE_BIN)

# [TEMPORARY / TO BE REMOVED BENCHMARK]
bench_heavy: $(BENCH_HEAVY_BIN)
	./$(BENCH_HEAVY_BIN)

run: $(POC_TARGET)
	./$(POC_TARGET)

clean:
	rm -rf $(POC_TARGET) $(REFINE_BIN) $(INDEX_SCALE_BIN) $(BENCH_HEAVY_BIN) *.o *.dSYM

.PHONY: all run clean test_refine test_index_scale bench_heavy
