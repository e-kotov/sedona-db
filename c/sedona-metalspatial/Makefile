# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

CXX = clang++
CXXFLAGS = -std=c++20 -O3 -Wall -Wextra -fobjc-arc
LDFLAGS = -framework Metal -framework Foundation

REFINE_BIN = test_refine_bin
INDEX_SCALE_BIN = test_index_scale_bin
BENCH_DIR = bench
POC_TARGET = $(BENCH_DIR)/poc_metal
BENCH_HEAVY_BIN = $(BENCH_DIR)/test_large_scale_bench_bin

all: metal_shaders.h $(REFINE_BIN) $(INDEX_SCALE_BIN)

metal_shaders.h: bvh.metal spatial_hash.metal refine.metal
	cargo check --package sedona-metalspatial

$(REFINE_BIN): test_refine.mm refine.metal geom_types.hpp box_intersection.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) test_refine.mm -o $(REFINE_BIN)

$(INDEX_SCALE_BIN): test_index_scale.mm spatial_index.mm spatial_index.hpp metal_shaders.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) test_index_scale.mm spatial_index.mm -o $(INDEX_SCALE_BIN)

$(POC_TARGET): $(BENCH_DIR)/main.mm box_intersection.metal
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(BENCH_DIR)/main.mm -o $(POC_TARGET)

$(BENCH_HEAVY_BIN): $(BENCH_DIR)/test_large_scale_bench.mm spatial_index.mm spatial_index.hpp metal_shaders.h
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(BENCH_DIR)/test_large_scale_bench.mm spatial_index.mm -o $(BENCH_HEAVY_BIN)

test_refine: $(REFINE_BIN)
	./$(REFINE_BIN)

test_index_scale: $(INDEX_SCALE_BIN)
	./$(INDEX_SCALE_BIN)

bench: $(POC_TARGET) $(BENCH_HEAVY_BIN)
	./$(POC_TARGET)
	./$(BENCH_HEAVY_BIN)

clean:
	rm -rf $(REFINE_BIN) $(INDEX_SCALE_BIN) $(POC_TARGET) $(BENCH_HEAVY_BIN) *.o *.dSYM $(BENCH_DIR)/*.o $(BENCH_DIR)/*.dSYM

.PHONY: all clean test_refine test_index_scale bench
