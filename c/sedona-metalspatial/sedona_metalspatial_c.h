// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef SEDONA_METALSPATIAL_C_H
#define SEDONA_METALSPATIAL_C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int SedonaMetalIndexCreate(void** out_index);
int SedonaMetalIndexPushBuild(void* index, const float* rects, uint32_t count);
int SedonaMetalIndexFinish(void* index);
int SedonaMetalIndexProbe(void* index, const float* rects, uint32_t count,
                          uint32_t** out_build, uint32_t** out_probe, uint32_t* out_len);
void SedonaMetalIndexFreeResults(uint32_t* out_build, uint32_t* out_probe);
void SedonaMetalIndexFree(void* index);
int SedonaMetalIndexClear(void* index);
const char* SedonaMetalIndexGetLastError(void* index);
uint64_t SedonaMetalIndexGetMemUsage(void* index);

// Metal Spatial Refiner C-ABI
int SedonaMetalRefinerCreate(void** out_refiner);
#ifdef ENABLE_TEST_INTERNALS
int SedonaMetalRefinerCreateWithMode(void** out_refiner, int bound_mode);
#endif
int SedonaMetalRefinerPushPolygons(void* refiner, const void* polys, uint32_t poly_count,
                                   const void* parts, uint32_t part_count,
                                   const void* rings, uint32_t ring_count,
                                   const void* vertices, uint32_t vertex_count);
int SedonaMetalRefinerFinish(void* refiner);
int SedonaMetalRefinerRefine(void* refiner, const void* points, uint32_t point_count,
                             const uint32_t* candidate_build_indices,
                             const uint32_t* candidate_probe_indices,
                             uint32_t candidate_count, uint8_t* out_states);
#ifdef ENABLE_TEST_INTERNALS
// PROTOTYPE: exact second-stage resolver (measurement only)
int SedonaMetalRefinerFinishExact(void* refiner, const void* vertices, uint32_t vertex_count,
                                  const uint32_t* poly_exact_ok, uint32_t poly_count);
int SedonaMetalRefinerRefineExact(void* refiner, const void* points, uint32_t point_count,
                                  const uint32_t* candidate_build_indices,
                                  const uint32_t* candidate_probe_indices,
                                  uint32_t candidate_count, uint8_t* out_states);
uint64_t SedonaMetalRefinerGetExactMemUsage(void* refiner);
#endif
int SedonaMetalRefinerClear(void* refiner);
void SedonaMetalRefinerFree(void* refiner);
const char* SedonaMetalRefinerGetLastError(void* refiner);
const char* SedonaMetalRefinerGetDeviceName(void* refiner);
uint64_t SedonaMetalRefinerGetMemUsage(void* refiner);

#ifdef __cplusplus
}
#endif

#endif  // SEDONA_METALSPATIAL_C_H
