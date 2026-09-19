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

#ifdef __cplusplus
}
#endif

#endif // SEDONA_METALSPATIAL_C_H
