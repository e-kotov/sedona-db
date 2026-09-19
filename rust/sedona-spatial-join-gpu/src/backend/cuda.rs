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

//! Pure delegation wrapper around CUDA sedona-libgpuspatial.

use super::RefineOutcome;
use crate::options::GpuOptions;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use sedona_libgpuspatial::{
    GpuSpatialIndex as RawGpuSpatialIndex, GpuSpatialOptions,
    GpuSpatialRefiner as RawGpuSpatialRefiner, GpuSpatialRelationPredicate,
};
use sedona_spatial_join::spatial_predicate::SpatialRelationType;
use sedona_spatial_join::SpatialPredicate;

pub struct PlatformSpatialIndex {
    raw: RawGpuSpatialIndex,
}

impl PlatformSpatialIndex {
    pub fn try_new(options: &GpuOptions) -> Result<Self> {
        let gpu_libspatial_options = GpuSpatialOptions {
            cuda_use_memory_pool: options.use_memory_pool,
            cuda_memory_pool_init_percent: options.memory_pool_init_percentage as i32,
            concurrency: 1,
            device_id: options.device_id as i32,
            compress_bvh: options.compress_bvh,
            pipeline_batches: options.pipeline_batches as u32,
        };
        let raw = RawGpuSpatialIndex::try_new(&gpu_libspatial_options)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self { raw })
    }

    pub fn try_new_with_concurrency(options: &GpuOptions, concurrency: u32) -> Result<Self> {
        let gpu_libspatial_options = GpuSpatialOptions {
            cuda_use_memory_pool: options.use_memory_pool,
            cuda_memory_pool_init_percent: options.memory_pool_init_percentage as i32,
            concurrency,
            device_id: options.device_id as i32,
            compress_bvh: options.compress_bvh,
            pipeline_batches: options.pipeline_batches as u32,
        };
        let raw = RawGpuSpatialIndex::try_new(&gpu_libspatial_options)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self { raw })
    }

    pub fn is_available() -> bool {
        RawGpuSpatialIndex::try_new(&GpuSpatialOptions {
            cuda_use_memory_pool: false,
            cuda_memory_pool_init_percent: 1,
            concurrency: 1,
            device_id: 0,
            compress_bvh: false,
            pipeline_batches: 1,
        })
        .is_ok()
    }

    pub fn push_build(&mut self, rects: &[[f32; 4]]) -> Result<()> {
        self.raw.push_build(rects).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to push rectangles to GPU spatial index: {e:?}"
            ))
        })
    }

    pub fn finish_building(&mut self) -> Result<()> {
        self.raw.finish_building().map_err(|e| {
            DataFusionError::Execution(format!("Failed to build spatial index on GPU: {e:?}"))
        })
    }

    pub fn probe(&self, rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>)> {
        self.raw
            .probe(rects)
            .map_err(|e| DataFusionError::Execution(format!("CUDA spatial query failed: {e:?}")))
    }

    pub fn get_index_mem_usage(&self) -> usize {
        0
    }
}

pub struct PlatformSpatialRefiner {
    raw: RawGpuSpatialRefiner,
}

impl PlatformSpatialRefiner {
    pub fn try_new(options: &GpuOptions) -> Result<Self> {
        let gpu_libspatial_options = GpuSpatialOptions {
            cuda_use_memory_pool: options.use_memory_pool,
            cuda_memory_pool_init_percent: options.memory_pool_init_percentage as i32,
            concurrency: 1,
            device_id: options.device_id as i32,
            compress_bvh: options.compress_bvh,
            pipeline_batches: options.pipeline_batches as u32,
        };
        let raw = RawGpuSpatialRefiner::try_new(&gpu_libspatial_options)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self { raw })
    }

    pub fn try_new_with_concurrency(options: &GpuOptions, concurrency: u32) -> Result<Self> {
        let gpu_libspatial_options = GpuSpatialOptions {
            cuda_use_memory_pool: options.use_memory_pool,
            cuda_memory_pool_init_percent: options.memory_pool_init_percentage as i32,
            concurrency,
            device_id: options.device_id as i32,
            compress_bvh: options.compress_bvh,
            pipeline_batches: options.pipeline_batches as u32,
        };
        let raw = RawGpuSpatialRefiner::try_new(&gpu_libspatial_options)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Self { raw })
    }

    pub fn init_build_schema(&mut self, data_type: &DataType) -> Result<()> {
        self.raw.init_build_schema(data_type).map_err(|e| {
            DataFusionError::Execution(format!("Failed to init schema for refiner: {e:?}"))
        })
    }

    pub fn push_build(&mut self, array: &ArrayRef) -> Result<()> {
        self.raw.push_build(array).map_err(|e| {
            DataFusionError::Execution(format!("Failed to add geometries to GPU refiner: {e:?}"))
        })
    }

    pub fn finish_building(&mut self) -> Result<()> {
        self.raw.finish_building().map_err(|e| {
            DataFusionError::Execution(format!("Failed to build spatial refiner on GPU: {e:?}"))
        })
    }

    pub fn refine(
        &self,
        probe_geoms: &ArrayRef,
        predicate: &SpatialPredicate,
        candidate_build: &[u32],
        candidate_probe: &[u32],
    ) -> Result<RefineOutcome> {
        match predicate {
            SpatialPredicate::Relation(rel_p) => {
                let mut build_indices = candidate_build.to_vec();
                let mut probe_indices = candidate_probe.to_vec();
                self.raw
                    .refine(
                        probe_geoms,
                        Self::convert_relation_type(&rel_p.relation_type)?,
                        &mut build_indices,
                        &mut probe_indices,
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "GPU spatial refinement failed: {:?}",
                            e
                        ))
                    })?;
                Ok(RefineOutcome {
                    verified_build: build_indices,
                    verified_probe: probe_indices,
                    uncertain_build: Vec::new(),
                    uncertain_probe: Vec::new(),
                })
            }
            _ => Err(DataFusionError::NotImplemented(
                "Only Relation predicate is supported for GPU spatial query".to_string(),
            )),
        }
    }

    pub fn supports_predicate(relation: &SpatialRelationType) -> bool {
        matches!(
            relation,
            SpatialRelationType::Intersects
                | SpatialRelationType::Contains
                | SpatialRelationType::Within
                | SpatialRelationType::Covers
                | SpatialRelationType::CoveredBy
                | SpatialRelationType::Touches
                | SpatialRelationType::Equals
        )
    }

    pub fn get_refiner_mem_usage(&self) -> usize {
        0
    }

    fn convert_relation_type(t: &SpatialRelationType) -> Result<GpuSpatialRelationPredicate> {
        match t {
            SpatialRelationType::Equals => Ok(GpuSpatialRelationPredicate::Equals),
            SpatialRelationType::Touches => Ok(GpuSpatialRelationPredicate::Touches),
            SpatialRelationType::Contains => Ok(GpuSpatialRelationPredicate::Contains),
            SpatialRelationType::Covers => Ok(GpuSpatialRelationPredicate::Covers),
            SpatialRelationType::Intersects => Ok(GpuSpatialRelationPredicate::Intersects),
            SpatialRelationType::Within => Ok(GpuSpatialRelationPredicate::Within),
            SpatialRelationType::CoveredBy => Ok(GpuSpatialRelationPredicate::CoveredBy),
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported spatial relation type for GPU: {:?}",
                t
            ))),
        }
    }
}
