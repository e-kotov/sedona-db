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

//! Metal spatial acceleration backend for Apple Silicon.

use super::RefineOutcome;
use crate::options::GpuOptions;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use sedona_metalspatial::{ContainerSide, MetalSpatialIndex, MetalSpatialRefiner};
use sedona_spatial_join::spatial_predicate::SpatialRelationType;
use sedona_spatial_join::SpatialPredicate;

pub struct PlatformSpatialIndex {
    raw: MetalSpatialIndex,
}

impl PlatformSpatialIndex {
    pub fn try_new(_options: &GpuOptions) -> Result<Self> {
        let raw = MetalSpatialIndex::try_new().map_err(|e| {
            DataFusionError::Execution(format!("Failed to initialize Metal spatial index: {e}"))
        })?;
        Ok(Self { raw })
    }

    pub fn try_new_with_concurrency(_options: &GpuOptions, _concurrency: u32) -> Result<Self> {
        Self::try_new(_options)
    }

    pub fn is_available() -> bool {
        MetalSpatialIndex::is_available()
    }

    pub fn push_build(&mut self, rects: &[[f32; 4]]) -> Result<()> {
        self.raw.push_build(rects).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to push rectangles to Metal spatial index: {e}"
            ))
        })
    }

    pub fn finish_building(&mut self) -> Result<()> {
        self.raw.finish_building().map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to finish building Metal spatial index: {e}"
            ))
        })
    }

    pub fn probe(&self, rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>)> {
        self.raw
            .probe(rects)
            .map_err(|e| DataFusionError::Execution(format!("Metal spatial query failed: {e}")))
    }

    pub fn get_index_mem_usage(&self) -> usize {
        self.raw.get_memory_usage()
    }
}

pub struct PlatformSpatialRefiner {
    raw: MetalSpatialRefiner,
}

impl PlatformSpatialRefiner {
    pub fn try_new(_options: &GpuOptions) -> Result<Self> {
        let raw = MetalSpatialRefiner::try_new().map_err(|e| {
            DataFusionError::Execution(format!("Failed to initialize Metal spatial refiner: {e}"))
        })?;
        Ok(Self { raw })
    }

    pub fn try_new_with_concurrency(_options: &GpuOptions, _concurrency: u32) -> Result<Self> {
        Self::try_new(_options)
    }

    pub fn init_build_schema(&mut self, _data_type: &DataType) -> Result<()> {
        // No-op on Metal: WKB topology is parsed on push_build
        Ok(())
    }

    pub fn push_build(&mut self, array: &ArrayRef) -> Result<()> {
        self.raw.push_build(array).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to push build geometries to Metal refiner: {e}"
            ))
        })
    }

    pub fn finish_building(&mut self) -> Result<()> {
        self.raw.finish_building().map_err(|e| {
            DataFusionError::Execution(format!("Failed to finalize Metal refiner build: {e}"))
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
                let is_rejection_only = matches!(
                    &rel_p.relation_type,
                    SpatialRelationType::Touches | SpatialRelationType::Equals
                );

                let container = match &rel_p.relation_type {
                    SpatialRelationType::Contains | SpatialRelationType::Covers => {
                        ContainerSide::Build
                    }
                    SpatialRelationType::Within | SpatialRelationType::CoveredBy => {
                        ContainerSide::Probe
                    }
                    SpatialRelationType::Intersects
                    | SpatialRelationType::Touches
                    | SpatialRelationType::Equals => ContainerSide::Either,
                    other => {
                        return Err(DataFusionError::Plan(format!(
                            "Spatial relation {:?} is not supported by Metal refiner",
                            other
                        )))
                    }
                };

                let mut verified_build = Vec::new();
                let mut verified_probe = Vec::new();
                let mut uncertain_build = Vec::new();
                let mut uncertain_probe = Vec::new();

                self.raw
                    .refine(
                        probe_geoms,
                        container,
                        candidate_build,
                        candidate_probe,
                        &mut verified_build,
                        &mut verified_probe,
                        &mut uncertain_build,
                        &mut uncertain_probe,
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Metal spatial refinement failed: {e}"))
                    })?;

                // For Touches and Equals, the GPU serves as a pure rejection filter:
                // - A certified Inside point lies strictly in the polygon's interior, so Touches(poly, pt) is false.
                // - Equals(point, polygon) is always false due to dimension mismatch.
                // - Certified Outside pairs are already discarded by the GPU kernel.
                // - Only Uncertain pairs (boundary/near-boundary or unsupported geometries) can possibly
                //   satisfy Touches or Equals; discarding verified pairs ensures zero false positives on GPU
                //   while forwarding all plausible candidates to exact CPU GEOS evaluation.
                if is_rejection_only {
                    verified_build.clear();
                    verified_probe.clear();
                }

                Ok(RefineOutcome {
                    verified_build,
                    verified_probe,
                    uncertain_build,
                    uncertain_probe,
                })
            }
            _ => Err(DataFusionError::NotImplemented(
                "Only Relation predicate is supported for Metal spatial query".to_string(),
            )),
        }
    }

    pub fn supports_predicate(relation: &SpatialRelationType) -> bool {
        matches!(
            relation,
            SpatialRelationType::Contains
                | SpatialRelationType::Covers
                | SpatialRelationType::Within
                | SpatialRelationType::CoveredBy
                | SpatialRelationType::Intersects
                | SpatialRelationType::Touches
                | SpatialRelationType::Equals
        )
    }

    pub fn get_refiner_mem_usage(&self) -> usize {
        self.raw.get_memory_usage()
    }
}
