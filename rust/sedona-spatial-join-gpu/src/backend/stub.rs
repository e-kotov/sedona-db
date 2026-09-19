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

//! Stub backend when neither CUDA nor Metal is active.

use super::RefineOutcome;
use crate::options::GpuOptions;
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use sedona_spatial_join::SpatialPredicate;

pub struct PlatformSpatialIndex;

impl PlatformSpatialIndex {
    pub fn try_new(_options: &GpuOptions) -> Result<Self> {
        Err(DataFusionError::NotImplemented(
            "GPU spatial acceleration is not compiled in this build (neither 'gpu' nor 'metal' feature enabled)".to_string(),
        ))
    }

    pub fn try_new_with_concurrency(_options: &GpuOptions, _concurrency: u32) -> Result<Self> {
        Self::try_new(_options)
    }

    pub fn is_available() -> bool {
        false
    }

    pub fn push_build(&mut self, _rects: &[[f32; 4]]) -> Result<()> {
        Err(DataFusionError::NotImplemented("GPU spatial index stub".to_string()))
    }

    pub fn finish_building(&mut self) -> Result<()> {
        Err(DataFusionError::NotImplemented("GPU spatial index stub".to_string()))
    }

    pub fn probe(&self, _rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>)> {
        Err(DataFusionError::NotImplemented("GPU spatial index stub".to_string()))
    }
}

pub struct PlatformSpatialRefiner;

impl PlatformSpatialRefiner {
    pub fn try_new(_options: &GpuOptions) -> Result<Self> {
        Err(DataFusionError::NotImplemented(
            "GPU spatial acceleration is not compiled in this build".to_string(),
        ))
    }

    pub fn try_new_with_concurrency(_options: &GpuOptions, _concurrency: u32) -> Result<Self> {
        Self::try_new(_options)
    }

    pub fn init_build_schema(&mut self, _data_type: &DataType) -> Result<()> {
        Err(DataFusionError::NotImplemented("GPU spatial refiner stub".to_string()))
    }

    pub fn push_build(&mut self, _array: &ArrayRef) -> Result<()> {
        Err(DataFusionError::NotImplemented("GPU spatial refiner stub".to_string()))
    }

    pub fn finish_building(&mut self) -> Result<()> {
        Err(DataFusionError::NotImplemented("GPU spatial refiner stub".to_string()))
    }

    pub fn refine(
        &self,
        _probe_geoms: &ArrayRef,
        _predicate: &SpatialPredicate,
        _candidate_build: &[u32],
        _candidate_probe: &[u32],
    ) -> Result<RefineOutcome> {
        Err(DataFusionError::NotImplemented("GPU spatial refiner stub".to_string()))
    }

    pub fn supports_predicate(_predicate_name: &str) -> bool {
        false
    }

    pub fn get_refiner_mem_usage(&self) -> usize {
        0
    }
}
