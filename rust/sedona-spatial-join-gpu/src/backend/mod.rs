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

//! Platform backend abstraction for GPU spatial join (CUDA and Apple Metal).

/// Outcome of geometric refinement on the GPU.
#[derive(Debug, Default, Clone)]
pub struct RefineOutcome {
    /// Pairs verified definitely inside on the GPU
    pub verified_build: Vec<u32>,
    pub verified_probe: Vec<u32>,
    /// Pairs that are ambiguous, on numerical boundary, or require CPU resolution
    pub uncertain_build: Vec<u32>,
    pub uncertain_probe: Vec<u32>,
}

#[cfg(all(target_os = "macos", feature = "metal"))]
mod metal;
#[cfg(all(target_os = "macos", feature = "metal"))]
pub use metal::{PlatformSpatialIndex, PlatformSpatialRefiner};

#[cfg(all(feature = "gpu", not(all(target_os = "macos", feature = "metal"))))]
mod cuda;
#[cfg(all(feature = "gpu", not(all(target_os = "macos", feature = "metal"))))]
pub use cuda::{PlatformSpatialIndex, PlatformSpatialRefiner};

#[cfg(not(any(all(target_os = "macos", feature = "metal"), feature = "gpu")))]
mod stub;
#[cfg(not(any(all(target_os = "macos", feature = "metal"), feature = "gpu")))]
pub use stub::{PlatformSpatialIndex, PlatformSpatialRefiner};
