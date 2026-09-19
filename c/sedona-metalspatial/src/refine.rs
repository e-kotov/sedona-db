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

use crate::MetalSpatialError;
use crate::ffi;
use crate::flattener::{
    STATE_INSIDE, STATE_OUTSIDE, STATE_UNCERTAIN, flatten_build_polygons, flatten_probe_points,
};
use arrow_array::ArrayRef;
use std::ffi::{CStr, c_void};

/// Container side gating for spatial relation containment semantics (R1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerSide {
    /// Build geometry is the container (e.g. Contains(build, probe), Covers(build, probe)).
    Build,
    /// Probe geometry is the container (e.g. Contains(probe, build), Covers(probe, build)).
    Probe,
    /// Symmetric relation (e.g. Intersects(build, probe)).
    Either,
}

pub struct MetalSpatialRefiner {
    #[cfg(target_os = "macos")]
    raw: *mut c_void,
    device_name: String,
    num_build_polygons: usize,
}

#[cfg(target_os = "macos")]
impl MetalSpatialRefiner {
    /// Creates a new MetalSpatialRefiner instance on the default Metal device using certified v2 bound.
    pub fn try_new() -> Result<Self, MetalSpatialError> {
        let mut raw = std::ptr::null_mut();
        let rc = unsafe { ffi::SedonaMetalRefinerCreate(&mut raw) };
        if rc != 0 || raw.is_null() {
            let msg = unsafe {
                let ptr = ffi::SedonaMetalRefinerGetLastError(raw);
                let err_str = if ptr.is_null() {
                    "Refiner initialization failed".to_string()
                } else {
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                };
                if !raw.is_null() {
                    ffi::SedonaMetalRefinerFree(raw);
                }
                err_str
            };
            return Err(MetalSpatialError::CreationFailed(msg));
        }

        let dev_name = unsafe {
            let ptr = ffi::SedonaMetalRefinerGetDeviceName(raw);
            if ptr.is_null() {
                "Unknown Apple Silicon GPU".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };

        Ok(Self {
            raw,
            device_name: dev_name,
            num_build_polygons: 0,
        })
    }

    /// Internal/test constructor specifying bound mode:
    /// 0: certified v2 (default)
    /// 1: flawed legacy v1 det bound only
    /// 2: band off only
    /// 3: naive delta only
    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn try_new_with_mode(bound_mode: i32) -> Result<Self, MetalSpatialError> {
        let mut raw = std::ptr::null_mut();
        let rc = unsafe { ffi::SedonaMetalRefinerCreateWithMode(&mut raw, bound_mode) };
        if rc != 0 || raw.is_null() {
            let msg = unsafe {
                let ptr = ffi::SedonaMetalRefinerGetLastError(raw);
                let err_str = if ptr.is_null() {
                    "Refiner initialization failed".to_string()
                } else {
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                };
                if !raw.is_null() {
                    ffi::SedonaMetalRefinerFree(raw);
                }
                err_str
            };
            return Err(MetalSpatialError::CreationFailed(msg));
        }

        let dev_name = unsafe {
            let ptr = ffi::SedonaMetalRefinerGetDeviceName(raw);
            if ptr.is_null() {
                "Unknown Apple Silicon GPU".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };

        Ok(Self {
            raw,
            device_name: dev_name,
            num_build_polygons: 0,
        })
    }

    /// Returns the name of the Metal device running the refiner.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Returns the last error recorded on this refiner instance.
    pub fn last_error(&self) -> String {
        if self.raw.is_null() {
            return "Null refiner handle".to_string();
        }
        unsafe {
            let ptr = ffi::SedonaMetalRefinerGetLastError(self.raw);
            if ptr.is_null() {
                "Unknown refiner error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        }
    }

    /// Returns the number of bytes allocated for polygon buffers on the GPU.
    pub fn get_memory_usage(&self) -> usize {
        if self.raw.is_null() {
            return 0;
        }
        unsafe { ffi::SedonaMetalRefinerGetMemUsage(self.raw) as usize }
    }

    /// Plan-time check called by physical planner.
    pub fn supports_predicate(predicate_name: &str) -> bool {
        matches!(
            predicate_name,
            "Contains" | "Within" | "Covers" | "CoveredBy" | "Intersects"
        )
    }

    /// Clears accumulated build polygons and resets state.
    pub fn clear(&mut self) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState(
                "Refiner handle is null".to_string(),
            ));
        }
        let rc = unsafe { ffi::SedonaMetalRefinerClear(self.raw) };
        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::ClearFailed { code: rc, msg });
        }
        self.num_build_polygons = 0;
        Ok(())
    }

    /// Parses Arrow array of build geometries, flattens to GPU records, and discards WKB.
    pub fn push_build(&mut self, array: &ArrayRef) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState(
                "Refiner handle is null".to_string(),
            ));
        }
        if array.is_empty() {
            return Ok(());
        }

        let (polys, parts, rings, verts) = flatten_build_polygons(array);

        let rc = unsafe {
            ffi::SedonaMetalRefinerPushPolygons(
                self.raw,
                polys.as_ptr() as *const c_void,
                polys.len() as u32,
                parts.as_ptr() as *const c_void,
                parts.len() as u32,
                rings.as_ptr() as *const c_void,
                rings.len() as u32,
                verts.as_ptr() as *const c_void,
                verts.len() as u32,
            )
        };

        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::PushBuildFailed { code: rc, msg });
        }

        self.num_build_polygons += polys.len();
        Ok(())
    }

    /// Finalizes the build stage, transferring polygon topology to GPU shared memory.
    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState(
                "Refiner handle is null".to_string(),
            ));
        }
        let rc = unsafe { ffi::SedonaMetalRefinerFinish(self.raw) };
        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::FinishFailed { code: rc, msg });
        }
        Ok(())
    }

    /// Evaluates candidate pairs against probe geometries.
    ///
    /// - Pairs certified definitely inside (and container is Build or Either) are written to `out_verified_*`.
    /// - Pairs that are ambiguous, non-point/MultiPoint/empty/NaN, or container is Probe are written to `out_uncertain_*`.
    /// - Pairs definitely outside are rejected.
    pub fn refine(
        &self,
        probe: &ArrayRef,
        container: ContainerSide,
        candidate_build_indices: &[u32],
        candidate_probe_indices: &[u32],
        out_verified_build: &mut Vec<u32>,
        out_verified_probe: &mut Vec<u32>,
        out_uncertain_build: &mut Vec<u32>,
        out_uncertain_probe: &mut Vec<u32>,
    ) -> Result<(), MetalSpatialError> {
        if candidate_build_indices.len() != candidate_probe_indices.len() {
            return Err(MetalSpatialError::InvalidState(
                "Candidate build and probe index slices must have equal length".to_string(),
            ));
        }

        let n = candidate_build_indices.len();
        if n == 0 {
            return Ok(());
        }

        // R1: Predicate argument direction gating:
        // If container is Probe, probe point cannot contain a 2D polygon with non-zero area.
        // All pairs route directly to out_uncertain_* without GPU inside emission.
        if container == ContainerSide::Probe {
            out_uncertain_build.extend_from_slice(candidate_build_indices);
            out_uncertain_probe.extend_from_slice(candidate_probe_indices);
            return Ok(());
        }

        // Flatten probe geometries into DecomposedPoint buffer
        let probe_points = flatten_probe_points(probe);

        // Pre-filter candidate pairs:
        // Empty, NaN, MultiPoint, or non-Point geometries route directly to uncertain.
        let mut decisions = vec![STATE_UNCERTAIN; n];
        let mut candidate_map = Vec::with_capacity(n);
        let mut gpu_build_indices = Vec::with_capacity(n);
        let mut gpu_probe_indices = Vec::with_capacity(n);

        for i in 0..n {
            let b_idx = candidate_build_indices[i];
            let p_idx = candidate_probe_indices[i];

            if (b_idx as usize) >= self.num_build_polygons || (p_idx as usize) >= probe_points.len()
            {
                // Out of range: remains STATE_UNCERTAIN
                continue;
            }

            let pt = &probe_points[p_idx as usize];
            if pt.is_valid == 0 {
                // Invalid point: remains STATE_UNCERTAIN
                continue;
            }

            candidate_map.push(i);
            gpu_build_indices.push(b_idx);
            gpu_probe_indices.push(p_idx);
        }

        if !gpu_build_indices.is_empty() {
            let gpu_count = gpu_build_indices.len() as u32;
            let mut states = vec![0u8; gpu_count as usize];

            let rc = unsafe {
                ffi::SedonaMetalRefinerRefine(
                    self.raw,
                    probe_points.as_ptr() as *const c_void,
                    probe_points.len() as u32,
                    gpu_build_indices.as_ptr(),
                    gpu_probe_indices.as_ptr(),
                    gpu_count,
                    states.as_mut_ptr(),
                )
            };

            if rc != 0 {
                let msg = self.last_error();
                return Err(MetalSpatialError::ProbeFailed { code: rc, msg });
            }

            for (k, &state) in states.iter().enumerate() {
                let orig_i = candidate_map[k];
                decisions[orig_i] = state as u32;
            }
        }

        // Emit verified and uncertain preserving original candidate order
        for i in 0..n {
            let b_idx = candidate_build_indices[i];
            let p_idx = candidate_probe_indices[i];
            match decisions[i] {
                STATE_INSIDE => {
                    out_verified_build.push(b_idx);
                    out_verified_probe.push(p_idx);
                }
                STATE_UNCERTAIN => {
                    out_uncertain_build.push(b_idx);
                    out_uncertain_probe.push(p_idx);
                }
                STATE_OUTSIDE => {
                    // Definitely outside: rejected
                }
                _ => {
                    out_uncertain_build.push(b_idx);
                    out_uncertain_probe.push(p_idx);
                }
            }
        }

        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl Drop for MetalSpatialRefiner {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::SedonaMetalRefinerFree(self.raw);
            }
            self.raw = std::ptr::null_mut();
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl MetalSpatialRefiner {
    pub fn try_new() -> Result<Self, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn device_name(&self) -> &str {
        "None"
    }

    pub fn last_error(&self) -> String {
        "Metal is not supported on non-macOS platforms".to_string()
    }

    pub fn get_memory_usage(&self) -> usize {
        0
    }

    pub fn supports_predicate(_predicate_name: &str) -> bool {
        false
    }

    pub fn clear(&mut self) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn push_build(&mut self, _array: &ArrayRef) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn refine(
        &self,
        _probe: &ArrayRef,
        _container: ContainerSide,
        _candidate_build_indices: &[u32],
        _candidate_probe_indices: &[u32],
        _out_verified_build: &mut Vec<u32>,
        _out_verified_probe: &mut Vec<u32>,
        _out_uncertain_build: &mut Vec<u32>,
        _out_uncertain_probe: &mut Vec<u32>,
    ) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }
}

unsafe impl Send for MetalSpatialRefiner {}
unsafe impl Sync for MetalSpatialRefiner {}
