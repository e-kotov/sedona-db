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
use arrow_array::ArrayRef;

#[cfg(target_os = "macos")]
use crate::ffi;
#[cfg(target_os = "macos")]
use crate::flattener::{
    STATE_INSIDE, STATE_OUTSIDE, STATE_UNCERTAIN, flatten_build_polygons, flatten_probe_points,
};
#[cfg(target_os = "macos")]
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

/// Configuration of the ray-traced edge index the refine kernel uses for large rings.
/// Layout must match `SedonaMetalRtConfig` in `sedona_metalspatial_c.h`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtConfig {
    /// 0: every ring takes the linear scan (the pre-index kernel).
    pub enabled: u32,
    /// Rings with fewer vertices take the linear scan.
    pub min_ring_vertices: u32,
    /// Consecutive ring segments grouped per bounding box.
    pub segs_per_box: u32,
    /// Test only: first z slot, used to exercise the slot limit guard.
    pub slot_base: u32,
    /// Compile the kernel with diagnostic counters.
    pub collect_stats: u32,
}

/// Build facts and (optional) kernel counters of the ray-traced edge index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RtInfo {
    pub indexed_rings: u64,
    pub boxes: u64,
    pub accel_bytes: u64,
    pub scratch_bytes: u64,
    pub gpu_build_micros: u64,
    pub host_prep_micros: u64,
    pub rings_skipped_slot_limit: u64,
    pub rings_skipped_numeric: u64,
    /// Kernel counters below are zero unless `RtConfig::collect_stats` is set.
    pub ring_evals_x: u64,
    pub ring_evals_y: u64,
    pub box_reports: u64,
    pub duplicate_reports: u64,
    pub foreign_reports: u64,
    pub eta_fallbacks: u64,
    pub edges_visited: u64,
    pub pairs_with_rt: u64,
}

pub struct MetalSpatialRefiner {
    #[cfg(target_os = "macos")]
    raw: *mut c_void,
    #[cfg(target_os = "macos")]
    device_name: String,
    #[cfg(target_os = "macos")]
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

    /// Internal/test constructor that also pins the ray-traced edge index configuration
    /// (`enabled: 0` gives the linear-scan kernel used as the reference in parity gates).
    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn try_new_with_rt_config(
        bound_mode: i32,
        rt_config: RtConfig,
    ) -> Result<Self, MetalSpatialError> {
        let mut raw = std::ptr::null_mut();
        let rc =
            unsafe { ffi::SedonaMetalRefinerCreateWithRtConfig(&mut raw, bound_mode, &rt_config) };
        if rc != 0 || raw.is_null() {
            let msg = unsafe {
                let ptr = ffi::SedonaMetalRefinerGetLastError(raw);
                if ptr.is_null() {
                    "Refiner initialization failed".to_string()
                } else {
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                }
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

    /// Build facts and kernel counters of the ray-traced edge index (all zero when no ring
    /// is indexed).
    pub fn rt_info(&self) -> RtInfo {
        let mut v = [0u64; 16];
        if self.raw.is_null()
            || unsafe { ffi::SedonaMetalRefinerGetRtInfo(self.raw, v.as_mut_ptr()) } != 0
        {
            return RtInfo::default();
        }
        RtInfo {
            indexed_rings: v[0],
            boxes: v[1],
            accel_bytes: v[2],
            scratch_bytes: v[3],
            gpu_build_micros: v[4],
            host_prep_micros: v[5],
            rings_skipped_slot_limit: v[6],
            rings_skipped_numeric: v[7],
            ring_evals_x: v[8],
            ring_evals_y: v[9],
            box_reports: v[10],
            duplicate_reports: v[11],
            foreign_reports: v[12],
            eta_fallbacks: v[13],
            edges_visited: v[14],
            pairs_with_rt: v[15],
        }
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
            "Contains" | "Within" | "Covers" | "CoveredBy" | "Intersects" | "Touches" | "Equals"
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
    #[allow(clippy::too_many_arguments)]
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

        let decisions =
            self.candidate_states(probe, candidate_build_indices, candidate_probe_indices)?;

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

    /// Test hook: raw per-candidate kernel states (no container gating), used by the
    /// linear-scan vs ray-traced parity gates.
    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn refine_states(
        &self,
        probe: &ArrayRef,
        candidate_build_indices: &[u32],
        candidate_probe_indices: &[u32],
    ) -> Result<Vec<u32>, MetalSpatialError> {
        if candidate_build_indices.len() != candidate_probe_indices.len() {
            return Err(MetalSpatialError::InvalidState(
                "Candidate build and probe index slices must have equal length".to_string(),
            ));
        }
        self.candidate_states(probe, candidate_build_indices, candidate_probe_indices)
    }

    /// Per-candidate 3-state decisions in candidate order. Pairs the kernel cannot evaluate
    /// stay `STATE_UNCERTAIN`.
    fn candidate_states(
        &self,
        probe: &ArrayRef,
        candidate_build_indices: &[u32],
        candidate_probe_indices: &[u32],
    ) -> Result<Vec<u32>, MetalSpatialError> {
        let n = candidate_build_indices.len();

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

        Ok(decisions)
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

    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn try_new_with_mode(_bound_mode: i32) -> Result<Self, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn try_new_with_rt_config(
        _bound_mode: i32,
        _rt_config: RtConfig,
    ) -> Result<Self, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    #[cfg(feature = "test-internals")]
    #[doc(hidden)]
    pub fn refine_states(
        &self,
        _probe: &ArrayRef,
        _candidate_build_indices: &[u32],
        _candidate_probe_indices: &[u32],
    ) -> Result<Vec<u32>, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn rt_info(&self) -> RtInfo {
        RtInfo::default()
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
