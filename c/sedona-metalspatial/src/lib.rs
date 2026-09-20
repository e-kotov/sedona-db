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

pub mod flattener;
pub mod refine;

pub use refine::{ContainerSide, MetalSpatialRefiner};

#[cfg(target_os = "macos")]
pub mod ffi {
    use std::ffi::c_void;

    unsafe extern "C" {
        pub fn SedonaMetalIndexCreate(out_index: *mut *mut c_void) -> i32;
        pub fn SedonaMetalIndexPushBuild(index: *mut c_void, rects: *const f32, count: u32) -> i32;
        pub fn SedonaMetalIndexFinish(index: *mut c_void) -> i32;
        pub fn SedonaMetalIndexProbe(
            index: *mut c_void,
            rects: *const f32,
            count: u32,
            out_build: *mut *mut u32,
            out_probe: *mut *mut u32,
            out_len: *mut u32,
        ) -> i32;
        pub fn SedonaMetalIndexFreeResults(out_build: *mut u32, out_probe: *mut u32);
        pub fn SedonaMetalIndexFree(index: *mut c_void);
        pub fn SedonaMetalIndexClear(index: *mut c_void) -> i32;
        pub fn SedonaMetalIndexGetLastError(index: *mut c_void) -> *const std::ffi::c_char;
        pub fn SedonaMetalIndexGetMemUsage(index: *mut c_void) -> u64;

        // Refiner FFI
        pub fn SedonaMetalRefinerCreate(out_refiner: *mut *mut c_void) -> i32;
        #[cfg(feature = "test-internals")]
        pub fn SedonaMetalRefinerCreateWithMode(
            out_refiner: *mut *mut c_void,
            bound_mode: i32,
        ) -> i32;
        pub fn SedonaMetalRefinerPushPolygons(
            refiner: *mut c_void,
            polys: *const c_void,
            poly_count: u32,
            parts: *const c_void,
            part_count: u32,
            rings: *const c_void,
            ring_count: u32,
            vertices: *const c_void,
            vertex_count: u32,
        ) -> i32;
        pub fn SedonaMetalRefinerFinish(refiner: *mut c_void) -> i32;
        pub fn SedonaMetalRefinerRefine(
            refiner: *mut c_void,
            points: *const c_void,
            point_count: u32,
            candidate_build_indices: *const u32,
            candidate_probe_indices: *const u32,
            candidate_count: u32,
            out_states: *mut u8,
        ) -> i32;
        // PROTOTYPE: exact second-stage resolver (test-internals only)
        #[cfg(feature = "test-internals")]
        pub fn SedonaMetalRefinerFinishExact(
            refiner: *mut c_void,
            vertices: *const c_void,
            vertex_count: u32,
            poly_exact_ok: *const u32,
            poly_count: u32,
        ) -> i32;
        #[cfg(feature = "test-internals")]
        pub fn SedonaMetalRefinerRefineExact(
            refiner: *mut c_void,
            points: *const c_void,
            point_count: u32,
            candidate_build_indices: *const u32,
            candidate_probe_indices: *const u32,
            candidate_count: u32,
            out_states: *mut u8,
        ) -> i32;
        #[cfg(feature = "test-internals")]
        pub fn SedonaMetalRefinerGetExactMemUsage(refiner: *mut c_void) -> u64;

        pub fn SedonaMetalRefinerClear(refiner: *mut c_void) -> i32;
        pub fn SedonaMetalRefinerFree(refiner: *mut c_void);
        pub fn SedonaMetalRefinerGetLastError(refiner: *mut c_void) -> *const std::ffi::c_char;
        pub fn SedonaMetalRefinerGetDeviceName(refiner: *mut c_void) -> *const std::ffi::c_char;
        pub fn SedonaMetalRefinerGetMemUsage(refiner: *mut c_void) -> u64;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetalSpatialError {
    #[error("Metal spatial engine is only supported on macOS")]
    PlatformNotSupported,
    #[error("Failed to create Metal spatial index: {0}")]
    CreationFailed(String),
    #[error("Failed to push build rectangles: code {code}: {msg}")]
    PushBuildFailed { code: i32, msg: String },
    #[error("Failed to finish building index: code {code}: {msg}")]
    FinishFailed { code: i32, msg: String },
    #[error("Failed to clear index: code {code}: {msg}")]
    ClearFailed { code: i32, msg: String },
    #[error("Probe failed: code {code}: {msg}")]
    ProbeFailed { code: i32, msg: String },
    #[error("Refiner execution failed: code {code}: {msg}")]
    RefinerExecutionFailed { code: i32, msg: String },
    #[error("Null pointer or invalid state: {0}")]
    InvalidState(String),
}

pub struct MetalSpatialIndex {
    #[cfg(target_os = "macos")]
    raw: *mut std::ffi::c_void,
}

#[cfg(target_os = "macos")]
impl MetalSpatialIndex {
    pub fn try_new() -> Result<Self, MetalSpatialError> {
        let mut raw = std::ptr::null_mut();
        let rc = unsafe { ffi::SedonaMetalIndexCreate(&mut raw) };
        if rc != 0 || raw.is_null() {
            let msg = if !raw.is_null() {
                let err_msg = unsafe {
                    let ptr = ffi::SedonaMetalIndexGetLastError(raw);
                    if ptr.is_null() {
                        "Initialization failed".to_string()
                    } else {
                        std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
                    }
                };
                unsafe { ffi::SedonaMetalIndexFree(raw) };
                err_msg
            } else {
                "Failed to allocate Metal index".to_string()
            };
            return Err(MetalSpatialError::CreationFailed(msg));
        }
        Ok(Self { raw })
    }

    /// Checks whether Metal hardware and pipeline state objects are available.
    pub fn is_available() -> bool {
        Self::try_new().is_ok()
    }

    pub fn last_error(&self) -> String {
        if self.raw.is_null() {
            return "Null index pointer".to_string();
        }
        unsafe {
            let ptr = ffi::SedonaMetalIndexGetLastError(self.raw);
            if ptr.is_null() {
                "Unknown error".to_string()
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        }
    }

    pub fn push_build(&mut self, rects: &[[f32; 4]]) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState("Index is null".to_string()));
        }
        if rects.is_empty() {
            return Ok(());
        }
        let rc = unsafe {
            ffi::SedonaMetalIndexPushBuild(
                self.raw,
                rects.as_ptr() as *const f32,
                rects.len() as u32,
            )
        };
        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::PushBuildFailed { code: rc, msg });
        }
        Ok(())
    }

    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState("Index is null".to_string()));
        }
        let rc = unsafe { ffi::SedonaMetalIndexFinish(self.raw) };
        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::FinishFailed { code: rc, msg });
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState("Index is null".to_string()));
        }
        let rc = unsafe { ffi::SedonaMetalIndexClear(self.raw) };
        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::ClearFailed { code: rc, msg });
        }
        Ok(())
    }

    pub fn probe(&self, rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState("Index is null".to_string()));
        }
        if rects.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut out_build: *mut u32 = std::ptr::null_mut();
        let mut out_probe: *mut u32 = std::ptr::null_mut();
        let mut out_len: u32 = 0;

        let rc = unsafe {
            ffi::SedonaMetalIndexProbe(
                self.raw,
                rects.as_ptr() as *const f32,
                rects.len() as u32,
                &mut out_build,
                &mut out_probe,
                &mut out_len,
            )
        };

        if rc != 0 {
            let msg = self.last_error();
            return Err(MetalSpatialError::ProbeFailed { code: rc, msg });
        }

        let (build_vec, probe_vec) = if out_len > 0 {
            if out_build.is_null() || out_probe.is_null() {
                unsafe { ffi::SedonaMetalIndexFreeResults(out_build, out_probe) };
                return Err(MetalSpatialError::InvalidState(
                    "Probe returned null buffers with non-zero count".to_string(),
                ));
            }
            let b_slice = unsafe { std::slice::from_raw_parts(out_build, out_len as usize) };
            let p_slice = unsafe { std::slice::from_raw_parts(out_probe, out_len as usize) };
            let b = b_slice.to_vec();
            let p = p_slice.to_vec();
            unsafe { ffi::SedonaMetalIndexFreeResults(out_build, out_probe) };
            (b, p)
        } else {
            unsafe { ffi::SedonaMetalIndexFreeResults(out_build, out_probe) };
            (Vec::new(), Vec::new())
        };

        Ok((build_vec, probe_vec))
    }

    /// Returns the number of bytes allocated for spatial index buffers on the GPU.
    pub fn get_memory_usage(&self) -> usize {
        if self.raw.is_null() {
            return 0;
        }
        unsafe { ffi::SedonaMetalIndexGetMemUsage(self.raw) as usize }
    }
}

#[cfg(target_os = "macos")]
impl Drop for MetalSpatialIndex {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::SedonaMetalIndexFree(self.raw);
            }
            self.raw = std::ptr::null_mut();
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl MetalSpatialIndex {
    pub fn is_available() -> bool {
        false
    }

    pub fn try_new() -> Result<Self, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn push_build(&mut self, _rects: &[[f32; 4]]) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn clear(&mut self) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn last_error(&self) -> String {
        "Metal is not supported on this platform".to_string()
    }

    pub fn probe(&self, _rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn get_memory_usage(&self) -> usize {
        0
    }
}

unsafe impl Send for MetalSpatialIndex {}
unsafe impl Sync for MetalSpatialIndex {}
