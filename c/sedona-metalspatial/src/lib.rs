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
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetalSpatialError {
    #[error("Metal spatial engine is only supported on macOS")]
    PlatformNotSupported,
    #[error("Failed to create Metal spatial index")]
    CreationFailed,
    #[error("Failed to push build rectangles: code {0}")]
    PushBuildFailed(i32),
    #[error("Failed to finish building index: code {0}")]
    FinishFailed(i32),
    #[error("Probe failed: code {0}")]
    ProbeFailed(i32),
    #[error("Null pointer or invalid state")]
    InvalidState,
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
            return Err(MetalSpatialError::CreationFailed);
        }
        Ok(Self { raw })
    }

    pub fn push_build(&mut self, rects: &[[f32; 4]]) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState);
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
            return Err(MetalSpatialError::PushBuildFailed(rc));
        }
        Ok(())
    }

    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState);
        }
        let rc = unsafe { ffi::SedonaMetalIndexFinish(self.raw) };
        if rc != 0 {
            return Err(MetalSpatialError::FinishFailed(rc));
        }
        Ok(())
    }

    pub fn probe(&self, rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>), MetalSpatialError> {
        if self.raw.is_null() {
            return Err(MetalSpatialError::InvalidState);
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
            return Err(MetalSpatialError::ProbeFailed(rc));
        }

        let (build_vec, probe_vec) = if out_len > 0 {
            if out_build.is_null() || out_probe.is_null() {
                unsafe { ffi::SedonaMetalIndexFreeResults(out_build, out_probe) };
                return Err(MetalSpatialError::InvalidState);
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
    pub fn try_new() -> Result<Self, MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn push_build(&mut self, _rects: &[[f32; 4]]) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn finish_building(&mut self) -> Result<(), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }

    pub fn probe(&self, _rects: &[[f32; 4]]) -> Result<(Vec<u32>, Vec<u32>), MetalSpatialError> {
        Err(MetalSpatialError::PlatformNotSupported)
    }
}

unsafe impl Send for MetalSpatialIndex {}
unsafe impl Sync for MetalSpatialIndex {}
