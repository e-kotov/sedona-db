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

pub(crate) mod backend;
mod index;
mod join_provider;

pub mod options;
pub mod physical_planner;

use std::sync::atomic::{AtomicU64, Ordering};

static TOTAL_GPU_VERIFIED: AtomicU64 = AtomicU64::new(0);

/// Returns the total number of candidate pairs verified directly on GPU since last reset.
pub fn total_gpu_verified() -> u64 {
    TOTAL_GPU_VERIFIED.load(Ordering::Relaxed)
}

/// Resets the total GPU verified pairs counter.
pub fn reset_gpu_verified() {
    TOTAL_GPU_VERIFIED.store(0, Ordering::Relaxed);
}

pub(crate) fn record_gpu_verified(count: usize) {
    if count > 0 {
        TOTAL_GPU_VERIFIED.fetch_add(count as u64, Ordering::Relaxed);
    }
}
