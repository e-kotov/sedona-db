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

use sedona_metalspatial::MetalSpatialIndex;
use std::collections::BTreeSet;

fn cpu_intersects(b: &[f32; 4], p: &[f32; 4]) -> bool {
    !(p[2] < b[0] || p[0] > b[2] || p[3] < b[1] || p[1] > b[3])
}

#[test]
fn test_lifecycle_and_empty() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");
    index.push_build(&[]).expect("Empty push_build should succeed");
    index.finish_building().expect("finish_building should succeed");

    let (build_res, probe_res) = index.probe(&[]).expect("Empty probe should succeed");
    assert!(build_res.is_empty());
    assert!(probe_res.is_empty());

    let dummy_probes = vec![[0.0, 0.0, 1.0, 1.0]];
    let (b_res2, p_res2) = index.probe(&dummy_probes).expect("Probe on empty index should succeed");
    assert!(b_res2.is_empty());
    assert!(p_res2.is_empty());

    // Test clear() lifecycle
    index.push_build(&[[0.0, 0.0, 1.0, 1.0]]).unwrap();
    index.finish_building().unwrap();
    let (b_match, _) = index.probe(&[[0.5, 0.5, 0.5, 0.5]]).unwrap();
    assert_eq!(b_match.len(), 1);

    index.clear().unwrap();
    let (b_cleared, _) = index.probe(&[[0.5, 0.5, 0.5, 0.5]]).unwrap();
    assert!(b_cleared.is_empty());
}

#[test]
fn test_basic_box_intersections() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");

    let build_boxes = vec![
        [0.0, 0.0, 10.0, 10.0],
        [20.0, 20.0, 30.0, 30.0],
    ];
    index.push_build(&build_boxes).expect("push_build failed");
    index.finish_building().expect("finish_building failed");

    let probe_boxes = vec![
        [5.0, 5.0, 6.0, 6.0],       // intersects box 0
        [25.0, 25.0, 26.0, 26.0],   // intersects box 1
        [50.0, 50.0, 60.0, 60.0],   // intersects neither
        [5.0, 5.0, 25.0, 25.0],     // intersects box 0 and box 1
    ];

    let (b_res, p_res) = index.probe(&probe_boxes).expect("probe failed");
    assert_eq!(b_res.len(), p_res.len());

    let mut actual_pairs = BTreeSet::new();
    for (&b, &p) in b_res.iter().zip(p_res.iter()) {
        actual_pairs.insert((b, p));
    }

    let mut expected_pairs = BTreeSet::new();
    expected_pairs.insert((0, 0));
    expected_pairs.insert((1, 1));
    expected_pairs.insert((0, 3));
    expected_pairs.insert((1, 3));

    assert_eq!(actual_pairs, expected_pairs);
}

#[test]
fn test_point_probes() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");

    let build_boxes = vec![
        [0.0, 0.0, 10.0, 10.0],
        [100.0, 100.0, 200.0, 200.0],
    ];
    index.push_build(&build_boxes).expect("push_build failed");
    index.finish_building().expect("finish_building failed");

    let probe_points = vec![
        [5.0, 5.0, 5.0, 5.0],           // in box 0
        [50.0, 50.0, 50.0, 50.0],       // neither
        [150.0, 150.0, 150.0, 150.0],   // in box 1
    ];

    let (b_res, p_res) = index.probe(&probe_points).expect("probe failed");
    assert_eq!(b_res.len(), p_res.len());

    let mut actual_pairs = BTreeSet::new();
    for (&b, &p) in b_res.iter().zip(p_res.iter()) {
        actual_pairs.insert((b, p));
    }

    let mut expected_pairs = BTreeSet::new();
    expected_pairs.insert((0, 0));
    expected_pairs.insert((1, 2));

    assert_eq!(actual_pairs, expected_pairs);
}

#[test]
fn test_chunked_build_and_ground_truth() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");

    // Generate 100 build boxes in 2 chunks
    let mut build_chunk1 = Vec::new();
    let mut build_chunk2 = Vec::new();
    for i in 0..50 {
        let x = (i as f32) * 5.0;
        let y = (i as f32) * 3.0;
        build_chunk1.push([x, y, x + 8.0, y + 8.0]);
    }
    for i in 50..100 {
        let x = (i as f32) * 5.0;
        let y = (i as f32) * 3.0;
        build_chunk2.push([x, y, x + 8.0, y + 8.0]);
    }

    index.push_build(&build_chunk1).expect("push chunk 1 failed");
    index.push_build(&build_chunk2).expect("push chunk 2 failed");
    index.finish_building().expect("finish_building failed");

    // Generate 80 probe boxes
    let mut probe_boxes = Vec::new();
    for j in 0..80 {
        let px = (j as f32) * 4.0;
        let py = (j as f32) * 2.5;
        probe_boxes.push([px, py, px + 6.0, py + 6.0]);
    }

    let (b_res, p_res) = index.probe(&probe_boxes).expect("probe failed");
    assert_eq!(b_res.len(), p_res.len());

    let mut actual_pairs = BTreeSet::new();
    for (&b, &p) in b_res.iter().zip(p_res.iter()) {
        actual_pairs.insert((b, p));
    }

    // Combine build boxes for ground truth
    let mut all_build = build_chunk1;
    all_build.extend(build_chunk2);

    let mut expected_pairs = BTreeSet::new();
    for (p_idx, p) in probe_boxes.iter().enumerate() {
        for (b_idx, b) in all_build.iter().enumerate() {
            if cpu_intersects(b, p) {
                expected_pairs.insert((b_idx as u32, p_idx as u32));
            }
        }
    }

    assert_eq!(actual_pairs, expected_pairs);
    assert!(!expected_pairs.is_empty(), "Should have found matching pairs");
}

#[test]
fn test_concurrent_multi_threaded_probes() {
    use std::sync::Arc;
    use std::thread;

    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");

    // Build index with 100 boxes
    let mut build_boxes = Vec::new();
    for i in 0..100 {
        let x = (i as f32) * 3.0;
        let y = (i as f32) * 2.0;
        build_boxes.push([x, y, x + 10.0, y + 10.0]);
    }
    index.push_build(&build_boxes).expect("push_build failed");
    index.finish_building().expect("finish_building failed");

    let shared_index = Arc::new(index);
    let build_boxes_arc = Arc::new(build_boxes);

    let mut handles = Vec::new();
    let num_threads = 8;
    let probes_per_thread = 25;

    for t in 0..num_threads {
        let idx = Arc::clone(&shared_index);
        let b_boxes = Arc::clone(&build_boxes_arc);

        let handle = thread::spawn(move || {
            for iter in 0..probes_per_thread {
                let offset = (t * probes_per_thread + iter) as f32;
                let mut probes = Vec::new();
                for k in 0..20 {
                    let px = offset + (k as f32) * 2.5;
                    let py = offset * 0.5 + (k as f32) * 1.8;
                    probes.push([px, py, px + 5.0, py + 5.0]);
                }

                let (b_res, p_res) = idx.probe(&probes).expect("concurrent probe failed");
                assert_eq!(b_res.len(), p_res.len());

                let mut actual = BTreeSet::new();
                for (&b, &p) in b_res.iter().zip(p_res.iter()) {
                    actual.insert((b, p));
                }

                let mut expected = BTreeSet::new();
                for (p_i, p) in probes.iter().enumerate() {
                    for (b_i, b) in b_boxes.iter().enumerate() {
                        if cpu_intersects(b, p) {
                            expected.insert((b_i as u32, p_i as u32));
                        }
                    }
                }

                assert_eq!(actual, expected, "Mismatch in thread {t}, iter {iter}");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Worker thread panicked!");
    }
}

#[test]
fn test_nan_inf_handling() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create MetalSpatialIndex");

    let qnan = f32::NAN;
    let pinf = f32::INFINITY;
    let ninf = f32::NEG_INFINITY;

    let build_boxes = vec![
        [0.0, 0.0, 10.0, 10.0],   // 0: valid
        [qnan, 0.0, 10.0, 10.0],  // 1: NaN
        [0.0, qnan, 10.0, 10.0],  // 2: NaN
        [0.0, 0.0, qnan, 10.0],   // 3: NaN
        [0.0, 0.0, 10.0, qnan],   // 4: NaN
        [pinf, 0.0, 10.0, 10.0],  // 5: Inf
        [ninf, 0.0, 10.0, 10.0],  // 6: -Inf
        [0.0, 0.0, pinf, 10.0],   // 7: Inf
        [10.0, 0.0, 5.0, 10.0],   // 8: inverted
        [0.0, 10.0, 10.0, 5.0],   // 9: inverted
        [20.0, 20.0, 30.0, 30.0], // 10: valid
    ];
    index.push_build(&build_boxes).expect("push_build failed");
    index.finish_building().expect("finish_building failed");

    let probe_boxes = vec![
        [5.0, 5.0, 5.0, 5.0],         // 0: in box 0
        [qnan, 5.0, qnan, 5.0],       // 1: NaN probe
        [5.0, pinf, 5.0, pinf],       // 2: Inf probe
        [25.0, 25.0, 25.0, 25.0],     // 3: in box 10
        [15.0, 0.0, 5.0, 0.0],        // 4: inverted probe
    ];

    let (b_res, p_res) = index.probe(&probe_boxes).expect("probe failed");
    assert_eq!(b_res.len(), 2);
    assert_eq!(p_res.len(), 2);

    let mut actual_pairs = BTreeSet::new();
    for (&b, &p) in b_res.iter().zip(p_res.iter()) {
        actual_pairs.insert((b, p));
    }

    let mut expected_pairs = BTreeSet::new();
    expected_pairs.insert((0, 0));
    expected_pairs.insert((10, 3));
    assert_eq!(actual_pairs, expected_pairs);

    // All-NaN build test
    let mut empty_build_index = MetalSpatialIndex::try_new().expect("Failed to create index");
    empty_build_index.push_build(&[[qnan, qnan, qnan, qnan], [pinf, pinf, pinf, pinf]]).unwrap();
    empty_build_index.finish_building().unwrap();
    let (b_res2, p_res2) = empty_build_index.probe(&probe_boxes).unwrap();
    assert!(b_res2.is_empty());
    assert!(p_res2.is_empty());

    // All-NaN probe test
    let (b_res3, p_res3) = index.probe(&[[qnan, qnan, qnan, qnan], [pinf, pinf, pinf, pinf]]).unwrap();
    assert!(b_res3.is_empty());
    assert!(p_res3.is_empty());
}

#[test]
fn test_error_handling_and_last_error() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create index");
    let initial_err = index.last_error();
    assert!(initial_err.is_empty() || !initial_err.contains("Null"));

    // Push valid data
    index.push_build(&[[0.0, 0.0, 1.0, 1.0]]).unwrap();
    index.finish_building().unwrap();

    let (b_res, p_res) = index.probe(&[[0.5, 0.5, 0.5, 0.5]]).unwrap();
    assert_eq!(b_res.len(), 1);
    assert_eq!(p_res.len(), 1);
}

#[test]
fn test_hierarchical_adversarial() {
    let mut index = MetalSpatialIndex::try_new().expect("Failed to create index");

    // 10,000 full-extent build boxes
    let n_build = 10_000;
    let build_boxes = vec![[0.0f32, 0.0, 100.0, 100.0]; n_build];
    index.push_build(&build_boxes).unwrap();
    index.finish_building().unwrap();

    // 5 probe boxes
    let probe_boxes = vec![
        [10.0f32, 10.0, 20.0, 20.0],
        [50.0, 50.0, 60.0, 60.0],
    ];

    let (b_res, p_res) = index.probe(&probe_boxes).unwrap();
    // Every probe intersects all 10,000 boxes -> 20,000 matches total
    assert_eq!(b_res.len(), 20_000);
    assert_eq!(p_res.len(), 20_000);
}



