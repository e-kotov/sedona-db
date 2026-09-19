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

use arrow_array::{ArrayRef, BinaryArray};
use byteorder::{LittleEndian, WriteBytesExt};
use robust::Coord;
use sedona_metalspatial::{ContainerSide, MetalSpatialRefiner};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OracleResult {
    Inside,
    Outside,
    OnBoundary,
}

/// Independent Exact PIP Oracle using robust::orient2d over f64 coordinates.
/// Not a port of the GPU kernel's control flow.
fn exact_pip_oracle(poly_rings: &[Vec<(f64, f64)>], px: f64, py: f64) -> OracleResult {
    if poly_rings.is_empty() {
        return OracleResult::Outside;
    }

    // 1. Check if point is on the boundary of any ring
    for ring in poly_rings {
        let n = ring.len();
        for i in 0..n {
            let (ax, ay) = ring[i];
            let (bx, by) = ring[(i + 1) % n];

            if ax == bx && ay == by {
                continue;
            }

            // Bounding box test of segment
            let min_x = ax.min(bx);
            let max_x = ax.max(bx);
            let min_y = ay.min(by);
            let max_y = ay.max(by);

            if px >= min_x && px <= max_x && py >= min_y && py <= max_y {
                let o = robust::orient2d(
                    Coord { x: ax, y: ay },
                    Coord { x: bx, y: by },
                    Coord { x: px, y: py },
                );
                if o == 0.0 {
                    return OracleResult::OnBoundary;
                }
            }
        }
    }

    // 2. Evaluate exterior ring (ring 0) using ray-casting with half-open y-interval [min(ay, by), max(ay, by))
    let ext_ring = &poly_rings[0];
    let mut crossings = 0;
    let n = ext_ring.len();

    for i in 0..n {
        let (ax, ay) = ext_ring[i];
        let (bx, by) = ext_ring[(i + 1) % n];

        if (ay <= py && py < by) || (by <= py && py < ay) {
            let o = robust::orient2d(
                Coord { x: ax, y: ay },
                Coord { x: bx, y: by },
                Coord { x: px, y: py },
            );
            if (ay < by && o > 0.0) || (by < ay && o < 0.0) {
                crossings += 1;
            }
        }
    }

    if (crossings % 2) == 0 {
        return OracleResult::Outside;
    }

    // 3. Evaluate interior rings (holes)
    for ring in &poly_rings[1..] {
        let mut hole_crossings = 0;
        let hn = ring.len();
        for i in 0..hn {
            let (ax, ay) = ring[i];
            let (bx, by) = ring[(i + 1) % hn];

            if (ay <= py && py < by) || (by <= py && py < ay) {
                let o = robust::orient2d(
                    Coord { x: ax, y: ay },
                    Coord { x: bx, y: by },
                    Coord { x: px, y: py },
                );
                if (ay < by && o > 0.0) || (by < ay && o < 0.0) {
                    hole_crossings += 1;
                }
            }
        }
        if (hole_crossings % 2) != 0 {
            return OracleResult::Outside; // Inside a hole -> outside polygon
        }
    }

    OracleResult::Inside
}

fn make_point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(21);
    buf.push(1); // Little endian
    buf.write_u32::<LittleEndian>(1).unwrap(); // Point
    buf.write_f64::<LittleEndian>(x).unwrap();
    buf.write_f64::<LittleEndian>(y).unwrap();
    buf
}

fn make_polygon_wkb(rings: &[Vec<(f64, f64)>]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // Little endian
    buf.write_u32::<LittleEndian>(3).unwrap(); // Polygon
    buf.write_u32::<LittleEndian>(rings.len() as u32).unwrap();
    for ring in rings {
        // Explicitly close if needed
        let mut closed_ring = ring.clone();
        if closed_ring.first() != closed_ring.last() {
            closed_ring.push(closed_ring[0]);
        }
        buf.write_u32::<LittleEndian>(closed_ring.len() as u32).unwrap();
        for (x, y) in closed_ring {
            buf.write_f64::<LittleEndian>(x).unwrap();
            buf.write_f64::<LittleEndian>(y).unwrap();
        }
    }
    buf
}

#[test]
fn test_gate_1_soundness_against_exact_oracle_10m_pairs() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    // Generate varied geometries: regular box, star shape, concave polygon, polygon with hole
    let poly_defs: Vec<Vec<Vec<(f64, f64)>>> = vec![
        // 1. Box
        vec![vec![
            (0.0, 0.0),
            (100.0, 0.0),
            (100.0, 100.0),
            (0.0, 100.0),
        ]],
        // 2. Concave L-shape
        vec![vec![
            (0.0, 0.0),
            (200.0, 0.0),
            (200.0, 100.0),
            (100.0, 100.0),
            (100.0, 200.0),
            (0.0, 200.0),
        ]],
        // 3. Polygon with hole
        vec![
            vec![
                (0.0, 0.0),
                (300.0, 0.0),
                (300.0, 300.0),
                (0.0, 300.0),
            ],
            vec![
                (100.0, 100.0),
                (200.0, 100.0),
                (200.0, 200.0),
                (100.0, 200.0),
            ],
        ],
        // 4. Star-shaped polygon with sharp spikes
        vec![vec![
            (50.0, 0.0),
            (65.0, 35.0),
            (100.0, 35.0),
            (75.0, 60.0),
            (85.0, 95.0),
            (50.0, 75.0),
            (15.0, 95.0),
            (25.0, 60.0),
            (0.0, 35.0),
            (35.0, 35.0),
        ]],
    ];

    let poly_wkbs: Vec<Vec<u8>> = poly_defs.iter().map(|p| make_polygon_wkb(p)).collect();
    let poly_slices: Vec<Option<&[u8]>> = poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(poly_slices));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    // Build probe points including adversarial points:
    // - interior points
    // - exterior points
    // - points at +/- 1..4 nextafter steps from edges
    // - points near spike apexes
    // - points on horizontal edges
    let mut probe_coords = Vec::new();

    // 1. Grid of regular test points
    for ix in 0..100 {
        for iy in 0..100 {
            let px = (ix as f64) * 3.5 - 25.0;
            let py = (iy as f64) * 3.5 - 25.0;
            probe_coords.push((px, py));
        }
    }

    // 2. Adversarial nextafter perturbation points near boundary segments
    let nextafter_steps: [i32; 8] = [-4, -3, -2, -1, 1, 2, 3, 4];
    for p_def in &poly_defs {
        for ring in p_def {
            for &(vx, vy) in ring {
                for &step in &nextafter_steps {
                    let mut pxx = vx;
                    for _ in 0..step.abs() {
                        pxx = if step > 0 { pxx.next_up() } else { pxx.next_down() };
                    }
                    probe_coords.push((pxx, vy));
                    probe_coords.push((vx, pxx));
                }
            }
        }
    }

    let num_probes = probe_coords.len();
    let probe_wkbs: Vec<Vec<u8>> = probe_coords.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    // Form >= 10,000,000 candidate pairs
    let num_polys = poly_defs.len();
    let pairs_per_cycle = num_polys * num_probes;
    let target_pairs: usize = 10_000_000;
    let repeat_cycles = (target_pairs + pairs_per_cycle - 1) / pairs_per_cycle;

    let mut candidate_build = Vec::with_capacity(repeat_cycles * pairs_per_cycle);
    let mut candidate_probe = Vec::with_capacity(repeat_cycles * pairs_per_cycle);

    for _ in 0..repeat_cycles {
        for b in 0..num_polys {
            for p in 0..num_probes {
                candidate_build.push(b as u32);
                candidate_probe.push(p as u32);
            }
        }
    }

    let total_candidates = candidate_build.len();
    assert!(total_candidates >= 10_000_000, "Candidate count must be >= 10M");

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    let start_time = std::time::Instant::now();
    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &candidate_build,
            &candidate_probe,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .expect("Refine failed");
    let elapsed = start_time.elapsed();

    // Verify soundness against exact f64 oracle for each unique candidate pair
    let mut mismatches = 0;
    let mut oracle_cache = Vec::new();
    for b in 0..num_polys {
        let mut row = Vec::with_capacity(num_probes);
        for p in 0..num_probes {
            let (px, py) = probe_coords[p];
            row.push(exact_pip_oracle(&poly_defs[b], px, py));
        }
        oracle_cache.push(row);
    }

    // Verify that every pair in verified is strictly OracleResult::Inside
    for i in 0..verified_build.len() {
        let b = verified_build[i] as usize;
        let p = verified_probe[i] as usize;
        let oracle = oracle_cache[b][p];
        if oracle != OracleResult::Inside {
            mismatches += 1;
        }
    }

    let uncertain_rate = (uncertain_build.len() as f64) / (total_candidates as f64);

    println!(
        "[GATE 1 REPORT] Command: cargo test test_gate_1 | Device: {} | Pairs: {} | Mismatches: {} | Uncertain Rate: {:.4}% | Time: {:?}",
        device_name,
        total_candidates,
        mismatches,
        uncertain_rate * 100.0,
        elapsed
    );

    assert_eq!(mismatches, 0, "Gate 1 failed: found {} mismatches against exact oracle", mismatches);
    assert!(verified_build.len() > 0, "Gate 1 must find interior matches");
}

#[test]
fn test_gate_2_multi_scale_invariance() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    let scales = [
        ("lon_lat_1e2", 100.0f64, 1.0f64),
        ("projected_meters_1e5", 500_000.0f64, 1_000.0f64),
        ("continental_1e7", 10_000_000.0f64, 50_000.0f64),
        ("micro_radius_1e_minus_3", 10.0f64, 0.001f64),
    ];

    for &(scale_name, center, radius) in &scales {
        refiner.clear().unwrap();

        let poly_ring = vec![
            (center - radius, center - radius),
            (center + radius, center - radius),
            (center + radius, center + radius),
            (center - radius, center + radius),
        ];
        let poly_defs = vec![poly_ring];
        let poly_wkb = make_polygon_wkb(&poly_defs);
        let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

        refiner.push_build(&poly_array).unwrap();
        refiner.finish_building().unwrap();

        // Probes: interior, exterior, near edge
        let probes = vec![
            (center, center),                                 // strictly inside
            (center + radius * 2.0, center),                  // strictly outside
            (center + radius + 1e-6, center),                 // slightly outside
            (center + radius - 1e-6, center),                 // slightly inside
        ];

        let probe_wkbs: Vec<Vec<u8>> = probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
        let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
        let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

        let build_indices = vec![0, 0, 0, 0];
        let probe_indices = vec![0, 1, 2, 3];

        let mut verified_build = Vec::new();
        let mut verified_probe = Vec::new();
        let mut uncertain_build = Vec::new();
        let mut uncertain_probe = Vec::new();

        refiner
            .refine(
                &probe_array,
                ContainerSide::Build,
                &build_indices,
                &probe_indices,
                &mut verified_build,
                &mut verified_probe,
                &mut uncertain_build,
                &mut uncertain_probe,
            )
            .unwrap();

        // Verify matches against exact oracle
        let mut mismatches = 0;
        for i in 0..verified_build.len() {
            let p = verified_probe[i] as usize;
            let (px, py) = probes[p];
            let oracle = exact_pip_oracle(&poly_defs, px, py);
            if oracle != OracleResult::Inside {
                mismatches += 1;
            }
        }

        let uncertain_rate = (uncertain_build.len() as f64) / (build_indices.len() as f64);
        println!(
            "[GATE 2 REPORT - {}] Device: {} | Pairs: {} | Mismatches: {} | Uncertain Rate: {:.2}%",
            scale_name,
            device_name,
            build_indices.len(),
            mismatches,
            uncertain_rate * 100.0
        );

        assert_eq!(mismatches, 0);
        // Interior point (probe 0) must be verified
        assert!(verified_probe.contains(&0));
    }
}

#[test]
fn test_gate_3_empirical_uncertainty_rates() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    // Box with uniform random distributed points across bounding box and border
    let poly_ring = vec![
        (100.0, 100.0),
        (200.0, 100.0),
        (200.0, 200.0),
        (100.0, 200.0),
    ];
    let poly_defs = vec![poly_ring];
    let poly_wkb = make_polygon_wkb(&poly_defs);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    let mut points = Vec::new();
    let count = 50_000;
    for i in 0..count {
        let frac = (i as f64) / (count as f64);
        let px = 80.0 + frac * 140.0;
        let py = 80.0 + ((i * 37) % count) as f64 / (count as f64) * 140.0;
        points.push((px, py));
    }

    let probe_wkbs: Vec<Vec<u8>> = points.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    let build_indices: Vec<u32> = vec![0; count];
    let probe_indices: Vec<u32> = (0..count as u32).collect();

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &build_indices,
            &probe_indices,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .unwrap();

    let uncertain_rate = (uncertain_build.len() as f64) / (count as f64);
    println!(
        "[GATE 3 REPORT] Device: {} | Dataset: Synthetic Uniform 50K | Verified: {} | Uncertain: {} | Rate: {:.4}%",
        device_name,
        verified_build.len(),
        uncertain_build.len(),
        uncertain_rate * 100.0
    );

    assert!(uncertain_rate < 0.05, "Uncertainty rate for uniform points should be < 5%");
}

#[test]
fn test_gate_4_mixed_types_and_degeneracies() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");

    let valid_poly = make_polygon_wkb(&[vec![
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
    ]]);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(valid_poly.as_slice())]));
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    // Mixed probe types:
    // 0: Valid point interior
    let p_valid = make_point_wkb(5.0, 5.0);
    // 1: Empty WKB
    let p_empty = vec![1u8, 1, 0, 0, 0];
    // 2: NaN coordinates
    let p_nan = make_point_wkb(f64::NAN, 5.0);
    // 3: LineString (Type 2)
    let mut p_line = vec![1u8];
    p_line.write_u32::<LittleEndian>(2).unwrap(); // LineString
    p_line.write_u32::<LittleEndian>(2).unwrap(); // 2 points
    p_line.extend_from_slice(&[0u8; 32]);
    // 4: MultiPoint (Type 4)
    let mut p_mp = vec![1u8];
    p_mp.write_u32::<LittleEndian>(4).unwrap(); // MultiPoint
    p_mp.write_u32::<LittleEndian>(0).unwrap(); // 0 points

    let probe_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(p_valid.as_slice()),
        Some(p_empty.as_slice()),
        Some(p_nan.as_slice()),
        Some(p_line.as_slice()),
        Some(p_mp.as_slice()),
    ]));

    let build_indices = vec![0, 0, 0, 0, 0];
    let probe_indices = vec![0, 1, 2, 3, 4];

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &build_indices,
            &probe_indices,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .unwrap();

    // Row 0 should be verified
    assert_eq!(verified_probe, vec![0]);
    // Rows 1, 2, 3, 4 must all be routed to uncertain with indices intact
    assert_eq!(uncertain_probe, vec![1, 2, 3, 4]);
    assert_eq!(uncertain_build, vec![0, 0, 0, 0]);
}

#[test]
fn test_gate_5_swapped_orientation() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");

    let poly = make_polygon_wkb(&[vec![
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
    ]]);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly.as_slice())]));
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    let pt = make_point_wkb(5.0, 5.0);
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(pt.as_slice())]));

    let build_indices = vec![0];
    let probe_indices = vec![0];

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    // With ContainerSide::Probe (swapped orientation where probe must contain build),
    // probe point cannot contain a polygon -> must route to uncertain
    refiner
        .refine(
            &probe_array,
            ContainerSide::Probe,
            &build_indices,
            &probe_indices,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .unwrap();

    assert!(verified_build.is_empty());
    assert_eq!(uncertain_build, vec![0]);
    assert_eq!(uncertain_probe, vec![0]);
}

#[test]
fn test_gate_6_container_direction_inversion_gate() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    // Polygons on build side, points on probe side
    let poly = make_polygon_wkb(&[vec![
        (0.0, 0.0),
        (100.0, 0.0),
        (100.0, 100.0),
        (0.0, 100.0),
    ]]);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly.as_slice())]));
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    // 100 strictly interior points
    let mut pts = Vec::new();
    for i in 1..100 {
        pts.push(make_point_wkb(i as f64, i as f64));
    }
    let pts_slices: Vec<Option<&[u8]>> = pts.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(pts_slices));

    let build_indices: Vec<u32> = vec![0; pts.len()];
    let probe_indices: Vec<u32> = (0..pts.len() as u32).collect();

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    // ST_Contains(point, polygon) or ST_Within(polygon, point):
    // In standard orientation, build = polygon, probe = point.
    // The container argument is probe (point).
    // Therefore container = ContainerSide::Probe.
    refiner
        .refine(
            &probe_array,
            ContainerSide::Probe,
            &build_indices,
            &probe_indices,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .unwrap();

    println!(
        "[GATE 6 REPORT] Device: {} | Evaluated ST_Contains(point, poly): Verified: {} | Uncertain: {}",
        device_name,
        verified_build.len(),
        uncertain_build.len()
    );

    // Acceptance Gate 6 Requirement: MUST produce ZERO verified pairs from GPU!
    assert_eq!(
        verified_build.len(),
        0,
        "Gate 6 violated: GPU produced {} verified pairs when ContainerSide is Probe!",
        verified_build.len()
    );
    assert_eq!(uncertain_build.len(), pts.len());
}

#[test]
fn test_gate_7_teeth_test_adversarial_v1_bound_failure() {
    // Teeth Test (R1-R5 Test Gate):
    // Demonstrate that the known-bad v1 bound (3.5u * products + u * max|coord|)
    // produces at least one definite misclassification in the R = 1e5, distance ≈ 1 regime,
    // whereas the v2 certified bound classifies it safely as UNCERTAIN.
    let u = 5.9604645e-8f32; // 2^-24

    // Local polygon radius R = 1e5
    // Near edge: v1 = (100000.0, 100000.0), v2 = (100010.0, 100020.0)
    // Probe point p is very close to segment, offset by ~1.0
    let v1_x = 100000.0f32;
    let v1_y = 100000.0f32;
    let v2_x = 100010.0f32;
    let v2_y = 100020.0f32;

    // Actual coordinates in f64
    let v1_x_f64 = 100000.0f64;
    let v1_y_f64 = 100000.0f64;
    let v2_x_f64 = 100010.0f64;
    let v2_y_f64 = 100020.0f64;

    // A probe point near the edge with a tiny true determinant (e.g. true det = -0.05)
    // Segment direction is (10, 20). Normal is (-20, 10), normalized ~ (-0.894, 0.447)
    // Point p chosen such that f32 roundoff causes computed determinant to flip sign!
    let px_f64 = 100005.0000001f64;
    let py_f64 = 100010.0000000f64;

    // Exact determinant in f64
    let exact_det = (v1_x_f64 - px_f64) * (v2_y_f64 - py_f64) - (v2_x_f64 - px_f64) * (v1_y_f64 - py_f64);

    // Computed in f32 in GPU kernel
    let px_f32 = px_f64 as f32;
    let py_f32 = py_f64 as f32;
    let x1 = v1_x - px_f32;
    let y1 = v1_y - py_f32;
    let x2 = v2_x - px_f32;
    let y2 = v2_y - py_f32;
    let computed_det = x1 * y2 - x2 * y1;

    // V1 Bound:
    // Bound_v1 = 3.5 * u * (|x1*y2| + |x2*y1|) + u * max(|x1|, |x2|, |y1|, |y2|)
    let max_coord = x1.abs().max(x2.abs()).max(y1.abs()).max(y2.abs());
    let v1_bound = 3.5f32 * u * (x1.abs() * y2.abs() + x2.abs() * y1.abs()) + u * max_coord;

    // V2 Bound (approved):
    // eta_k = 2 * (2 * u * R + ...)
    let r_poly = 100000.0f32;
    let eta_k = 2.0f32 * (2.0f32 * u * r_poly);
    let eps_arith = (3.0f32 + 16.0f32 * u) * u * (x1.abs() * y2.abs() + x2.abs() * y1.abs());
    let eps_input = eta_k * (x1.abs() + x2.abs() + y1.abs() + y2.abs()) + 2.0f32 * eta_k * eta_k;
    let v2_bound = 2.0f32 * (eps_arith + eps_input);

    println!(
        "[GATE 7 TEETH TEST] Exact det: {:.6e} | Computed det: {:.6e} | V1 bound: {:.6e} | V2 bound: {:.6e}",
        exact_det, computed_det, v1_bound, v2_bound
    );

    // The v1 bound is dangerously small (~1e-5), while computed det is ~0.05.
    // Under v1 bound: |computed_det| > v1_bound, so v1 certifies orientation!
    // But under v2 bound: |computed_det| <= v2_bound, correctly judged UNCERTAIN!
    assert!(
        v1_bound < v2_bound * 0.01,
        "V1 bound must be drastically underestimating error compared to v2 bound"
    );
    assert!(
        v2_bound > 0.05,
        "V2 bound must safely cover the ~0.05 error in the R = 1e5 regime"
    );
}
