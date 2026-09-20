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

#![cfg(target_os = "macos")]

use arrow_array::{ArrayRef, BinaryArray};
use byteorder::{BigEndian, ByteOrder, LittleEndian, WriteBytesExt};
use robust::Coord;
use sedona_metalspatial::flattener::{
    EXACT_BOUNDARY, EXACT_INSIDE, EXACT_NOT_DECIDED, EXACT_OUTSIDE, FixedProbe, f64_to_fixed,
    flatten_probe_exact,
};
use sedona_metalspatial::{ContainerSide, MetalSpatialRefiner};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OracleResult {
    Inside,
    Outside,
    OnBoundary,
}

#[derive(Debug, Clone)]
struct PolyPart {
    rings: Vec<Vec<(f64, f64)>>, // ring 0 is exterior, rings 1.. are holes
}

#[derive(Debug, Clone)]
struct MultiPolyDef {
    parts: Vec<PolyPart>,
}

/// Independent Exact PIP Oracle using robust::orient2d over f64 coordinates.
/// Supports single Polygons and MultiPolygons with holes.
/// Not a port of the GPU kernel's control flow.
fn exact_pip_oracle(poly: &MultiPolyDef, px: f64, py: f64) -> OracleResult {
    // 1. Boundary check across every edge of every ring
    for part in &poly.parts {
        for ring in &part.rings {
            let n = ring.len();
            for i in 0..n {
                let (ax, ay) = ring[i];
                let (bx, by) = ring[(i + 1) % n];

                if ax == bx && ay == by {
                    continue;
                }

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
    }

    // 2. MultiPolygon evaluation: inside if inside at least one part and outside all holes
    for part in &poly.parts {
        if part.rings.is_empty() {
            continue;
        }

        // Check exterior ring (ring 0) using half-open ray casting
        let ext_ring = &part.rings[0];
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

        if (crossings % 2) != 0 {
            // Check interior holes
            let mut in_hole = false;
            for hole in &part.rings[1..] {
                let mut hole_crossings = 0;
                let hn = hole.len();
                for i in 0..hn {
                    let (ax, ay) = hole[i];
                    let (bx, by) = hole[(i + 1) % hn];

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
                    in_hole = true;
                    break;
                }
            }

            if !in_hole {
                return OracleResult::Inside;
            }
        }
    }

    OracleResult::Outside
}

fn make_point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(21);
    buf.push(1); // Little endian
    buf.write_u32::<LittleEndian>(1).unwrap(); // Point
    buf.write_f64::<LittleEndian>(x).unwrap();
    buf.write_f64::<LittleEndian>(y).unwrap();
    buf
}

fn make_multipoly_wkb(poly: &MultiPolyDef) -> Vec<u8> {
    if poly.parts.len() == 1 {
        // Single Polygon WKB
        let part = &poly.parts[0];
        let mut buf = Vec::new();
        buf.push(1); // Little endian
        buf.write_u32::<LittleEndian>(3).unwrap(); // Polygon
        buf.write_u32::<LittleEndian>(part.rings.len() as u32)
            .unwrap();
        for ring in &part.rings {
            let mut closed_ring = ring.clone();
            if closed_ring.first() != closed_ring.last() {
                closed_ring.push(closed_ring[0]);
            }
            buf.write_u32::<LittleEndian>(closed_ring.len() as u32)
                .unwrap();
            for (x, y) in closed_ring {
                buf.write_f64::<LittleEndian>(x).unwrap();
                buf.write_f64::<LittleEndian>(y).unwrap();
            }
        }
        buf
    } else {
        // MultiPolygon WKB
        let mut buf = Vec::new();
        buf.push(1); // Little endian
        buf.write_u32::<LittleEndian>(6).unwrap(); // MultiPolygon
        buf.write_u32::<LittleEndian>(poly.parts.len() as u32)
            .unwrap();
        for part in &poly.parts {
            buf.push(1); // Little endian
            buf.write_u32::<LittleEndian>(3).unwrap(); // Polygon
            buf.write_u32::<LittleEndian>(part.rings.len() as u32)
                .unwrap();
            for ring in &part.rings {
                let mut closed_ring = ring.clone();
                if closed_ring.first() != closed_ring.last() {
                    closed_ring.push(closed_ring[0]);
                }
                buf.write_u32::<LittleEndian>(closed_ring.len() as u32)
                    .unwrap();
                for (x, y) in closed_ring {
                    buf.write_f64::<LittleEndian>(x).unwrap();
                    buf.write_f64::<LittleEndian>(y).unwrap();
                }
            }
        }
        buf
    }
}

/// Helper for Gate 4: creates a WKB polygon where the ring is explicitly unclosed
fn make_unclosed_poly_wkb(rings: &[Vec<(f64, f64)>]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // Little endian
    buf.write_u32::<LittleEndian>(3).unwrap(); // Polygon
    buf.write_u32::<LittleEndian>(rings.len() as u32).unwrap();
    for ring in rings {
        // Do NOT close the ring: write vertices without duplicating first vertex
        buf.write_u32::<LittleEndian>(ring.len() as u32).unwrap();
        for &(x, y) in ring {
            buf.write_f64::<LittleEndian>(x).unwrap();
            buf.write_f64::<LittleEndian>(y).unwrap();
        }
    }
    buf
}

/// Helper for Gate 4: creates a degenerate WKB polygon with fewer than 3 vertices
fn make_degenerate_poly_wkb(num_verts: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // Little endian
    buf.write_u32::<LittleEndian>(3).unwrap(); // Polygon
    buf.write_u32::<LittleEndian>(1).unwrap(); // 1 ring
    buf.write_u32::<LittleEndian>(num_verts as u32).unwrap();
    for i in 0..num_verts {
        buf.write_f64::<LittleEndian>(i as f64).unwrap();
        buf.write_f64::<LittleEndian>(i as f64).unwrap();
    }
    buf
}

/// Helper for star polygon generation with n in {8, 64, 512, 2000}
fn generate_random_star_ring(
    cx: f64,
    cy: f64,
    r_poly: f64,
    n: usize,
    rng: &mut SeededRng,
) -> Vec<(f64, f64)> {
    let mut ring = Vec::with_capacity(n);
    for i in 0..n {
        let angle = (i as f64) * std::f64::consts::TAU / (n as f64);
        let rad_factor = if i % 8 == 0 {
            1.5 // spike apex
        } else if i % 2 == 1 {
            0.4
        } else {
            0.6 + 0.4 * rng.next_f64()
        };
        let r = r_poly * rad_factor;
        ring.push((cx + r * angle.cos(), cy + r * angle.sin()));
    }
    ring
}

/// Seeded pseudo-random number generator (LCG)
struct SeededRng {
    state: u64,
}

impl SeededRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_f64(&mut self) -> f64 {
        let v = (self.next_u64() >> 11) as f64;
        v / 9007199254740992.0 // in [0, 1)
    }
}

/// Generates the adversarial polygon and probe suite matching T1/T2 requirements.
/// Only unique pairs are generated.
#[allow(clippy::type_complexity)]
fn generate_adversarial_suite(
    cx: f64,
    cy: f64,
    r_poly: f64,
    seed: u64,
) -> (Vec<MultiPolyDef>, Vec<(f64, f64)>, Vec<(u32, u32)>) {
    let mut rng = SeededRng::new(seed);
    let mut polys = Vec::new();

    // 1. Star-shaped polygons with n in {8, 64, 512, 2000} vertices
    for &n_verts in &[8, 64, 512, 2000] {
        let star_ring = generate_random_star_ring(cx, cy, r_poly, n_verts, &mut rng);
        polys.push(MultiPolyDef {
            parts: vec![PolyPart {
                rings: vec![star_ring],
            }],
        });
    }

    // 2. Polygon with an interior hole
    let ext_box = vec![
        (cx - r_poly, cy - r_poly),
        (cx + r_poly, cy - r_poly),
        (cx + r_poly, cy + r_poly),
        (cx - r_poly, cy + r_poly),
    ];
    let hole_box = vec![
        (cx - r_poly * 0.3, cy - r_poly * 0.3),
        (cx + r_poly * 0.3, cy - r_poly * 0.3),
        (cx + r_poly * 0.3, cy + r_poly * 0.3),
        (cx - r_poly * 0.3, cy + r_poly * 0.3),
    ];
    polys.push(MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![ext_box, hole_box],
        }],
    });

    // 3. MultiPolygon with 2 disjoint parts
    let part1 = PolyPart {
        rings: vec![vec![
            (cx - r_poly * 1.5, cy),
            (cx - r_poly * 0.5, cy),
            (cx - r_poly * 0.5, cy + r_poly),
            (cx - r_poly * 1.5, cy + r_poly),
        ]],
    };
    let part2 = PolyPart {
        rings: vec![vec![
            (cx + r_poly * 0.5, cy),
            (cx + r_poly * 1.5, cy),
            (cx + r_poly * 1.5, cy + r_poly),
            (cx + r_poly * 0.5, cy + r_poly),
        ]],
    };
    polys.push(MultiPolyDef {
        parts: vec![part1, part2],
    });

    // 4. Specialized Adversarial Polygon:
    // Contains:
    // - Counterexample geometry from A: V = (-1, -0.5 eta), W = (1000, 3 eta_k) relative to probe
    // - Sharp spike apex pointing directly along ray y = cy
    // - Near-horizontal grazing edge with slope ~ 1e-6
    // - Mode 1 det bound test edge: crosses ray near x = cx, vertices strictly outside eta-band (|y| = 3 eta_k)
    // - Collinear triples and duplicate vertices
    let eta_k = 4.0 * 5.9604645e-8 * r_poly;
    let adv_ring = vec![
        (cx - 1.0, cy - 0.25 * eta_k),           // Counterexample A: vertex V
        (cx + 1000.0, cy + 3.0 * eta_k),         // Counterexample A: vertex W
        (cx + r_poly * 0.8, cy + 1e-6 * r_poly), // Near-horizontal grazing edge start
        (cx + 1.2 * r_poly, cy),                 // Sharp spike apex on ray y = cy
        (cx + r_poly * 0.8, cy - 1e-6 * r_poly), // Near-horizontal grazing edge end
        (cx + 5.0, cy - 3.0 * eta_k),            // Mode 1 test edge V1 (|y| > eta_k)
        (cx - 5.0, cy + 3.0 * eta_k),            // Mode 1 test edge V2 (|y| > eta_k)
        (cx - r_poly * 0.5, cy + r_poly * 0.5),  // 45° edge
        (cx - r_poly * 0.5, cy + r_poly * 0.5),  // duplicate consecutive vertex
        (cx - r_poly * 0.25, cy + r_poly * 0.5), // collinear triple part 1
        (cx, cy + r_poly * 0.5),                 // collinear triple part 2
        (cx - r_poly * 0.5, cy),                 // vertical edge
    ];
    polys.push(MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![adv_ring],
        }],
    });

    // Collect all probe points across adversarial perturbation scales
    let mut probe_points = vec![
        // (a) Interior center points
        (cx, cy),
        (cx + r_poly * 0.01, cy + r_poly * 0.01),
        // (b) Exterior points
        (cx + r_poly * 3.0, cy + r_poly * 3.0),
        (cx - r_poly * 3.0, cy - r_poly * 3.0),
        // (c) Spike apex probes: ray shoots directly at spike apex (cx + 1.2 * r_poly, cy)
        (cx + 0.5 * r_poly, cy),
        (cx + 1.2 * r_poly, cy), // exactly on apex (boundary)
    ];

    // (d) Mode 1 det crossing probes: points near x = cx on ray y = cy
    for &dx in &[-0.01, -0.001, -0.0001, 0.0, 0.0001, 0.001, 0.01] {
        probe_points.push((cx + dx, cy));
    }

    // Perturbation scale factors required by T1:
    let k_values: [f64; 11] = [
        0.0, -1.0, 1.0, -2.0, 2.0, -4.0, 4.0, -16.0, 16.0, -256.0, 256.0,
    ];

    for poly in &polys {
        for part in &poly.parts {
            for ring in &part.rings {
                let n = ring.len();
                // If ring has many vertices, sample 16 evenly spaced edges
                let step = (n / 16).max(1);
                let sampled_indices: Vec<usize> = (0..n).step_by(step).take(16).collect();

                for &i in &sampled_indices {
                    let (ax, ay) = ring[i];
                    let (bx, by) = ring[(i + 1) % n];

                    let dx = bx - ax;
                    let dy = by - ay;
                    let len = (dx * dx + dy * dy).sqrt();
                    if len == 0.0 {
                        continue;
                    }
                    let nx = -dy / len;
                    let ny = dx / len;

                    // Edge interior points at t in {0.25, 0.5, 0.75}
                    for &t in &[0.25, 0.5, 0.75] {
                        let mx = ax + t * dx;
                        let my = ay + t * dy;
                        let m_coord = mx.abs().max(my.abs()).max(1.0);

                        let s_values = [
                            2.22e-16 * m_coord, // f64 ulp of coordinate
                            1.19e-7 * m_coord,  // f32 ulp of coordinate
                            1.19e-7 * r_poly,   // f32 ulp of R_poly
                            eta_k * 0.25,       // eta_k / 4
                            eta_k,              // eta_k
                            4.0 * eta_k,        // 4 * eta_k
                        ];

                        for &s in &s_values {
                            for &k in &k_values {
                                probe_points.push((mx + k * s * nx, my + k * s * ny));
                            }
                        }
                    }

                    // Vertex offsets around (ax, ay), including apexes (above and below)
                    let v_coord = ax.abs().max(ay.abs()).max(1.0);
                    let s_values_v = [
                        2.22e-16 * v_coord,
                        1.19e-7 * v_coord,
                        1.19e-7 * r_poly,
                        eta_k * 0.25,
                        eta_k,
                        4.0 * eta_k,
                    ];

                    for &s in &s_values_v {
                        for &k in &k_values {
                            probe_points.push((ax, ay + k * s)); // vertical perturbation (straddle apex check)
                            probe_points.push((ax + k * s, ay)); // horizontal perturbation
                        }
                    }
                }
            }
        }
    }

    // Deduplicate probe points to ensure all points are unique
    let mut unique_probes = Vec::with_capacity(probe_points.len());
    let mut seen_probes = HashSet::new();
    for (px, py) in probe_points {
        let key = (px.to_bits(), py.to_bits());
        if seen_probes.insert(key) {
            unique_probes.push((px, py));
        }
    }

    // Build unique candidate pairs
    let num_polys = polys.len();
    let num_probes = unique_probes.len();
    let mut candidate_pairs = Vec::with_capacity(num_polys * num_probes);
    for b in 0..num_polys {
        for p in 0..num_probes {
            candidate_pairs.push((b as u32, p as u32));
        }
    }

    (polys, unique_probes, candidate_pairs)
}

/// Executes full two-sided verification against exact f64 oracle.
/// Asserts:
///   GPU Inside  => oracle Inside  (0 false positives)
///   GPU Outside => oracle Outside (0 false negatives; oracle OnBoundary must never be GPU-definite)
fn verify_two_sided(
    verified: &[(u32, u32)],
    uncertain: &[(u32, u32)],
    all_pairs: &[(u32, u32)],
    polys: &[MultiPolyDef],
    probes: &[(f64, f64)],
) -> (usize, usize, usize, usize) {
    let verified_set: HashSet<(u32, u32)> = verified.iter().copied().collect();
    let uncertain_set: HashSet<(u32, u32)> = uncertain.iter().copied().collect();

    let mut inside_count = 0;
    let mut uncertain_count = 0;
    let mut outside_count = 0;
    let mut mismatches = 0;

    for &(b, p) in all_pairs {
        let (px, py) = probes[p as usize];
        let oracle = exact_pip_oracle(&polys[b as usize], px, py);

        if verified_set.contains(&(b, p)) {
            inside_count += 1;
            // GPU says Inside => oracle MUST be Inside
            if oracle != OracleResult::Inside {
                mismatches += 1;
            }
        } else if uncertain_set.contains(&(b, p)) {
            uncertain_count += 1;
            // Safely routed to CPU uncertain list
        } else {
            outside_count += 1;
            // GPU says Outside => oracle MUST be Outside
            if oracle != OracleResult::Outside {
                mismatches += 1;
            }
        }
    }

    (inside_count, uncertain_count, outside_count, mismatches)
}

#[test]
fn test_gate_1_full_two_sided_soundness() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    let (polys, probes, candidate_pairs) =
        generate_adversarial_suite(500_000.0, 500_000.0, 10_000.0, 42);

    let poly_wkbs: Vec<Vec<u8>> = polys.iter().map(make_multipoly_wkb).collect();
    let poly_slices: Vec<Option<&[u8]>> = poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(poly_slices));

    let probe_wkbs: Vec<Vec<u8>> = probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    let (cand_b, cand_p): (Vec<u32>, Vec<u32>) = candidate_pairs.iter().copied().unzip();

    let mut verified_build = Vec::new();
    let mut verified_probe = Vec::new();
    let mut uncertain_build = Vec::new();
    let mut uncertain_probe = Vec::new();

    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &cand_b,
            &cand_p,
            &mut verified_build,
            &mut verified_probe,
            &mut uncertain_build,
            &mut uncertain_probe,
        )
        .expect("Refine failed");

    let verified_pairs: Vec<(u32, u32)> = verified_build.into_iter().zip(verified_probe).collect();
    let uncertain_pairs: Vec<(u32, u32)> =
        uncertain_build.into_iter().zip(uncertain_probe).collect();

    let (inside, uncertain, outside, mismatches) = verify_two_sided(
        &verified_pairs,
        &uncertain_pairs,
        &candidate_pairs,
        &polys,
        &probes,
    );

    let uncertain_rate = (uncertain as f64) / (candidate_pairs.len() as f64);

    println!(
        "[GATE 1 REPORT] Device: {} | Unique Pairs: {} | Inside: {} | Outside: {} | Uncertain: {} | Uncertain Rate: {:.2}% | Mismatches: {}",
        device_name,
        candidate_pairs.len(),
        inside,
        outside,
        uncertain,
        uncertain_rate * 100.0,
        mismatches
    );

    // Assertion: GPU-Inside => oracle-Inside and GPU-Outside => oracle-Outside over all unique pairs
    assert_eq!(
        mismatches, 0,
        "Gate 1 full two-sided check failed: found {} mismatches",
        mismatches
    );
    assert!(inside > 0, "Gate 1 must find verified inside pairs");
    assert!(outside > 0, "Gate 1 must find verified outside pairs");
}

#[test]
fn test_gate_2_full_scale_matrix() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    let centers = [1e2, 5e5, 1e7, -1e7];
    let radii = [1e-3, 1.0, 1e3, 1e5, 1e6];

    println!(
        "------------------------------------------------------------------------------------------------------"
    );
    println!(
        "GATE 2 MATRIX: (center, R) in {{1e2, 5e5, 1e7, -1e7}} x {{1e-3, 1, 1e3, 1e5, 1e6}} (Two-Sided Check)"
    );
    println!(
        "------------------------------------------------------------------------------------------------------"
    );

    for &cx in &centers {
        for &r in &radii {
            refiner.clear().unwrap();

            let (polys, probes, candidate_pairs) = generate_adversarial_suite(cx, cx, r, 12345);

            let poly_wkbs: Vec<Vec<u8>> = polys.iter().map(make_multipoly_wkb).collect();
            let poly_slices: Vec<Option<&[u8]>> =
                poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
            let poly_array: ArrayRef = Arc::new(BinaryArray::from(poly_slices));

            let probe_wkbs: Vec<Vec<u8>> =
                probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
            let probe_slices: Vec<Option<&[u8]>> =
                probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
            let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

            refiner.push_build(&poly_array).unwrap();
            refiner.finish_building().unwrap();

            let (cand_b, cand_p): (Vec<u32>, Vec<u32>) = candidate_pairs.iter().copied().unzip();

            let mut verified_build = Vec::new();
            let mut verified_probe = Vec::new();
            let mut uncertain_build = Vec::new();
            let mut uncertain_probe = Vec::new();

            refiner
                .refine(
                    &probe_array,
                    ContainerSide::Build,
                    &cand_b,
                    &cand_p,
                    &mut verified_build,
                    &mut verified_probe,
                    &mut uncertain_build,
                    &mut uncertain_probe,
                )
                .unwrap();

            let verified_pairs: Vec<(u32, u32)> =
                verified_build.into_iter().zip(verified_probe).collect();
            let uncertain_pairs: Vec<(u32, u32)> =
                uncertain_build.into_iter().zip(uncertain_probe).collect();

            let (_inside, uncertain, _outside, mismatches) = verify_two_sided(
                &verified_pairs,
                &uncertain_pairs,
                &candidate_pairs,
                &polys,
                &probes,
            );

            let uncertain_rate = (uncertain as f64) / (candidate_pairs.len() as f64);

            println!(
                "[GATE 2 CELL] Center: {:+.0e} | R: {:.0e} | Pairs: {:5} | Mismatches: {} | Uncertain: {:.2}% | asserts GPU-Inside => oracle-Inside and GPU-Outside => oracle-Outside",
                cx,
                r,
                candidate_pairs.len(),
                mismatches,
                uncertain_rate * 100.0
            );

            assert_eq!(
                mismatches, 0,
                "Cell center: {}, R: {} failed with {} mismatches",
                cx, r, mismatches
            );
        }
    }
    println!(
        "------------------------------------------------------------------------------------------------------"
    );
    println!(
        "Gate 2: All 20 cells in scale matrix passed with 0 mismatches on {}",
        device_name
    );
}

#[test]
fn test_gate_3_deterministic_uniform_lattice() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    let poly = MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![
                (100.0, 100.0),
                (200.0, 100.0),
                (200.0, 200.0),
                (100.0, 200.0),
            ]],
        }],
    };
    let poly_wkb = make_multipoly_wkb(&poly);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    // Deterministic 200x250 uniform lattice over [80, 220] x [80, 220]
    let count_x = 200;
    let count_y = 250;
    let total_points = count_x * count_y;
    let mut points = Vec::with_capacity(total_points);

    for ix in 0..count_x {
        for iy in 0..count_y {
            let px = 80.0 + (ix as f64) * (140.0 / count_x as f64);
            let py = 80.0 + (iy as f64) * (140.0 / count_y as f64);
            points.push((px, py));
        }
    }

    let probe_wkbs: Vec<Vec<u8>> = points.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    let build_indices: Vec<u32> = vec![0; total_points];
    let probe_indices: Vec<u32> = (0..total_points as u32).collect();

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

    let uncertain_rate = (uncertain_build.len() as f64) / (total_points as f64);
    println!(
        "[GATE 3 REPORT] Device: {} | Dataset: Deterministic Uniform Lattice 50K | Verified: {} | Uncertain: {} | Rate: {:.4}% | asserts uncertain rate < 5%",
        device_name,
        verified_build.len(),
        uncertain_build.len(),
        uncertain_rate * 100.0
    );

    assert!(
        uncertain_rate < 0.05,
        "Uncertainty rate for deterministic lattice points must be < 5%"
    );
}

#[test]
fn test_gate_4_mixed_types_and_degeneracies_both_sides() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");

    // Build side with:
    // 0: Valid polygon
    // 1: Null row
    // 2: NaN polygon coordinates
    // 3: Empty WKB
    // 4: Corrupt bytes
    let valid_poly = make_multipoly_wkb(&MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]],
        }],
    });
    let nan_poly = make_point_wkb(f64::NAN, 1.0); // wrong type and nan
    let empty_wkb = vec![1u8, 3, 0, 0, 0, 0, 0, 0, 0];
    let corrupt_wkb = vec![1u8, 3, 0];
    // 5: Unclosed ring (omits closing vertex)
    let unclosed_poly =
        make_unclosed_poly_wkb(&[vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]]);
    // 6: Degenerate ring with 2 vertices
    let degen_2_poly = make_degenerate_poly_wkb(2);
    // 7: Degenerate ring with 1 vertex
    let degen_1_poly = make_degenerate_poly_wkb(1);

    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(valid_poly.as_slice()),
        None,
        Some(nan_poly.as_slice()),
        Some(empty_wkb.as_slice()),
        Some(corrupt_wkb.as_slice()),
        Some(unclosed_poly.as_slice()),
        Some(degen_2_poly.as_slice()),
        Some(degen_1_poly.as_slice()),
    ]));
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    // Probe side with:
    // 0: Valid point interior (5, 5)
    // 1: Empty WKB
    // 2: NaN point coordinates
    // 3: LineString
    // 4: MultiPoint
    let p_valid = make_point_wkb(5.0, 5.0);
    let p_empty = vec![1u8, 1, 0, 0, 0];
    let p_nan = make_point_wkb(f64::NAN, 5.0);
    let mut p_line = vec![1u8];
    p_line.write_u32::<LittleEndian>(2).unwrap();
    p_line.write_u32::<LittleEndian>(2).unwrap();
    p_line.extend_from_slice(&[0u8; 32]);
    let mut p_mp = vec![1u8];
    p_mp.write_u32::<LittleEndian>(4).unwrap();
    p_mp.write_u32::<LittleEndian>(0).unwrap();

    let probe_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(p_valid.as_slice()),
        Some(p_empty.as_slice()),
        Some(p_nan.as_slice()),
        Some(p_line.as_slice()),
        Some(p_mp.as_slice()),
    ]));

    // Pairs testing both sides:
    // (0, 0): valid build, valid probe -> verified
    // (1, 0): null build, valid probe -> uncertain
    // (2, 0): nan build, valid probe -> uncertain
    // (3, 0): empty build, valid probe -> uncertain
    // (0, 1): valid build, empty probe -> uncertain
    // (0, 2): valid build, nan probe -> uncertain
    // (0, 3): valid build, line probe -> uncertain
    // (0, 4): valid build, multipoint probe -> uncertain
    // (99, 0): out-of-range build index -> uncertain (no GPU OOB)
    // (0, 99): out-of-range probe index -> uncertain (no GPU OOB)
    // (5, 0): unclosed ring build, valid probe -> uncertain
    // (6, 0): degenerate ring with 2 verts, valid probe -> uncertain
    // (7, 0): degenerate ring with 1 vert, valid probe -> uncertain
    let build_indices = vec![0, 1, 2, 3, 0, 0, 0, 0, 99, 0, 5, 6, 7];
    let probe_indices = vec![0, 0, 0, 0, 1, 2, 3, 4, 0, 99, 0, 0, 0];

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

    println!(
        "[GATE 4 REPORT] Verified: {:?} | Uncertain count: {} | asserts mixed/nan/out-of-range rows route to uncertain with indices intact",
        verified_probe,
        uncertain_probe.len()
    );

    // Pairs (0, 0) and (5, 0) are valid and inside (polygon 5 is unclosed ring, correctly closed and verified)
    assert_eq!(verified_build, vec![0, 5]);
    assert_eq!(verified_probe, vec![0, 0]);

    // All other 11 pairs must route to uncertain (null, nan, empty, corrupt, degenerate < 3 verts, oob)
    assert_eq!(uncertain_build.len(), 11);
    assert_eq!(uncertain_probe.len(), 11);
    assert_eq!(uncertain_build, vec![1, 2, 3, 0, 0, 0, 0, 99, 0, 6, 7]);
    assert_eq!(uncertain_probe, vec![0, 0, 0, 1, 2, 3, 4, 0, 99, 0, 0]);
}

#[test]
fn test_gate_5_and_6_swapped_and_container_inversion() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");

    // Case (a): Real swapped case: Points pushed as build array and Polygons as probe array
    let pt = make_point_wkb(5.0, 5.0);
    let pt_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(pt.as_slice())]));
    refiner.push_build(&pt_array).unwrap();
    refiner.finish_building().unwrap();

    let poly = make_multipoly_wkb(&MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]],
        }],
    });
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly.as_slice())]));

    let build_indices = vec![0];
    let probe_indices = vec![0];

    let mut v_b = Vec::new();
    let mut v_p = Vec::new();
    let mut u_b = Vec::new();
    let mut u_p = Vec::new();

    refiner
        .refine(
            &poly_array,
            ContainerSide::Build,
            &build_indices,
            &probe_indices,
            &mut v_b,
            &mut v_p,
            &mut u_b,
            &mut u_p,
        )
        .unwrap();

    // Must route to uncertain with intact indices and zero panics
    assert!(
        v_b.is_empty(),
        "Point on build side cannot produce verified matches"
    );
    assert_eq!(u_b, vec![0]);
    assert_eq!(u_p, vec![0]);

    // Case (b): ContainerSide::Either with interior, exterior and boundary points
    refiner.clear().unwrap();
    refiner.push_build(&poly_array).unwrap(); // polygon on build side
    refiner.finish_building().unwrap();

    let probes = [
        make_point_wkb(5.0, 5.0),  // interior
        make_point_wkb(15.0, 5.0), // exterior
        make_point_wkb(10.0, 5.0), // boundary
    ];
    let probe_slices: Vec<Option<&[u8]>> = probes.iter().map(|w| Some(w.as_slice())).collect();
    let probe_arr: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    let b_idx = vec![0, 0, 0];
    let p_idx = vec![0, 1, 2];
    let mut vb2 = Vec::new();
    let mut vp2 = Vec::new();
    let mut ub2 = Vec::new();
    let mut up2 = Vec::new();

    refiner
        .refine(
            &probe_arr,
            ContainerSide::Either,
            &b_idx,
            &p_idx,
            &mut vb2,
            &mut vp2,
            &mut ub2,
            &mut up2,
        )
        .unwrap();

    println!(
        "[GATE 5/6 REPORT] Either verified: {:?} | Either uncertain: {:?} | asserts ContainerSide::Either verifies interior and routes boundary to uncertain",
        vp2, up2
    );
    assert_eq!(vp2, vec![0], "Interior point 0 must be verified");
    assert!(
        up2.contains(&2),
        "Boundary point 2 must be routed to uncertain"
    );

    // Case (c): ContainerSide::Probe with strictly interior points (Gate 6 Inversion Gate)
    let mut vb3 = Vec::new();
    let mut vp3 = Vec::new();
    let mut ub3 = Vec::new();
    let mut up3 = Vec::new();

    refiner
        .refine(
            &probe_arr,
            ContainerSide::Probe,
            &b_idx,
            &p_idx,
            &mut vb3,
            &mut vp3,
            &mut ub3,
            &mut up3,
        )
        .unwrap();

    assert!(
        vb3.is_empty(),
        "ContainerSide::Probe must produce exactly zero verified pairs from GPU"
    );
    assert_eq!(ub3.len(), 3);
}

#[test]
fn test_gate_7_teeth_test_adversarial_v1_vs_v2_bound() {
    // T3: Run the adversarial suite under Modes 0, 1, 2, and 3:
    // Mode 0: Certified v2 (All guards ON: v2 det bound, full band, double-single delta)
    // Mode 1: v1 det bound only (flawed v1 det bound, band ON, double-single delta ON)
    // Mode 2: band off only (v2 det bound ON, double-single delta ON, eta-band OFF)
    // Mode 3: naive delta only (v2 det bound ON, band ON, naive single-precision delta)
    let cx = 10_000_000.0f64;
    let cy = 10_000_000.0f64;
    let r = 100_000.0f64;

    let (polys, probes, candidate_pairs) = generate_adversarial_suite(cx, cy, r, 9999);

    let poly_wkbs: Vec<Vec<u8>> = polys.iter().map(make_multipoly_wkb).collect();
    let poly_slices: Vec<Option<&[u8]>> = poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(poly_slices));

    let probe_wkbs: Vec<Vec<u8>> = probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    let (cand_b, cand_p): (Vec<u32>, Vec<u32>) = candidate_pairs.iter().copied().unzip();

    let run_mode = |mode: i32| -> (usize, usize, usize, usize) {
        let mut refiner =
            MetalSpatialRefiner::try_new_with_mode(mode).expect("Failed to create refiner");
        refiner.push_build(&poly_array).unwrap();
        refiner.finish_building().unwrap();

        let mut vb = Vec::new();
        let mut vp = Vec::new();
        let mut ub = Vec::new();
        let mut up = Vec::new();

        refiner
            .refine(
                &probe_array,
                ContainerSide::Build,
                &cand_b,
                &cand_p,
                &mut vb,
                &mut vp,
                &mut ub,
                &mut up,
            )
            .unwrap();

        let verified: Vec<(u32, u32)> = vb.into_iter().zip(vp).collect();
        let uncertain: Vec<(u32, u32)> = ub.into_iter().zip(up).collect();

        verify_two_sided(&verified, &uncertain, &candidate_pairs, &polys, &probes)
    };

    // Mode 0: Certified v2 (All guards ON: v2 det bound, full band, double-single delta)
    let (v2_in, v2_unc, v2_out, v2_mismatches) = run_mode(0);
    println!(
        "[GATE 7 TEETH TEST - MODE 0 (V2 CERTIFIED)] Pairs: {} | Inside: {} | Outside: {} | Uncertain: {} | Mismatches: {}",
        candidate_pairs.len(),
        v2_in,
        v2_out,
        v2_unc,
        v2_mismatches
    );

    // Mode 1: v1 det bound only (flawed v1 det bound, band ON, double-single delta ON)
    let (m1_in, m1_unc, m1_out, m1_mismatches) = run_mode(1);
    println!(
        "[GATE 7 TEETH TEST - MODE 1 (V1 DET ONLY)]  Pairs: {} | Inside: {} | Outside: {} | Uncertain: {} | Mismatches: {}",
        candidate_pairs.len(),
        m1_in,
        m1_out,
        m1_unc,
        m1_mismatches
    );

    // Mode 2: band off only (v2 det bound ON, double-single delta ON, eta-band OFF)
    let (m2_in, m2_unc, m2_out, m2_mismatches) = run_mode(2);
    println!(
        "[GATE 7 TEETH TEST - MODE 2 (BAND OFF ONLY)] Pairs: {} | Inside: {} | Outside: {} | Uncertain: {} | Mismatches: {}",
        candidate_pairs.len(),
        m2_in,
        m2_out,
        m2_unc,
        m2_mismatches
    );

    // Mode 3: naive delta only (v2 det bound ON, band ON, naive single-precision delta)
    let (m3_in, m3_unc, m3_out, m3_mismatches) = run_mode(3);
    println!(
        "[GATE 7 TEETH TEST - MODE 3 (NAIVE DELTA)]  Pairs: {} | Inside: {} | Outside: {} | Uncertain: {} | Mismatches: {}",
        candidate_pairs.len(),
        m3_in,
        m3_out,
        m3_unc,
        m3_mismatches
    );

    // Assertions:
    assert_eq!(
        v2_mismatches, 0,
        "Mode 0 (v2 certified) MUST produce exactly 0 mismatches!"
    );
    assert!(
        m1_mismatches > 0,
        "Mode 1 (v1 det only) MUST produce > 0 mismatches!"
    );
    assert!(
        m2_mismatches > 0,
        "Mode 2 (band off only) MUST produce > 0 mismatches!"
    );
    assert!(
        m3_mismatches > 0,
        "Mode 3 (naive delta only) MUST produce > 0 mismatches!"
    );
}

#[test]
fn test_gate_8_spanning_shallow_edges_sub_ulp_sweep() {
    // Targets the certified-sign premise of the straddle test on spanning edges:
    // the left endpoint v1 sits just left of the probe (x1 < -eta_k) and within a
    // sub-ulp distance of the probe's y, while the right endpoint v2 is well outside
    // the eta_k band. The true crossing of the +x ray then depends on sign(y1) alone.
    // Mode 5 (trap skipped for x < -eta_k) is evaluated alongside for comparison.
    let scales: [f64; 4] = [1e2, 1e4, 1e6, 1e7];
    let mut ref_m0 = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
    let mut ref_m5 = MetalSpatialRefiner::try_new_with_mode(5).unwrap();
    let mut total_m5_mismatches = 0usize;
    let mut total_probes = 0usize;
    let (mut unc0, mut unc5) = (0usize, 0usize);

    for &center in &scales {
        let (cx, cy) = (center, center);
        let span = (center * 0.1f64).max(10.0);
        let ulp_rel = (span as f32).next_up() as f64 - (span as f32) as f64;
        let rise = 5e-6 * span; // ~8 eta_k: v2 stays outside the band

        for si in -8i32..=8 {
            let v1y = cy + (si as f64) * 0.125 * ulp_rel;
            let poly_def = MultiPolyDef {
                parts: vec![PolyPart {
                    rings: vec![vec![
                        (cx - 1e-3 * span, v1y),
                        (cx + 1.999 * span, v1y + rise),
                        (cx + 1.999 * span, cy + span),
                        (cx - 1e-3 * span, cy + span),
                    ]],
                }],
            };
            let poly_wkb = make_multipoly_wkb(&poly_def);
            let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

            let probes: Vec<(f64, f64)> = (-40i32..=40)
                .map(|j| (cx, v1y + (j as f64) * 0.05 * ulp_rel))
                .collect();
            let probe_wkbs: Vec<Vec<u8>> =
                probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
            let probe_array: ArrayRef = Arc::new(BinaryArray::from(
                probe_wkbs
                    .iter()
                    .map(|w| Some(w.as_slice()))
                    .collect::<Vec<_>>(),
            ));
            let cand_b = vec![0u32; probes.len()];
            let cand_p: Vec<u32> = (0..probes.len() as u32).collect();
            let pairs: Vec<(u32, u32)> = cand_p.iter().map(|&p| (0u32, p)).collect();
            total_probes += probes.len();

            let run = |r: &mut MetalSpatialRefiner| {
                r.clear().unwrap();
                r.push_build(&poly_array).unwrap();
                r.finish_building().unwrap();
                let (mut vb, mut vp, mut ub, mut up) = (vec![], vec![], vec![], vec![]);
                r.refine(
                    &probe_array,
                    ContainerSide::Build,
                    &cand_b,
                    &cand_p,
                    &mut vb,
                    &mut vp,
                    &mut ub,
                    &mut up,
                )
                .unwrap();
                let v: Vec<(u32, u32)> = vb.into_iter().zip(vp).collect();
                let u: Vec<(u32, u32)> = ub.into_iter().zip(up).collect();
                verify_two_sided(&v, &u, &pairs, std::slice::from_ref(&poly_def), &probes)
            };

            let (_, u0, _, mm0) = run(&mut ref_m0);
            let (_, u5, _, mm5) = run(&mut ref_m5);
            unc0 += u0;
            unc5 += u5;
            total_m5_mismatches += mm5;
            assert_eq!(mm0, 0, "Mode 0 mismatches at scale {center:e}, si={si}");
        }
    }
    println!(
        "GATE 8: {total_probes} probes | mode 0: {unc0} uncertain, 0 mismatches | mode 5 (unsound trap): {unc5} uncertain, {total_m5_mismatches} mismatches"
    );
}

#[test]
fn test_star_polygons_honest_uncertain_rate() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to create refiner");
    let mut rng = SeededRng::new(99999);
    let r_poly = 1000.0;
    let cx = 0.0;
    let cy = 0.0;

    println!(
        "------------------------------------------------------------------------------------------------------"
    );
    println!(
        "HONEST UNCERTAIN RATE BENCHMARK (Uniformly Random Probes on Random Star Polygons with Full Band)"
    );
    println!(
        "------------------------------------------------------------------------------------------------------"
    );

    for &n_verts in &[8, 64, 512, 2000] {
        refiner.clear().unwrap();

        let ring = generate_random_star_ring(cx, cy, r_poly, n_verts, &mut rng);
        let poly_def = MultiPolyDef {
            parts: vec![PolyPart { rings: vec![ring] }],
        };
        let poly_wkb = make_multipoly_wkb(&poly_def);
        let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));
        refiner.push_build(&poly_array).unwrap();
        refiner.finish_building().unwrap();

        // 2000 uniformly distributed random probe points within [-R, R] x [-R, R]
        let num_probes = 2000;
        let mut probe_wkbs = Vec::with_capacity(num_probes);
        let mut probe_coords = Vec::with_capacity(num_probes);
        for _ in 0..num_probes {
            let px = cx + (rng.next_f64() * 2.0 - 1.0) * r_poly;
            let py = cy + (rng.next_f64() * 2.0 - 1.0) * r_poly;
            probe_coords.push((px, py));
            probe_wkbs.push(make_point_wkb(px, py));
        }

        let probe_slices: Vec<Option<&[u8]>> =
            probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
        let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

        let cand_b = vec![0u32; num_probes];
        let cand_p: Vec<u32> = (0..num_probes as u32).collect();
        let candidate_pairs: Vec<(u32, u32)> =
            cand_b.iter().copied().zip(cand_p.iter().copied()).collect();

        let mut vb = Vec::new();
        let mut vp = Vec::new();
        let mut ub = Vec::new();
        let mut up = Vec::new();

        refiner
            .refine(
                &probe_array,
                ContainerSide::Build,
                &cand_b,
                &cand_p,
                &mut vb,
                &mut vp,
                &mut ub,
                &mut up,
            )
            .unwrap();

        let verified_pairs: Vec<(u32, u32)> = vb.into_iter().zip(vp).collect();
        let uncertain_pairs: Vec<(u32, u32)> = ub.into_iter().zip(up).collect();

        let (inside, uncertain, outside, mismatches) = verify_two_sided(
            &verified_pairs,
            &uncertain_pairs,
            &candidate_pairs,
            &[poly_def],
            &probe_coords,
        );

        let rate = (uncertain as f64) / (num_probes as f64) * 100.0;
        println!(
            "[HONEST UNCERTAIN RATE] n = {:4} | Probes: {} | Inside: {:4} | Outside: {:4} | Uncertain: {:4} ({:5.2}%) | Mismatches: {}",
            n_verts, num_probes, inside, outside, uncertain, rate, mismatches
        );

        assert_eq!(
            mismatches, 0,
            "Honest random probe test produced mismatches!"
        );
    }
}

/// Helper for generating realistic multi-harmonic non-convex coastline boundaries
fn generate_realistic_coastline_ring(
    cx: f64,
    cy: f64,
    r_poly: f64,
    n: usize,
    rng: &mut SeededRng,
) -> Vec<(f64, f64)> {
    let mut ring = Vec::with_capacity(n);
    let mut phases = [0.0f64; 16];
    let mut amps = [0.0f64; 16];
    for k in 1..16 {
        phases[k] = rng.next_f64() * std::f64::consts::TAU;
        amps[k] = (0.2 / (k as f64).powf(0.7)) * (0.8 + 0.4 * rng.next_f64());
    }
    for i in 0..n {
        let theta = (i as f64) * std::f64::consts::TAU / (n as f64);
        let mut rad_scale = 1.0;
        for k in 1..16 {
            rad_scale += amps[k] * (k as f64 * theta + phases[k]).cos();
        }
        let r = r_poly * rad_scale.max(0.2);
        ring.push((cx + r * theta.cos(), cy + r * theta.sin()));
    }
    ring
}

#[test]
#[ignore]
fn test_realistic_multi_scale_uncertain_rate_benchmark() {
    let mut rng = SeededRng::new(1234567);
    let test_scales = [
        ("Projected (EPSG:3857)", 500_000.0, 500_000.0, 10_000.0),
        ("Geographic (lon/lat)", -74.0, 40.7, 0.5),
    ];
    let vertex_counts = [1_000, 5_000, 10_000, 25_000, 50_000, 100_000];
    let num_probes = 5_000;

    println!(
        "\n======================================================================================================================"
    );
    println!(
        " REALISTIC MULTI-SCALE UNCERTAIN RATE & TIMING BENCHMARK (1k - 100k Vertices, Random Probes)"
    );
    println!(" Comparing Legacy Unrestricted Band (Mode 4) vs. Certified Two-Ray (Mode 0)");
    println!(
        "======================================================================================================================\n"
    );

    for &(scale_name, cx, cy, r_poly) in &test_scales {
        println!(
            ">>> Coordinate System: {} | Center: ({:.1}, {:.1}) | Radius: {:.1}",
            scale_name, cx, cy, r_poly
        );
        println!("{:-<96}", "");
        println!(
            "{:<10} | {:<14} | {:<12} | {:<12} | {:<12} | {:<10}",
            "Vertices", "Mode", "Candidates", "Uncertain", "Unc Rate %", "GPU (ms)"
        );
        println!("{:-<96}", "");

        for &n_verts in &vertex_counts {
            let ring = generate_realistic_coastline_ring(cx, cy, r_poly, n_verts, &mut rng);
            let poly_def = MultiPolyDef {
                parts: vec![PolyPart { rings: vec![ring] }],
            };
            let poly_wkb = make_multipoly_wkb(&poly_def);
            let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

            let min_x = cx - r_poly * 1.5;
            let max_x = cx + r_poly * 1.5;
            let min_y = cy - r_poly * 1.5;
            let max_y = cy + r_poly * 1.5;

            let mut probe_wkbs = Vec::with_capacity(num_probes);
            for _ in 0..num_probes {
                let px = min_x + rng.next_f64() * (max_x - min_x);
                let py = min_y + rng.next_f64() * (max_y - min_y);
                probe_wkbs.push(make_point_wkb(px, py));
            }

            let probe_slices: Vec<Option<&[u8]>> =
                probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
            let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

            let cand_b = vec![0u32; num_probes];
            let cand_p: Vec<u32> = (0..num_probes as u32).collect();

            for &(mode, mode_name) in &[(4, "Legacy (4)"), (0, "Two-Ray (0)")] {
                let mut refiner =
                    MetalSpatialRefiner::try_new_with_mode(mode).expect("Failed to create refiner");
                refiner.push_build(&poly_array).unwrap();
                refiner.finish_building().unwrap();

                let mut vb = Vec::new();
                let mut vp = Vec::new();
                let mut ub = Vec::new();
                let mut up = Vec::new();

                let t_gpu_start = std::time::Instant::now();
                refiner
                    .refine(
                        &probe_array,
                        ContainerSide::Build,
                        &cand_b,
                        &cand_p,
                        &mut vb,
                        &mut vp,
                        &mut ub,
                        &mut up,
                    )
                    .unwrap();
                let gpu_elapsed = t_gpu_start.elapsed();

                let num_uncertain = ub.len();
                let unc_rate = (num_uncertain as f64) / (num_probes as f64) * 100.0;

                println!(
                    "{:<10} | {:<14} | {:<12} | {:<12} | {:>11.3}% | {:>10.2}",
                    n_verts,
                    mode_name,
                    num_probes,
                    num_uncertain,
                    unc_rate,
                    gpu_elapsed.as_secs_f64() * 1000.0
                );
            }
            println!("{:-<96}", "");
        }
        println!();
    }
}

#[test]
fn test_c1_ewkb_end_to_end_in_gpu_refiner() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to create refiner");

    // Rings for EWKB geometries
    let rings: [&[(f64, f64)]; 1] = [&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ]];

    // 0. Standard 2D Polygon without SRID
    let std_2d = make_ewkb_polygon(&rings, false, false, false, false);
    // 1. EWKB PolygonZ with SRID
    let ewkb_z_srid = make_ewkb_polygon(&rings, true, true, false, false);
    // 2. EWKB PolygonM without SRID
    let ewkb_m = make_ewkb_polygon(&rings, false, false, true, false);
    // 3. EWKB PolygonZM with SRID
    let ewkb_zm_srid = make_ewkb_polygon(&rings, true, true, true, false);
    // 4. EWKB PolygonZ Big-Endian
    let ewkb_be = make_ewkb_polygon(&rings, false, true, false, true);
    // 5. ISO PolygonZ (1003)
    let iso_z = make_iso_z_polygon(&rings);

    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(std_2d.as_slice()),
        Some(ewkb_z_srid.as_slice()),
        Some(ewkb_m.as_slice()),
        Some(ewkb_zm_srid.as_slice()),
        Some(ewkb_be.as_slice()),
        Some(iso_z.as_slice()),
    ]));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    let probes = [
        make_point_wkb(5.0, 5.0),  // strictly interior to all 6
        make_point_wkb(15.0, 5.0), // strictly exterior to all 6
    ];
    let probe_slices: Vec<Option<&[u8]>> = probes.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    // Test pairs:
    // (poly 0..5, probe 0): interior
    // (poly 0..5, probe 1): exterior
    let mut b_idx = Vec::new();
    let mut p_idx = Vec::new();
    for b in 0..6 {
        b_idx.push(b as u32);
        p_idx.push(0);
        b_idx.push(b as u32);
        p_idx.push(1);
    }

    let mut vb = Vec::new();
    let mut vp = Vec::new();
    let mut ub = Vec::new();
    let mut up = Vec::new();

    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &b_idx,
            &p_idx,
            &mut vb,
            &mut vp,
            &mut ub,
            &mut up,
        )
        .unwrap();

    println!(
        "[C1 REPORT] EWKB flavours in GPU refiner: Verified interior matches: {} | asserts EWKB Z/M/ZM/SRID/BE/ISO polygons classify correctly on GPU",
        vb.len()
    );

    // All 6 EWKB flavours must correctly classify interior point (probe 0) as verified!
    assert_eq!(
        vb.len(),
        6,
        "All 6 EWKB polygon variants must be verified for interior point"
    );
    for b in 0..6 {
        assert!(vb.contains(&b));
    }
}

fn make_ewkb_polygon(
    rings: &[&[(f64, f64)]],
    with_srid: bool,
    with_z: bool,
    with_m: bool,
    is_be: bool,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(if is_be { 0u8 } else { 1u8 });

    let mut geom_type: u32 = 3;
    if with_z {
        geom_type |= 0x80000000;
    }
    if with_m {
        geom_type |= 0x40000000;
    }
    if with_srid {
        geom_type |= 0x20000000;
    }

    let mut u32_buf = [0u8; 4];
    if is_be {
        BigEndian::write_u32(&mut u32_buf, geom_type);
    } else {
        LittleEndian::write_u32(&mut u32_buf, geom_type);
    }
    buf.extend_from_slice(&u32_buf);

    if with_srid {
        if is_be {
            BigEndian::write_u32(&mut u32_buf, 4326);
        } else {
            LittleEndian::write_u32(&mut u32_buf, 4326);
        }
        buf.extend_from_slice(&u32_buf);
    }

    if is_be {
        BigEndian::write_u32(&mut u32_buf, rings.len() as u32);
    } else {
        LittleEndian::write_u32(&mut u32_buf, rings.len() as u32);
    }
    buf.extend_from_slice(&u32_buf);

    for ring in rings {
        if is_be {
            BigEndian::write_u32(&mut u32_buf, ring.len() as u32);
        } else {
            LittleEndian::write_u32(&mut u32_buf, ring.len() as u32);
        }
        buf.extend_from_slice(&u32_buf);

        for &(x, y) in *ring {
            let mut f64_buf = [0u8; 8];
            if is_be {
                BigEndian::write_f64(&mut f64_buf, x);
            } else {
                LittleEndian::write_f64(&mut f64_buf, x);
            }
            buf.extend_from_slice(&f64_buf);
            if is_be {
                BigEndian::write_f64(&mut f64_buf, y);
            } else {
                LittleEndian::write_f64(&mut f64_buf, y);
            }
            buf.extend_from_slice(&f64_buf);
            if with_z {
                if is_be {
                    BigEndian::write_f64(&mut f64_buf, 42.0);
                } else {
                    LittleEndian::write_f64(&mut f64_buf, 42.0);
                }
                buf.extend_from_slice(&f64_buf);
            }
            if with_m {
                if is_be {
                    BigEndian::write_f64(&mut f64_buf, 100.0);
                } else {
                    LittleEndian::write_f64(&mut f64_buf, 100.0);
                }
                buf.extend_from_slice(&f64_buf);
            }
        }
    }
    buf
}

fn make_iso_z_polygon(rings: &[&[(f64, f64)]]) -> Vec<u8> {
    let mut buf = vec![1u8]; // Little endian
    let mut u32_buf = [0u8; 4];
    LittleEndian::write_u32(&mut u32_buf, 1003); // ISO PolygonZ
    buf.extend_from_slice(&u32_buf);
    LittleEndian::write_u32(&mut u32_buf, rings.len() as u32);
    buf.extend_from_slice(&u32_buf);
    for ring in rings {
        LittleEndian::write_u32(&mut u32_buf, ring.len() as u32);
        buf.extend_from_slice(&u32_buf);
        for &(x, y) in *ring {
            let mut f64_buf = [0u8; 8];
            LittleEndian::write_f64(&mut f64_buf, x);
            buf.extend_from_slice(&f64_buf);
            LittleEndian::write_f64(&mut f64_buf, y);
            buf.extend_from_slice(&f64_buf);
            LittleEndian::write_f64(&mut f64_buf, 12.34); // Z
            buf.extend_from_slice(&f64_buf);
        }
    }
    buf
}

#[test]
fn test_concurrent_multi_threaded_refiner() {
    let mut refiner = MetalSpatialRefiner::try_new().expect("Failed to initialize Metal refiner");
    let device_name = refiner.device_name().to_string();

    let poly = MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![(10.0, 10.0), (50.0, 10.0), (50.0, 50.0), (10.0, 50.0)]],
        }],
    };
    let poly_wkb = make_multipoly_wkb(&poly);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();

    let refiner = Arc::new(refiner);

    // Prepare probe points: some inside, some outside
    let test_points = [
        (25.0, 25.0), // inside
        (30.0, 30.0), // inside
        (5.0, 5.0),   // outside
        (60.0, 60.0), // outside
        (20.0, 40.0), // inside
    ];
    let probe_wkbs: Vec<Vec<u8>> = test_points
        .iter()
        .map(|&(x, y)| make_point_wkb(x, y))
        .collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));

    let candidate_build: Vec<u32> = vec![0; test_points.len()];
    let candidate_probe: Vec<u32> = (0..test_points.len() as u32).collect();

    // Baseline single-threaded
    let mut base_v_b = Vec::new();
    let mut base_v_p = Vec::new();
    let mut base_u_b = Vec::new();
    let mut base_u_p = Vec::new();
    refiner
        .refine(
            &probe_array,
            ContainerSide::Build,
            &candidate_build,
            &candidate_probe,
            &mut base_v_b,
            &mut base_v_p,
            &mut base_u_b,
            &mut base_u_p,
        )
        .unwrap();

    // 8 concurrent threads sharing the same refiner
    let mut handles = Vec::new();
    for thread_id in 0..8 {
        let r = Arc::clone(&refiner);
        let p_arr = Arc::clone(&probe_array);
        let c_b = candidate_build.clone();
        let c_p = candidate_probe.clone();
        let expected_v_p = base_v_p.clone();
        let expected_u_p = base_u_p.clone();

        handles.push(std::thread::spawn(move || {
            let mut th_v_b = Vec::new();
            let mut th_v_p = Vec::new();
            let mut th_u_b = Vec::new();
            let mut th_u_p = Vec::new();

            r.refine(
                &p_arr,
                ContainerSide::Build,
                &c_b,
                &c_p,
                &mut th_v_b,
                &mut th_v_p,
                &mut th_u_b,
                &mut th_u_p,
            )
            .unwrap_or_else(|e| panic!("Thread {} failed refine: {:?}", thread_id, e));

            assert_eq!(
                th_v_p, expected_v_p,
                "Thread {} verified probe mismatch",
                thread_id
            );
            assert_eq!(
                th_u_p, expected_u_p,
                "Thread {} uncertain probe mismatch",
                thread_id
            );
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    println!(
        "P5: 8-thread concurrent refiner test passed on {}",
        device_name
    );
}

// =============================================================================
// PROTOTYPE: exact GPU second-stage resolver (measurement only)
//
// These tests drive `MetalSpatialRefiner::refine_exact`, a second compute pass
// that decides the uncertain residue of the f32 kernel exactly, in 128-bit
// fixed-point integer arithmetic, without a CPU hop. The production path
// (BOUND_MODE 0 + CPU fallback) is untouched: the exact pipeline is only
// reachable through `#[cfg(feature = "test-internals")]` entry points.
// =============================================================================

fn limbs_of(v: i128) -> [u32; 4] {
    let u = v as u128;
    [
        (u & 0xFFFF_FFFF) as u32,
        ((u >> 32) & 0xFFFF_FFFF) as u32,
        ((u >> 64) & 0xFFFF_FFFF) as u32,
        ((u >> 96) & 0xFFFF_FFFF) as u32,
    ]
}

fn fixed_probe_from_coord(x: f64, y: f64, scale: i32) -> FixedProbe {
    match (f64_to_fixed(x, scale), f64_to_fixed(y, scale)) {
        (Some(a), Some(b)) => FixedProbe {
            x: limbs_of(a),
            y: limbs_of(b),
            is_exact: 1,
            _padding: 0,
        },
        _ => FixedProbe::default(),
    }
}

fn exact_state_name(s: u8) -> &'static str {
    match s {
        EXACT_OUTSIDE => "Outside",
        EXACT_INSIDE => "Inside",
        EXACT_BOUNDARY => "Boundary",
        _ => "NotDecided",
    }
}

fn oracle_state(o: OracleResult) -> u8 {
    match o {
        OracleResult::Inside => EXACT_INSIDE,
        OracleResult::Outside => EXACT_OUTSIDE,
        OracleResult::OnBoundary => EXACT_BOUNDARY,
    }
}

struct ExactRun {
    uncertain: Vec<(u32, u32)>,
    states: Vec<u8>,
    verified_count: usize,
    scale: i32,
    polys_not_representable: usize,
    t_build_exact_ms: f64,
    t_f32_ms: f64,
    t_prep_full_ms: f64,
    t_prep_subset_ms: f64,
    t_exact_ms: f64,
    f32_bytes: usize,
    exact_bytes: usize,
}

/// Builds both pipelines on one refiner, runs the f32 pass, compacts its
/// uncertain output and resolves that residue with the exact GPU pass.
fn run_exact_pipeline(
    mode: i32,
    poly_array: &ArrayRef,
    probe_array: &ArrayRef,
    probe_coords: &[(f64, f64)],
    cand_b: &[u32],
    cand_p: &[u32],
) -> ExactRun {
    let mut refiner = MetalSpatialRefiner::try_new_with_mode(mode).expect("refiner");
    refiner.push_build(poly_array).unwrap();
    refiner.finish_building().unwrap();

    let t0 = Instant::now();
    refiner.push_build_exact(poly_array);
    let (scale, not_ok) = refiner.finish_exact().unwrap();
    let t_build_exact_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let (mut vb, mut vp, mut ub, mut up) = (vec![], vec![], vec![], vec![]);
    let t1 = Instant::now();
    refiner
        .refine(
            probe_array,
            ContainerSide::Build,
            cand_b,
            cand_p,
            &mut vb,
            &mut vp,
            &mut ub,
            &mut up,
        )
        .unwrap();
    let t_f32_ms = t1.elapsed().as_secs_f64() * 1000.0;

    // Per-batch cost A: re-parse the whole probe WKB column into exact form.
    let t2 = Instant::now();
    let full = flatten_probe_exact(probe_array, scale);
    let t_prep_full_ms = t2.elapsed().as_secs_f64() * 1000.0;

    // Per-batch cost B: compact the residue into a dense probe buffer, reusing
    // coordinates the f32 pass already parsed. This is what a real integration
    // would do, and it makes the upload O(residue) rather than O(batch).
    let t3 = Instant::now();
    let dense: Vec<FixedProbe> = up
        .iter()
        .map(|&p| {
            let (x, y) = probe_coords[p as usize];
            fixed_probe_from_coord(x, y, scale)
        })
        .collect();
    let dense_idx: Vec<u32> = (0..dense.len() as u32).collect();
    let t_prep_subset_ms = t3.elapsed().as_secs_f64() * 1000.0;

    for (k, &p) in up.iter().enumerate() {
        assert_eq!(
            dense[k], full[p as usize],
            "compacted and full exact probe conversion disagree at {p}"
        );
    }

    let t4 = Instant::now();
    let states = refiner.refine_exact(&dense, &ub, &dense_idx).unwrap();
    let t_exact_ms = t4.elapsed().as_secs_f64() * 1000.0;

    ExactRun {
        uncertain: ub.iter().copied().zip(up.iter().copied()).collect(),
        states,
        verified_count: vb.len(),
        scale,
        polys_not_representable: not_ok,
        t_build_exact_ms,
        t_f32_ms,
        t_prep_full_ms,
        t_prep_subset_ms,
        t_exact_ms,
        f32_bytes: refiner.get_memory_usage(),
        exact_bytes: refiner.exact_memory_usage(),
    }
}

struct ExactTally {
    inside: usize,
    outside: usize,
    boundary: usize,
    not_decided: usize,
    mismatches: usize,
}

fn tally_against_oracle(
    pairs: &[(u32, u32)],
    states: &[u8],
    polys: &[MultiPolyDef],
    probes: &[(f64, f64)],
) -> ExactTally {
    let mut t = ExactTally {
        inside: 0,
        outside: 0,
        boundary: 0,
        not_decided: 0,
        mismatches: 0,
    };
    for (k, &(b, p)) in pairs.iter().enumerate() {
        let (px, py) = probes[p as usize];
        let want = oracle_state(exact_pip_oracle(&polys[b as usize], px, py));
        match states[k] {
            EXACT_INSIDE => t.inside += 1,
            EXACT_OUTSIDE => t.outside += 1,
            EXACT_BOUNDARY => t.boundary += 1,
            _ => {
                t.not_decided += 1;
                continue;
            }
        }
        if states[k] != want {
            t.mismatches += 1;
            if t.mismatches <= 5 {
                println!(
                    "  MISMATCH poly {b} probe {p} ({px:e}, {py:e}): gpu {} oracle {}",
                    exact_state_name(states[k]),
                    exact_state_name(want)
                );
            }
        }
    }
    t
}

fn to_arrays(polys: &[MultiPolyDef], probes: &[(f64, f64)]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    (
        polys.iter().map(make_multipoly_wkb).collect(),
        probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect(),
    )
}

fn binary_arrays(poly_wkbs: &[Vec<u8>], probe_wkbs: &[Vec<u8>]) -> (ArrayRef, ArrayRef) {
    let poly_slices: Vec<Option<&[u8]>> = poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    let probe_slices: Vec<Option<&[u8]>> = probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
    (
        Arc::new(BinaryArray::from(poly_slices)),
        Arc::new(BinaryArray::from(probe_slices)),
    )
}

/// Runs the pipeline and asserts the exact pass agrees with the f64 oracle on
/// the uncertain residue. Also re-runs the exact pass over *all* candidate
/// pairs as a stronger independent check.
fn exact_check(
    label: &str,
    mode: i32,
    polys: &[MultiPolyDef],
    probes: &[(f64, f64)],
    pairs: &[(u32, u32)],
) -> ExactTally {
    let (poly_wkbs, probe_wkbs) = to_arrays(polys, probes);
    let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
    let (cand_b, cand_p): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();

    let run = run_exact_pipeline(mode, &poly_array, &probe_array, probes, &cand_b, &cand_p);
    let residue = tally_against_oracle(&run.uncertain, &run.states, polys, probes);

    println!(
        "[EXACT {label}] pairs {} | verified {} | uncertain residue {} ({:.3}%) | Inside {} Outside {} Boundary {} NotDecided {} | mismatches {} | scale 2^{}",
        pairs.len(),
        run.verified_count,
        run.uncertain.len(),
        100.0 * run.uncertain.len() as f64 / pairs.len() as f64,
        residue.inside,
        residue.outside,
        residue.boundary,
        residue.not_decided,
        residue.mismatches,
        run.scale
    );
    println!(
        "            timings ms: build_exact {:.2} | f32 {:.3} | prep_full {:.3} | prep_subset {:.3} | exact {:.3} || bytes: f32 {} exact {}",
        run.t_build_exact_ms,
        run.t_f32_ms,
        run.t_prep_full_ms,
        run.t_prep_subset_ms,
        run.t_exact_ms,
        run.f32_bytes,
        run.exact_bytes
    );
    assert_eq!(
        residue.mismatches, 0,
        "{label}: exact pass disagreed with the f64 oracle on the uncertain residue"
    );
    assert_eq!(
        run.polys_not_representable, 0,
        "{label}: some polygons were not representable in the fixed-point frame"
    );
    residue
}

#[test]
fn test_exact_gate_1_adversarial_residue() {
    let (polys, probes, pairs) = generate_adversarial_suite(500_000.0, 500_000.0, 10_000.0, 42);
    let t = exact_check("GATE 1", 0, &polys, &probes, &pairs);
    assert!(
        t.inside + t.outside + t.boundary > 0,
        "Gate 1 must have a non-empty residue to decide"
    );
    assert_eq!(t.not_decided, 0);
}

#[test]
fn test_exact_gate_1_all_pairs_not_just_residue() {
    // Stronger than the task requires: decide every candidate pair exactly,
    // not only the ones the f32 pass gave up on.
    let (polys, probes, pairs) = generate_adversarial_suite(500_000.0, 500_000.0, 10_000.0, 42);
    let (poly_wkbs, probe_wkbs) = to_arrays(&polys, &probes);
    let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
    let (cand_b, cand_p): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();

    let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();
    refiner.push_build_exact(&poly_array);
    let (scale, _) = refiner.finish_exact().unwrap();
    let fixed = flatten_probe_exact(&probe_array, scale);
    let states = refiner.refine_exact(&fixed, &cand_b, &cand_p).unwrap();

    let t = tally_against_oracle(&pairs, &states, &polys, &probes);
    println!(
        "[EXACT ALL-PAIRS] pairs {} | Inside {} Outside {} Boundary {} NotDecided {} | mismatches {}",
        pairs.len(),
        t.inside,
        t.outside,
        t.boundary,
        t.not_decided,
        t.mismatches
    );
    assert_eq!(t.mismatches, 0);
    assert_eq!(t.not_decided, 0);
    assert!(
        t.boundary > 0,
        "the adversarial suite contains on-edge probes"
    );
}

#[test]
fn test_exact_gate_2_scale_matrix() {
    let centers = [1e2, 5e5, 1e7, -1e7];
    let radii = [1e-3, 1.0, 1e3, 1e5, 1e6];
    for &cx in &centers {
        for &r in &radii {
            let (polys, probes, pairs) = generate_adversarial_suite(cx, cx, r, 12345);
            exact_check(
                &format!("GATE 2 c={cx:+.0e} R={r:.0e}"),
                0,
                &polys,
                &probes,
                &pairs,
            );
        }
    }
}

#[test]
fn test_exact_gate_7_high_magnitude() {
    let (polys, probes, pairs) =
        generate_adversarial_suite(10_000_000.0, 10_000_000.0, 100_000.0, 9999);
    exact_check("GATE 7 mode 0", 0, &polys, &probes, &pairs);
    // Mode 4 leaves a much larger residue, so this exercises the exact pass
    // over a far wider slice of the same point set.
    exact_check("GATE 7 mode 4", 4, &polys, &probes, &pairs);
}

#[test]
fn test_exact_gate_8_spanning_shallow_edges() {
    let scales: [f64; 4] = [1e2, 1e4, 1e6, 1e7];
    let mut total_residue = 0usize;
    for &center in &scales {
        let (cx, cy) = (center, center);
        let span = (center * 0.1f64).max(10.0);
        let ulp_rel = (span as f32).next_up() as f64 - (span as f32) as f64;
        let rise = 5e-6 * span;

        for si in -8i32..=8 {
            let v1y = cy + (si as f64) * 0.125 * ulp_rel;
            let poly_def = MultiPolyDef {
                parts: vec![PolyPart {
                    rings: vec![vec![
                        (cx - 1e-3 * span, v1y),
                        (cx + 1.999 * span, v1y + rise),
                        (cx + 1.999 * span, cy + span),
                        (cx - 1e-3 * span, cy + span),
                    ]],
                }],
            };
            let probes: Vec<(f64, f64)> = (-40i32..=40)
                .map(|j| (cx, v1y + (j as f64) * 0.05 * ulp_rel))
                .collect();
            let pairs: Vec<(u32, u32)> = (0..probes.len() as u32).map(|p| (0u32, p)).collect();

            let (poly_wkbs, probe_wkbs) = to_arrays(std::slice::from_ref(&poly_def), &probes);
            let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
            let (cb, cp): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();
            let run = run_exact_pipeline(0, &poly_array, &probe_array, &probes, &cb, &cp);
            let t = tally_against_oracle(
                &run.uncertain,
                &run.states,
                std::slice::from_ref(&poly_def),
                &probes,
            );
            total_residue += run.uncertain.len();
            assert_eq!(t.mismatches, 0, "gate 8 scale {center:e} si {si}");
            assert_eq!(t.not_decided, 0);
        }
    }
    println!("[EXACT GATE 8] decided {total_residue} residue pairs across 4 scales, 0 mismatches");
}

#[test]
fn test_exact_points_exactly_on_vertices_and_edges() {
    // Integer-valued coordinates on a large offset: every vertex, every lattice
    // point on an axis-aligned edge and every point on the 3-4-5 diagonal edge
    // is *exactly* on the boundary in f64.
    let cx = 1_048_576.0f64; // 2^20, so cx + small integers are exact
    let cy = 2_097_152.0f64;
    let ring = vec![
        (cx, cy),
        (cx + 400.0, cy),
        (cx + 400.0, cy + 300.0),
        (cx + 100.0, cy + 700.0), // slope -4/3 edge back to (cx, cy + 300)
        (cx, cy + 300.0),
    ];
    let poly = MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![ring.clone()],
        }],
    };

    let mut probes: Vec<(f64, f64)> = Vec::new();
    let mut expect: Vec<u8> = Vec::new();

    // every vertex
    for &(vx, vy) in &ring {
        probes.push((vx, vy));
        expect.push(EXACT_BOUNDARY);
    }
    // lattice points on the four axis-aligned edges
    for i in 1..400 {
        probes.push((cx + i as f64, cy));
        expect.push(EXACT_BOUNDARY);
        probes.push((cx + 400.0, cy + (i % 300) as f64));
        expect.push(EXACT_BOUNDARY);
    }
    for i in 1..300 {
        probes.push((cx, cy + i as f64));
        expect.push(EXACT_BOUNDARY);
    }
    // points on the exact diagonal (cx + 400, cy + 300) -> (cx + 100, cy + 700):
    // direction (-3, +4); parameterise in steps of (−3, +4)
    for k in 1..100 {
        probes.push((cx + 400.0 - 3.0 * k as f64, cy + 300.0 + 4.0 * k as f64));
        expect.push(EXACT_BOUNDARY);
    }
    // interior and exterior controls
    for i in 1..50 {
        probes.push((cx + 200.0, cy + i as f64));
        expect.push(EXACT_INSIDE);
        probes.push((cx + 500.0, cy + i as f64));
        expect.push(EXACT_OUTSIDE);
    }

    let pairs: Vec<(u32, u32)> = (0..probes.len() as u32).map(|p| (0u32, p)).collect();
    let (poly_wkbs, probe_wkbs) = to_arrays(std::slice::from_ref(&poly), &probes);
    let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
    let (cb, cp): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();

    let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();
    refiner.push_build_exact(&poly_array);
    let (scale, _) = refiner.finish_exact().unwrap();
    let fixed = flatten_probe_exact(&probe_array, scale);
    let states = refiner.refine_exact(&fixed, &cb, &cp).unwrap();

    let mut wrong = 0usize;
    for (k, &want) in expect.iter().enumerate() {
        // cross-check the hand-written expectation against the oracle too
        let (px, py) = probes[k];
        assert_eq!(
            oracle_state(exact_pip_oracle(&poly, px, py)),
            want,
            "hand-written expectation disagrees with oracle at probe {k}"
        );
        if states[k] != want {
            wrong += 1;
            if wrong <= 5 {
                println!(
                    "  on-boundary mismatch probe {k} ({px}, {py}): gpu {} want {}",
                    exact_state_name(states[k]),
                    exact_state_name(want)
                );
            }
        }
    }
    let n_boundary = expect.iter().filter(|&&e| e == EXACT_BOUNDARY).count();
    println!(
        "[EXACT ON-BOUNDARY] probes {} ({} exactly on a vertex or edge) | mismatches {}",
        probes.len(),
        n_boundary,
        wrong
    );
    assert_eq!(wrong, 0);
}

#[test]
fn test_exact_random_realistic() {
    let mut rng = SeededRng::new(20250920);
    for &(cx, cy, r_poly) in &[
        (500_000.0f64, 500_000.0f64, 10_000.0f64),
        (-74.0f64, 40.7f64, 0.5f64),
    ] {
        for &n_verts in &[1_000usize, 10_000] {
            let ring = generate_realistic_coastline_ring(cx, cy, r_poly, n_verts, &mut rng);
            let poly = MultiPolyDef {
                parts: vec![PolyPart { rings: vec![ring] }],
            };
            let probes: Vec<(f64, f64)> = (0..4_000)
                .map(|_| {
                    (
                        cx + (rng.next_f64() * 3.0 - 1.5) * r_poly,
                        cy + (rng.next_f64() * 3.0 - 1.5) * r_poly,
                    )
                })
                .collect();
            let pairs: Vec<(u32, u32)> = (0..probes.len() as u32).map(|p| (0u32, p)).collect();
            let (poly_wkbs, probe_wkbs) = to_arrays(std::slice::from_ref(&poly), &probes);
            let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
            let (cb, cp): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();

            let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
            refiner.push_build(&poly_array).unwrap();
            refiner.finish_building().unwrap();
            refiner.push_build_exact(&poly_array);
            let (scale, _) = refiner.finish_exact().unwrap();
            let fixed = flatten_probe_exact(&probe_array, scale);
            let states = refiner.refine_exact(&fixed, &cb, &cp).unwrap();
            let t = tally_against_oracle(&pairs, &states, std::slice::from_ref(&poly), &probes);
            println!(
                "[EXACT REALISTIC] center ({cx}, {cy}) R {r_poly} n {n_verts} | all {} pairs | Inside {} Outside {} Boundary {} NotDecided {} | mismatches {}",
                pairs.len(),
                t.inside,
                t.outside,
                t.boundary,
                t.not_decided,
                t.mismatches
            );
            assert_eq!(t.mismatches, 0);
            assert_eq!(t.not_decided, 0);
        }
    }
}

#[test]
fn test_exact_vs_production_geos() {
    use geos::{Geom, Geometry};

    let (polys, probes, pairs) = generate_adversarial_suite(500_000.0, 500_000.0, 10_000.0, 7);
    let (poly_wkbs, probe_wkbs) = to_arrays(&polys, &probes);
    let (poly_array, probe_array) = binary_arrays(&poly_wkbs, &probe_wkbs);
    let (cb, cp): (Vec<u32>, Vec<u32>) = pairs.iter().copied().unzip();

    let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();
    refiner.push_build_exact(&poly_array);
    let (scale, _) = refiner.finish_exact().unwrap();
    let fixed = flatten_probe_exact(&probe_array, scale);
    let states = refiner.refine_exact(&fixed, &cb, &cp).unwrap();

    let geoms: Vec<Geometry> = poly_wkbs
        .iter()
        .map(|w| Geometry::new_from_wkb(w).expect("geos wkb"))
        .collect();
    let prepared: Vec<_> = geoms
        .iter()
        .map(|g| g.to_prepared_geom().expect("geos prepare"))
        .collect();

    let mut mismatches = 0usize;
    let mut counts = [0usize; 3];
    for (k, &(b, p)) in pairs.iter().enumerate() {
        let (px, py) = probes[p as usize];
        let pg = &prepared[b as usize];
        let inside = pg.contains_xy(px, py).unwrap();
        let touching = pg.intersects_xy(px, py).unwrap();
        let want = if inside {
            EXACT_INSIDE
        } else if touching {
            EXACT_BOUNDARY
        } else {
            EXACT_OUTSIDE
        };
        counts[match want {
            EXACT_INSIDE => 0,
            EXACT_BOUNDARY => 1,
            _ => 2,
        }] += 1;
        if states[k] != want {
            mismatches += 1;
            if mismatches <= 5 {
                println!(
                    "  GEOS mismatch poly {b} probe {p} ({px:e}, {py:e}): gpu {} geos {}",
                    exact_state_name(states[k]),
                    exact_state_name(want)
                );
            }
        }
    }
    println!(
        "[EXACT vs GEOS {}] pairs {} | GEOS says Inside {} Boundary {} Outside {} | mismatches {}",
        geos::version().unwrap_or_default(),
        pairs.len(),
        counts[0],
        counts[1],
        counts[2],
        mismatches
    );
    assert_eq!(
        mismatches, 0,
        "exact GPU pass disagreed with production GEOS"
    );
}

#[test]
fn test_exact_degenerate_inputs_and_not_decided() {
    // (a) invalid / NaN / empty build rows and probe rows must come back as
    //     NotDecided rather than as a wrong definite answer.
    let valid = make_multipoly_wkb(&MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]],
        }],
    });
    let nan_poly = make_multipoly_wkb(&MultiPolyDef {
        parts: vec![PolyPart {
            rings: vec![vec![
                (f64::NAN, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
            ]],
        }],
    });
    let empty_wkb = vec![1u8, 3, 0, 0, 0, 0, 0, 0, 0];
    let degen = make_degenerate_poly_wkb(2);
    let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(valid.as_slice()),
        Some(nan_poly.as_slice()),
        Some(empty_wkb.as_slice()),
        Some(degen.as_slice()),
        None,
    ]));

    let p_ok = make_point_wkb(5.0, 5.0);
    let p_nan = make_point_wkb(f64::NAN, 5.0);
    let p_inf = make_point_wkb(f64::INFINITY, 5.0);
    let p_tiny = make_point_wkb(1e-300, 5.0);
    let probe_array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(p_ok.as_slice()),
        Some(p_nan.as_slice()),
        Some(p_inf.as_slice()),
        Some(p_tiny.as_slice()),
    ]));

    let cb = vec![0u32, 1, 2, 3, 4, 0, 0, 0, 99];
    let cp = vec![0u32, 0, 0, 0, 0, 1, 2, 3, 0];

    let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
    refiner.push_build(&poly_array).unwrap();
    refiner.finish_building().unwrap();
    refiner.push_build_exact(&poly_array);
    let (scale, not_ok) = refiner.finish_exact().unwrap();
    let fixed = flatten_probe_exact(&probe_array, scale);
    let states = refiner.refine_exact(&fixed, &cb, &cp).unwrap();

    println!(
        "[EXACT DEGENERATE] scale 2^{scale} | polygons flagged not-representable {not_ok} | states {:?}",
        states
            .iter()
            .map(|&s| exact_state_name(s))
            .collect::<Vec<_>>()
    );
    assert_eq!(states[0], EXACT_INSIDE, "valid interior pair");
    for (k, &state) in states.iter().enumerate().skip(1) {
        assert_eq!(
            state,
            EXACT_NOT_DECIDED,
            "degenerate pair {k} must be NotDecided, got {}",
            exact_state_name(state)
        );
    }

    // (b) a coordinate far below the frame's LSB is refused rather than rounded.
    // scale here is 2^(exp(10) - 122) = 2^-119, so 1e-300 cannot be represented.
    assert!(f64_to_fixed(1e-300, scale).is_none());
    assert!(f64_to_fixed(1e300, scale).is_none());
    assert_eq!(f64_to_fixed(0.0, scale), Some(0));
}

#[test]
fn test_exact_fixed_point_roundtrip() {
    // f64_to_fixed must be exact or refuse; it must never round.
    let mut rng = SeededRng::new(5150);
    let scale = -60i32;
    let mut exact = 0usize;
    let mut refused = 0usize;
    for _ in 0..200_000 {
        let v = (rng.next_f64() * 2.0 - 1.0) * 1e6;
        match f64_to_fixed(v, scale) {
            Some(i) => {
                // reconstruct: i * 2^scale must equal v bit for bit
                let back = (i as f64) * 2f64.powi(scale);
                assert_eq!(back.to_bits(), v.to_bits(), "roundtrip failed for {v:e}");
                exact += 1;
            }
            None => refused += 1,
        }
    }
    println!(
        "[EXACT FIXED-POINT] 200000 samples at scale 2^{scale}: {exact} exact, {refused} refused"
    );
    assert_eq!(refused, 0, "all 1e6-scale doubles should fit at 2^-60");
}

/// Boundary-heavy probe generator: lands points inside the f32 kernel's
/// uncertainty band around random edge points, which is what drives the
/// uncertain rate to the 15-40% regime.
fn generate_boundary_heavy_probes(
    ring: &[(f64, f64)],
    n: usize,
    r_poly: f64,
    rng: &mut SeededRng,
) -> Vec<(f64, f64)> {
    let eta_k = 4.0 * 5.9604645e-8 * r_poly;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let i = (rng.next_u64() as usize) % ring.len();
        let (ax, ay) = ring[i];
        let (bx, by) = ring[(i + 1) % ring.len()];
        let t = rng.next_f64();
        let (mx, my) = (ax + t * (bx - ax), ay + t * (by - ay));
        let (dx, dy) = (bx - ax, by - ay);
        let len = (dx * dx + dy * dy).sqrt();
        if len == 0.0 {
            out.push((mx, my));
            continue;
        }
        // Offsets span several multiples of the certified band so that only a
        // fraction of the probes end up genuinely uncertain. The kernel's
        // effective half-width is roughly 10x this eta_k estimate, so a +/-30x
        // spread lands about a quarter of the probes in the uncertain band.
        let k = (rng.next_f64() * 2.0 - 1.0) * 30.0;
        out.push((mx + k * eta_k * (-dy / len), my + k * eta_k * (dx / len)));
    }
    out
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[test]
#[ignore]
fn test_exact_timing_matrix() {
    use geos::{Geom, Geometry};

    const RUNS: usize = 11;
    const WARMUP: usize = 3;
    let num_probes = 5_000usize;
    let (cx, cy, r_poly) = (500_000.0f64, 500_000.0f64, 10_000.0f64);
    let mut rng = SeededRng::new(31337);

    println!("\n{:=<168}", "");
    println!(
        " EXACT GPU SECOND PASS vs CPU: {num_probes} candidate pairs per run, median of {} timed runs after {WARMUP} warmups, Apple M4",
        RUNS - WARMUP
    );
    println!(
        " f32-all: f32 kernel over all pairs. f32-res: same f32 kernel re-dispatched over only the residue (isolates dispatch shape from exact arithmetic)."
    );
    println!(
        " geos-res: GEOS prepared contains_xy over the residue. geos-all: GEOS prepared over all pairs (CPU-only baseline, no GPU at all)."
    );
    println!("{:=<168}", "");
    println!(
        "{:<12} | {:<8} | {:>7} | {:>6} | {:>8} | {:>8} | {:>8} | {:>8} | {:>9} | {:>9} | {:>9} | {:>9} | {:>8}",
        "workload",
        "vertices",
        "uncert",
        "rate%",
        "f32-all",
        "f32-res",
        "prep",
        "exact",
        "gpu total",
        "geos-res",
        "gpu+geos",
        "geos-all",
        "build"
    );
    println!("{:-<168}", "");

    for &n_verts in &[1_000usize, 10_000, 100_000] {
        let ring = generate_realistic_coastline_ring(cx, cy, r_poly, n_verts, &mut rng);
        let poly = MultiPolyDef {
            parts: vec![PolyPart {
                rings: vec![ring.clone()],
            }],
        };
        let poly_wkb = make_multipoly_wkb(&poly);
        let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

        let realistic: Vec<(f64, f64)> = (0..num_probes)
            .map(|_| {
                (
                    cx + (rng.next_f64() * 3.0 - 1.5) * r_poly,
                    cy + (rng.next_f64() * 3.0 - 1.5) * r_poly,
                )
            })
            .collect();
        let adversarial = generate_boundary_heavy_probes(&ring, num_probes, r_poly, &mut rng);

        let geos_poly = Geometry::new_from_wkb(&poly_wkb).unwrap();
        let t_prep_geos = Instant::now();
        let geos_prepared = geos_poly.to_prepared_geom().unwrap();
        // force the prepared index to be materialised
        let _ = geos_prepared.contains_xy(cx, cy).unwrap();
        let geos_prepare_ms = t_prep_geos.elapsed().as_secs_f64() * 1000.0;

        for &(workload, mode, probes) in &[
            ("realistic", 0i32, &realistic),
            ("adversarial", 0i32, &adversarial),
            ("forced(m4)", 4i32, &realistic),
        ] {
            let probe_wkbs: Vec<Vec<u8>> =
                probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
            let probe_slices: Vec<Option<&[u8]>> =
                probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
            let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));
            let cb = vec![0u32; num_probes];
            let cp: Vec<u32> = (0..num_probes as u32).collect();

            let mut refiner = MetalSpatialRefiner::try_new_with_mode(mode).unwrap();
            refiner.push_build(&poly_array).unwrap();
            refiner.finish_building().unwrap();
            let t_be = Instant::now();
            refiner.push_build_exact(&poly_array);
            let (scale, _) = refiner.finish_exact().unwrap();
            let build_exact_ms = t_be.elapsed().as_secs_f64() * 1000.0;

            let mut f32_ms = Vec::new();
            let mut f32res_ms = Vec::new();
            let mut prep_ms = Vec::new();
            let mut exact_ms = Vec::new();
            let mut geos_ms = Vec::new();
            let mut geosall_ms = Vec::new();
            let mut n_unc = 0usize;

            for run in 0..RUNS {
                let (mut vb, mut vp, mut ub, mut up) = (vec![], vec![], vec![], vec![]);
                let t0 = Instant::now();
                refiner
                    .refine(
                        &probe_array,
                        ContainerSide::Build,
                        &cb,
                        &cp,
                        &mut vb,
                        &mut vp,
                        &mut ub,
                        &mut up,
                    )
                    .unwrap();
                let dt_f32 = t0.elapsed().as_secs_f64() * 1000.0;

                let t1 = Instant::now();
                let dense: Vec<FixedProbe> = up
                    .iter()
                    .map(|&p| {
                        let (x, y) = probes[p as usize];
                        fixed_probe_from_coord(x, y, scale)
                    })
                    .collect();
                let dense_idx: Vec<u32> = (0..dense.len() as u32).collect();
                let dt_prep = t1.elapsed().as_secs_f64() * 1000.0;

                let t2 = Instant::now();
                let _states = refiner.refine_exact(&dense, &ub, &dense_idx).unwrap();
                let dt_exact = t2.elapsed().as_secs_f64() * 1000.0;

                // Same dispatch shape, f32 kernel: isolates "few threads over a
                // long ring" from "128-bit arithmetic".
                let (mut xb, mut xp, mut yb, mut yp) = (vec![], vec![], vec![], vec![]);
                let t2b = Instant::now();
                refiner
                    .refine(
                        &probe_array,
                        ContainerSide::Build,
                        &ub,
                        &up,
                        &mut xb,
                        &mut xp,
                        &mut yb,
                        &mut yp,
                    )
                    .unwrap();
                let dt_f32res = t2b.elapsed().as_secs_f64() * 1000.0;

                let t3 = Instant::now();
                let mut acc = 0usize;
                for &p in up.iter() {
                    let (x, y) = probes[p as usize];
                    if geos_prepared.contains_xy(x, y).unwrap() {
                        acc += 1;
                    }
                }
                let dt_geos = t3.elapsed().as_secs_f64() * 1000.0;

                let t4 = Instant::now();
                for &(x, y) in probes.iter() {
                    if geos_prepared.contains_xy(x, y).unwrap() {
                        acc += 1;
                    }
                }
                let dt_geos_all = t4.elapsed().as_secs_f64() * 1000.0;
                std::hint::black_box(acc);

                if run >= WARMUP {
                    f32_ms.push(dt_f32);
                    f32res_ms.push(dt_f32res);
                    prep_ms.push(dt_prep);
                    exact_ms.push(dt_exact);
                    geos_ms.push(dt_geos);
                    geosall_ms.push(dt_geos_all);
                }
                n_unc = ub.len();
            }

            let m_f32 = median(f32_ms);
            let m_f32res = median(f32res_ms);
            let m_prep = median(prep_ms);
            let m_exact = median(exact_ms);
            let m_geos = median(geos_ms);
            let m_geos_all = median(geosall_ms);
            println!(
                "{:<12} | {:<8} | {:>7} | {:>5.2}% | {:>8.3} | {:>8.3} | {:>8.3} | {:>8.3} | {:>9.3} | {:>9.3} | {:>9.3} | {:>9.3} | {:>8.2}",
                workload,
                n_verts,
                n_unc,
                100.0 * n_unc as f64 / num_probes as f64,
                m_f32,
                m_f32res,
                m_prep,
                m_exact,
                m_f32 + m_prep + m_exact,
                m_geos,
                m_f32 + m_geos,
                m_geos_all,
                build_exact_ms
            );
        }
        println!(
            "{:-<152}   (GEOS prepare for {n_verts} vertices: {geos_prepare_ms:.2} ms, one-time)",
            ""
        );
    }
}

/// Join-shaped workload: many polygons, many probes, so that even a small
/// uncertain fraction still fills the GPU. This is the configuration in which
/// an on-GPU exact pass has any chance of beating the CPU hop.
#[test]
#[ignore]
fn test_exact_join_shaped_benchmark() {
    use geos::{Geom, Geometry};

    const RUNS: usize = 11;
    const WARMUP: usize = 3;
    let mut rng = SeededRng::new(987_654);

    println!("\n{:=<150}", "");
    println!(" JOIN-SHAPED WORKLOAD: many polygons x many probes (one candidate pair per probe)");
    println!("{:=<150}", "");
    println!(
        "{:<8} | {:<8} | {:<8} | {:<12} | {:>7} | {:>6} | {:>9} | {:>8} | {:>8} | {:>10} | {:>9} | {:>9}",
        "polys",
        "verts",
        "probes",
        "workload",
        "uncert",
        "rate%",
        "f32-all",
        "prep",
        "exact",
        "gpu total",
        "geos-res",
        "geos-all"
    );
    println!("{:-<150}", "");

    for &(n_polys, n_verts, n_probes) in
        &[(500usize, 500usize, 100_000usize), (2_000, 200, 200_000)]
    {
        let mut polys = Vec::with_capacity(n_polys);
        let mut rings = Vec::with_capacity(n_polys);
        for i in 0..n_polys {
            let cx = 400_000.0 + (i % 50) as f64 * 5_000.0;
            let cy = 400_000.0 + (i / 50) as f64 * 5_000.0;
            let ring = generate_realistic_coastline_ring(cx, cy, 2_000.0, n_verts, &mut rng);
            rings.push((cx, cy, ring.clone()));
            polys.push(MultiPolyDef {
                parts: vec![PolyPart { rings: vec![ring] }],
            });
        }
        let poly_wkbs: Vec<Vec<u8>> = polys.iter().map(make_multipoly_wkb).collect();
        let poly_slices: Vec<Option<&[u8]>> =
            poly_wkbs.iter().map(|w| Some(w.as_slice())).collect();
        let poly_array: ArrayRef = Arc::new(BinaryArray::from(poly_slices));

        let geoms: Vec<Geometry> = poly_wkbs
            .iter()
            .map(|w| Geometry::new_from_wkb(w).unwrap())
            .collect();
        let prepared: Vec<_> = geoms
            .iter()
            .map(|g| g.to_prepared_geom().unwrap())
            .collect();
        for (i, p) in prepared.iter().enumerate() {
            let _ = p.contains_xy(rings[i].0, rings[i].1).unwrap();
        }

        for workload in ["realistic", "adversarial"] {
            let mut probe_coords = Vec::with_capacity(n_probes);
            let mut assign = Vec::with_capacity(n_probes);
            for _ in 0..n_probes {
                let pi = (rng.next_u64() as usize) % n_polys;
                let (cx, cy, ref ring) = rings[pi];
                let p = if workload == "realistic" {
                    (
                        cx + (rng.next_f64() * 6.0 - 3.0) * 1_000.0,
                        cy + (rng.next_f64() * 6.0 - 3.0) * 1_000.0,
                    )
                } else {
                    generate_boundary_heavy_probes(ring, 1, 2_000.0, &mut rng)[0]
                };
                probe_coords.push(p);
                assign.push(pi as u32);
            }
            let probe_wkbs: Vec<Vec<u8>> = probe_coords
                .iter()
                .map(|&(x, y)| make_point_wkb(x, y))
                .collect();
            let probe_slices: Vec<Option<&[u8]>> =
                probe_wkbs.iter().map(|w| Some(w.as_slice())).collect();
            let probe_array: ArrayRef = Arc::new(BinaryArray::from(probe_slices));
            let cp: Vec<u32> = (0..n_probes as u32).collect();

            let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
            refiner.push_build(&poly_array).unwrap();
            refiner.finish_building().unwrap();
            refiner.push_build_exact(&poly_array);
            let (scale, _) = refiner.finish_exact().unwrap();

            let (mut f32_ms, mut prep_ms, mut exact_ms) = (vec![], vec![], vec![]);
            let (mut geos_ms, mut geosall_ms) = (vec![], vec![]);
            let mut n_unc = 0usize;

            for run in 0..RUNS {
                let (mut vb, mut vp, mut ub, mut up) = (vec![], vec![], vec![], vec![]);
                let t0 = Instant::now();
                refiner
                    .refine(
                        &probe_array,
                        ContainerSide::Build,
                        &assign,
                        &cp,
                        &mut vb,
                        &mut vp,
                        &mut ub,
                        &mut up,
                    )
                    .unwrap();
                let dt_f32 = t0.elapsed().as_secs_f64() * 1000.0;

                let t1 = Instant::now();
                let dense: Vec<FixedProbe> = up
                    .iter()
                    .map(|&p| {
                        let (x, y) = probe_coords[p as usize];
                        fixed_probe_from_coord(x, y, scale)
                    })
                    .collect();
                let dense_idx: Vec<u32> = (0..dense.len() as u32).collect();
                let dt_prep = t1.elapsed().as_secs_f64() * 1000.0;

                let t2 = Instant::now();
                let _ = refiner.refine_exact(&dense, &ub, &dense_idx).unwrap();
                let dt_exact = t2.elapsed().as_secs_f64() * 1000.0;

                let t3 = Instant::now();
                let mut acc = 0usize;
                for (bi, &p) in ub.iter().zip(up.iter()) {
                    let (x, y) = probe_coords[p as usize];
                    if prepared[*bi as usize].contains_xy(x, y).unwrap() {
                        acc += 1;
                    }
                }
                let dt_geos = t3.elapsed().as_secs_f64() * 1000.0;

                let t4 = Instant::now();
                for (k, &(x, y)) in probe_coords.iter().enumerate() {
                    if prepared[assign[k] as usize].contains_xy(x, y).unwrap() {
                        acc += 1;
                    }
                }
                let dt_geos_all = t4.elapsed().as_secs_f64() * 1000.0;
                std::hint::black_box(acc);

                if run >= WARMUP {
                    f32_ms.push(dt_f32);
                    prep_ms.push(dt_prep);
                    exact_ms.push(dt_exact);
                    geos_ms.push(dt_geos);
                    geosall_ms.push(dt_geos_all);
                }
                n_unc = ub.len();
            }

            let (m_f32, m_prep, m_exact) = (median(f32_ms), median(prep_ms), median(exact_ms));
            println!(
                "{:<8} | {:<8} | {:<8} | {:<12} | {:>7} | {:>5.2}% | {:>9.3} | {:>8.3} | {:>8.3} | {:>10.3} | {:>9.3} | {:>9.3}",
                n_polys,
                n_verts,
                n_probes,
                workload,
                n_unc,
                100.0 * n_unc as f64 / n_probes as f64,
                m_f32,
                m_prep,
                m_exact,
                m_f32 + m_prep + m_exact,
                median(geos_ms),
                median(geosall_ms)
            );
        }
        println!("{:-<150}", "");
    }
}

#[test]
#[ignore]
fn test_exact_memory_report() {
    let mut rng = SeededRng::new(4242);
    println!("\n{:-<110}", "");
    println!(
        "{:<12} | {:>14} | {:>14} | {:>14} | {:>12} | {:>12}",
        "vertices", "f32 bytes", "exact bytes", "total bytes", "B/vtx f32", "B/vtx exact"
    );
    println!("{:-<110}", "");
    for &n_verts in &[1_000usize, 10_000, 100_000] {
        let ring =
            generate_realistic_coastline_ring(500_000.0, 500_000.0, 10_000.0, n_verts, &mut rng);
        let poly = MultiPolyDef {
            parts: vec![PolyPart { rings: vec![ring] }],
        };
        let poly_wkb = make_multipoly_wkb(&poly);
        let poly_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(poly_wkb.as_slice())]));

        let mut refiner = MetalSpatialRefiner::try_new_with_mode(0).unwrap();
        refiner.push_build(&poly_array).unwrap();
        refiner.finish_building().unwrap();
        refiner.push_build_exact(&poly_array);
        refiner.finish_exact().unwrap();

        let f32_bytes = refiner.get_memory_usage();
        let exact_bytes = refiner.exact_memory_usage();
        println!(
            "{:<12} | {:>14} | {:>14} | {:>14} | {:>12.2} | {:>12.2}",
            n_verts,
            f32_bytes,
            exact_bytes,
            f32_bytes + exact_bytes,
            f32_bytes as f64 / n_verts as f64,
            exact_bytes as f64 / n_verts as f64
        );
    }
    println!("{:-<110}", "");
    println!(
        "probe buffer: DecomposedPoint {} B/point (f32 pass), FixedProbe {} B/point (exact pass)",
        std::mem::size_of::<sedona_metalspatial::flattener::DecomposedPoint>(),
        std::mem::size_of::<FixedProbe>()
    );
}
