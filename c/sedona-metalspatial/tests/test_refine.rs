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
use sedona_metalspatial::{ContainerSide, MetalSpatialRefiner, RefinerIndexStats};
use std::collections::HashSet;
use std::sync::Arc;

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

// ============================================================================
// Ring slab edge index gates: the indexed scan must reproduce the linear scan
// state-for-state (same kernel, same per-edge body, different edge subset).
// ============================================================================

/// Mirrors `kIndexMinVertices` in spatial_refiner.mm. The threshold gate below fails if
/// the two drift apart.
const INDEX_MIN_VERTICES: usize = 64;
/// Mirrors `kEdgesPerSlab` / `kMaxSlabs` in spatial_refiner.mm (only used to aim probes).
const INDEX_EDGES_PER_SLAB: usize = 16;
const INDEX_MAX_SLABS: usize = 32768;

fn to_binary_array(wkbs: &[Vec<u8>]) -> ArrayRef {
    let slices: Vec<Option<&[u8]>> = wkbs.iter().map(|w| Some(w.as_slice())).collect();
    Arc::new(BinaryArray::from(slices))
}

/// Runs one refiner configuration and returns the per-pair state array
/// (0 outside, 1 inside, 2 uncertain) in candidate order, plus the path counters.
fn refine_states(
    poly_array: &ArrayRef,
    probe_array: &ArrayRef,
    cand_b: &[u32],
    cand_p: &[u32],
    bound_mode: i32,
    index_mode: i32,
    pad_scale: f32,
) -> (Vec<u8>, RefinerIndexStats, f64) {
    let mut refiner = MetalSpatialRefiner::try_new_with_mode(bound_mode).unwrap();
    refiner.set_index_mode(index_mode).unwrap();
    refiner.set_index_pad_scale(pad_scale);
    refiner.push_build(poly_array).unwrap();
    refiner.finish_building().unwrap();

    let (mut vb, mut vp, mut ub, mut up) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let t0 = std::time::Instant::now();
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
    let secs = t0.elapsed().as_secs_f64();

    // refine() emits verified and uncertain pairs as order-preserving subsequences of the
    // candidates, so a two-cursor walk recovers the exact state array.
    let mut states = Vec::with_capacity(cand_b.len());
    let (mut iv, mut iu) = (0usize, 0usize);
    for i in 0..cand_b.len() {
        let pair = (cand_b[i], cand_p[i]);
        if iv < vb.len() && (vb[iv], vp[iv]) == pair {
            states.push(1u8);
            iv += 1;
        } else if iu < ub.len() && (ub[iu], up[iu]) == pair {
            states.push(2u8);
            iu += 1;
        } else {
            states.push(0u8);
        }
    }
    assert_eq!(iv, vb.len(), "verified pairs not an ordered subsequence");
    assert_eq!(iu, ub.len(), "uncertain pairs not an ordered subsequence");
    (states, refiner.index_stats(), secs)
}

#[derive(Default)]
struct IndexGateTotals {
    pairs: u64,
    mismatches: u64,
    rings_indexed_y: u64,
    rings_indexed_x: u64,
    pairs_x_indexed: u64,
    pairs_y_ray: u64,
    pairs_y_indexed: u64,
    pairs_pad_fallback: u64,
    uncertain: u64,
}

/// Compares index modes 1 (y-slabs) and 2 (y+x slabs) against mode 0 (linear) for all
/// polygons x probes. Returns the mode-2 stats.
fn assert_index_matches_linear(
    label: &str,
    polys: &[MultiPolyDef],
    probes: &[(f64, f64)],
    bound_mode: i32,
    totals: &mut IndexGateTotals,
) -> RefinerIndexStats {
    let poly_wkbs: Vec<Vec<u8>> = polys.iter().map(make_multipoly_wkb).collect();
    let poly_array = to_binary_array(&poly_wkbs);
    let probe_wkbs: Vec<Vec<u8>> = probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_array = to_binary_array(&probe_wkbs);

    let mut cand_b = Vec::with_capacity(polys.len() * probes.len());
    let mut cand_p = Vec::with_capacity(polys.len() * probes.len());
    for b in 0..polys.len() as u32 {
        for p in 0..probes.len() as u32 {
            cand_b.push(b);
            cand_p.push(p);
        }
    }

    let (linear, stats0, _) = refine_states(
        &poly_array,
        &probe_array,
        &cand_b,
        &cand_p,
        bound_mode,
        0,
        1.0,
    );
    assert_eq!(stats0.rings_indexed_y + stats0.rings_indexed_x, 0);
    assert_eq!(stats0.pairs_x_indexed + stats0.pairs_y_indexed, 0);

    let mut stats2 = RefinerIndexStats::default();
    for index_mode in [1, 2] {
        let (indexed, stats, _) = refine_states(
            &poly_array,
            &probe_array,
            &cand_b,
            &cand_p,
            bound_mode,
            index_mode,
            1.0,
        );
        let mismatches = linear.iter().zip(&indexed).filter(|(a, b)| a != b).count() as u64;
        if let Some(i) = (0..linear.len()).find(|&i| linear[i] != indexed[i]) {
            println!(
                "[INDEX GATE] {label}: first mismatch poly {} probe {:?}: linear {} vs indexed {}",
                cand_b[i], probes[cand_p[i] as usize], linear[i], indexed[i]
            );
        }
        totals.pairs += linear.len() as u64;
        totals.mismatches += mismatches;
        assert_eq!(
            mismatches, 0,
            "{label}: index mode {index_mode} (bound mode {bound_mode}) differs from linear"
        );
        if index_mode == 2 {
            stats2 = stats;
        }
    }
    totals.rings_indexed_y += stats2.rings_indexed_y;
    totals.rings_indexed_x += stats2.rings_indexed_x;
    totals.pairs_x_indexed += stats2.pairs_x_indexed;
    totals.pairs_y_ray += stats2.pairs_y_ray;
    totals.pairs_y_indexed += stats2.pairs_y_indexed;
    totals.pairs_pad_fallback += stats2.pairs_pad_fallback;
    totals.uncertain += linear.iter().filter(|&&s| s == 2).count() as u64;
    println!(
        "[INDEX GATE] {label}: bound_mode={bound_mode} pairs={} rings_y={} rings_x={} x_indexed={} y_ray={} y_indexed={} pad_fallback={} mismatches=0",
        linear.len(),
        stats2.rings_indexed_y,
        stats2.rings_indexed_x,
        stats2.pairs_x_indexed,
        stats2.pairs_y_ray,
        stats2.pairs_y_indexed,
        stats2.pairs_pad_fallback
    );
    stats2
}

fn f32_ulp(v: f32) -> f64 {
    let a = v.abs().max(f32::MIN_POSITIVE);
    (a.next_up() - a) as f64
}

/// Test-side mirror of the slab grid that plan_axis()/polygon_pad() in spatial_refiner.mm
/// build for the first ring of a polygon. Only used to AIM probes and vertices; if the
/// builder halves K for its memory budget, its boundaries remain a subset of these.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MirrorGrid {
    origin: f64,
    lo: f32,
    hi: f32,
    cmin: f32,
    cmax: f32,
    inv_h: f32,
    k: usize,
}

impl MirrorGrid {
    fn new(poly: &MultiPolyDef, use_y: bool) -> Self {
        let arr = to_binary_array(&[make_multipoly_wkb(poly)]);
        let (recs, _parts, rings, verts) =
            sedona_metalspatial::flattener::flatten_build_polygons(&arr);
        let p = &recs[0];
        let ring = &rings[0];
        let n = ring.vertex_count as usize;
        let vs = &verts[ring.vertex_start as usize..ring.vertex_start as usize + n];

        let u = 2f64.powi(-24);
        let ox = p.origin_hi_x as f64 + p.origin_lo_x as f64;
        let oy = p.origin_hi_y as f64 + p.origin_lo_y as f64;
        let d = (p.min_x as f64 - ox)
            .abs()
            .max((p.max_x as f64 - ox).abs())
            .max((p.min_y as f64 - oy).abs())
            .max((p.max_y as f64 - oy).abs());
        let pn = (p
            .min_x
            .abs()
            .max(p.max_x.abs())
            .max(p.min_y.abs())
            .max(p.max_y.abs())) as f64;
        let eta = p.eta_poly as f64
            + 3.0 * u * (d + p.eta_poly as f64)
            + 2f64.powi(-48) * (pn + p.eta_poly as f64);
        let pad = (4.0 * eta) as f32;

        let coords: Vec<f32> = vs.iter().map(|v| if use_y { v.y } else { v.x }).collect();
        let cmin = coords.iter().copied().fold(f32::INFINITY, f32::min);
        let cmax = coords.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let lo_d = cmin as f64 - pad as f64;
        let hi_d = cmax as f64 + pad as f64;
        let mut lo = lo_d as f32;
        if (lo as f64) > lo_d {
            lo = lo.next_down();
        }
        let mut hi = hi_d as f32;
        if (hi as f64) < hi_d {
            hi = hi.next_up();
        }
        let k = n.div_ceil(INDEX_EDGES_PER_SLAB).min(INDEX_MAX_SLABS);
        let inv_h = (k as f64 / (hi as f64 - lo as f64)) as f32;
        Self {
            origin: if use_y { oy } else { ox },
            lo,
            hi,
            cmin,
            cmax,
            inv_h,
            k,
        }
    }

    /// The kernel's slab function (select_slab in refine.metal), in f32.
    fn slab(&self, d: f32) -> usize {
        let t = ((d - self.lo) * self.inv_h).min((self.k - 1) as f32);
        t as usize
    }

    /// Smallest f32 that the kernel maps to slab `s` (1 <= s < k).
    fn first_float_in_slab(&self, s: usize) -> f32 {
        let mut x = (self.lo as f64 + s as f64 / self.inv_h as f64) as f32;
        while self.slab(x) >= s {
            x = x.next_down();
        }
        while self.slab(x) < s {
            x = x.next_up();
        }
        x
    }
}

/// Absolute coordinates on the slab boundaries of the first ring of `poly`, perturbed by
/// -2..=2 f32 ulps of the origin-relative value the kernel sees, plus the padded extent
/// ends (empty-range branch on one side, first/last slab on the other).
fn slab_boundary_coords(poly: &MultiPolyDef, use_y: bool, max_samples: usize) -> Vec<f64> {
    let g = MirrorGrid::new(poly, use_y);
    let step = (g.k / max_samples).max(1);
    let mut slabs: Vec<usize> = (1..g.k).step_by(step).collect();
    slabs.extend([1, 2, g.k - 1]);
    let mut out = Vec::new();
    let mut push_around = |v: f32| {
        for ulps in -2i32..=2 {
            out.push(g.origin + v as f64 + ulps as f64 * f32_ulp(v));
        }
    };
    for s in slabs {
        if s >= 1 && s < g.k {
            push_around(g.first_float_in_slab(s));
        }
    }
    for v in [g.lo, g.hi, g.cmin, g.cmax] {
        push_around(v);
    }
    out
}

/// Probe set for one polygon aimed at everything the slab lookup can get wrong.
fn index_gate_probes(poly: &MultiPolyDef, n_random: usize, rng: &mut SeededRng) -> Vec<(f64, f64)> {
    let ring = &poly.parts[0].rings[0];
    let n = ring.len();
    let (mut min_x, mut min_y) = (f64::INFINITY, f64::INFINITY);
    let (mut max_x, mut max_y) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    let (mut imin_y, mut imax_y) = (0usize, 0usize);
    for (i, &(x, y)) in ring.iter().enumerate() {
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        if y < min_y {
            min_y = y;
            imin_y = i;
        }
        if y > max_y {
            max_y = y;
            imax_y = i;
        }
    }
    let (w, h) = (max_x - min_x, max_y - min_y);
    let r = w.max(h);
    let mut probes = Vec::new();

    // (1) Random probes over the (slightly enlarged) bbox.
    for _ in 0..n_random {
        probes.push((
            min_x - 0.1 * w + rng.next_f64() * 1.2 * w,
            min_y - 0.1 * h + rng.next_f64() * 1.2 * h,
        ));
    }

    // (2) Vertex-exact and boundary-exact probes; rays through vertex levels force the
    // +x trap and therefore the +y retry.
    let step = (n / 96).max(1);
    for i in (0..n).step_by(step) {
        let (ax, ay) = ring[i];
        let (bx, by) = ring[(i + 1) % n];
        probes.push((ax, ay));
        probes.push((0.5 * (ax + bx), 0.5 * (ay + by)));
        for &dx in &[-0.75 * r, -1e-3 * r, 1e-3 * r, 0.25 * r] {
            probes.push((ax + dx, ay));
            probes.push((ax, ay + dx));
        }
    }

    // (3) Probes exactly at the ring's ymin / ymax (and a few ulps around), left of,
    // at and right of the extreme vertex.
    for &i in &[imin_y, imax_y] {
        let (vx, vy) = ring[i];
        let ulp32 = f32_ulp(r as f32).max(f32_ulp(vy as f32));
        for k in -3i32..=3 {
            for &x in &[
                min_x - 0.05 * w,
                vx - 1e-3 * w,
                vx,
                vx + 1e-3 * w,
                max_x + 0.05 * w,
            ] {
                probes.push((x, vy + k as f64 * ulp32));
                probes.push((x, vy + k as f64 * vy.abs().max(1.0) * f64::EPSILON));
            }
        }
    }

    // (4) y exactly on y-slab boundaries (+x ray lookup).
    for y in slab_boundary_coords(poly, true, 48) {
        for &x in &[
            min_x - 0.05 * w,
            min_x + 0.37 * w,
            min_x + rng.next_f64() * w,
        ] {
            probes.push((x, y));
        }
    }

    // (5) x exactly on x-slab boundaries with y on a vertex level, so that the +x ray is
    // trapped and the +y retry performs the x-slab lookup.
    let xs = slab_boundary_coords(poly, false, 48);
    for (j, &x) in xs.iter().enumerate() {
        let (_, vy) = ring[(j * 7919) % n];
        probes.push((x, vy));
        probes.push((x, min_y + rng.next_f64() * h));
    }
    probes
}

fn single_ring_poly(ring: Vec<(f64, f64)>) -> MultiPolyDef {
    MultiPolyDef {
        parts: vec![PolyPart { rings: vec![ring] }],
    }
}

/// Rectilinear comb: every edge is exactly horizontal or vertical, many share y levels.
fn generate_comb_ring(cx: f64, cy: f64, r: f64, teeth: usize) -> Vec<(f64, f64)> {
    let mut ring = vec![(cx - r, cy - r), (cx + r, cy - r)];
    let pitch = 2.0 * r / (2 * teeth) as f64;
    for t in 0..teeth {
        let x_hi = cx + r - (2 * t) as f64 * pitch;
        let top = if t % 2 == 0 { cy + r } else { cy + 0.5 * r };
        ring.push((x_hi, top));
        ring.push((x_hi - pitch, top));
        ring.push((x_hi - pitch, cy));
        ring.push((x_hi - 2.0 * pitch, cy));
    }
    ring.pop();
    ring.push((cx - r, cy));
    ring
}

/// Square with `m` outward spikes per side. Spike tips are local extrema in y (bottom/top
/// sides) or x (left/right sides); one tip per side reaches the bbox so the extent is fixed.
/// `tip(side, j)` gives the tip's distance from the centre along the spike axis.
#[allow(clippy::type_complexity)]
fn generate_spiked_square(
    cx: f64,
    cy: f64,
    r: f64,
    m: usize,
    tip: &dyn Fn(usize, usize) -> f64,
) -> (Vec<(f64, f64)>, Vec<(usize, usize)>) {
    let base = 0.6 * r;
    let pitch = 2.0 * base / m as f64;
    let mut ring = Vec::new();
    let mut tips = Vec::new(); // (vertex index, side)
    for side in 0..4 {
        for j in 0..m {
            let a = -base + j as f64 * pitch; // along-side coordinate of the base vertex
            let (bx, by, tx, ty) = match side {
                0 => (a, -base, a + 0.5 * pitch, -tip(0, j)), // bottom, left -> right
                1 => (base, a, tip(1, j), a + 0.5 * pitch),   // right, bottom -> top
                2 => (-a, base, -a - 0.5 * pitch, tip(2, j)), // top, right -> left
                _ => (-base, -a, -tip(3, j), -a - 0.5 * pitch), // left, top -> bottom
            };
            ring.push((cx + bx, cy + by));
            tips.push((ring.len(), side));
            ring.push((cx + tx, cy + ty));
        }
    }
    (ring, tips)
}

/// Adversarial gate for the pad rule: spike tips are placed 2 ulps inside a slab, right at
/// its boundary, and probes sit a few ulps across the boundary in the neighbouring slab.
/// The linear scan traps those probes in the eta_k band of the tip; an index that files the
/// tip's edges only under the slab they geometrically occupy (no pad) misses the trap.
/// Bound mode 4 (single ray, no +y retry) exposes the +x ray's own state.
///
/// The thin variants (one axis squeezed 1000x, 8192 vertices) make a slab so short that
/// the builder's 1/32-slab slack is smaller than eta_k, so the probes placed at
/// 0.55..1.0 x (4 u R) across the boundary are covered by the pad alone.
#[test]
fn test_index_gate_vertices_on_slab_boundaries() {
    let mut totals = IndexGateTotals::default();
    for &(cx, cy, r, m, sx, sy, d0, d1) in &[
        (
            500_000.0, 500_000.0, 10_000.0, 64usize, 1.0, 1.0, 0.65, 0.95,
        ),
        (-74.0, 40.7, 0.5, 64, 1.0, 1.0, 0.65, 0.95),
        (0.0, 0.0, 1.0, 64, 1.0, 1.0, 0.65, 0.95),
        (
            500_000.0, 500_000.0, 10_000.0, 2048, 1.0, 1e-3, 0.602, 0.616,
        ),
        (-74.0, 40.7, 0.5, 2048, 1e-3, 1.0, 0.602, 0.616),
    ] {
        let mut rng = SeededRng::new(4242);
        let depths: Vec<f64> = (0..4 * m)
            .map(|_| (d0 + (d1 - d0) * rng.next_f64()) * r)
            .collect();
        let prelim = |side: usize, j: usize| if j == m / 2 { r } else { depths[side * m + j] };
        let (mut ring0, tips) = generate_spiked_square(cx, cy, r, m, &prelim);
        for v in ring0.iter_mut() {
            *v = (cx + (v.0 - cx) * sx, cy + (v.1 - cy) * sy);
        }
        let poly0 = single_ring_poly(ring0.clone());
        let gy = MirrorGrid::new(&poly0, true);
        let gx = MirrorGrid::new(&poly0, false);

        // Snap every non-extreme tip to sit 2 ulps inside its slab, at the boundary that
        // faces away from the polygon body.
        let mut ring = ring0.clone();
        let mut snapped = Vec::new(); // (vertex index, side, local f32 coordinate)
        for &(vi, side) in &tips {
            let (g, abs) = if side % 2 == 0 {
                (&gy, ring[vi].1)
            } else {
                (&gx, ring[vi].0)
            };
            let local = (abs - g.origin) as f32;
            if local <= g.cmin || local >= g.cmax {
                continue; // extent-defining tip
            }
            let s = g.slab(local);
            if s == 0 || s + 1 >= g.k {
                continue;
            }
            // Bottom (0) / left (3) tips are local minima: lowest floats of slab s.
            // Top (2) / right (1) tips are local maxima: highest floats of slab s.
            // On the thin variants every other tip sits 0.07 slab inside instead: beyond the
            // builder's 1/32-slab slack (even if it halved K once), so only the pad can
            // file its edges under the neighbouring slab.
            let thin = sx < 1.0 || sy < 1.0;
            let inset = if thin && (vi / 2) % 2 == 1 {
                0.07 / g.inv_h
            } else {
                0.0
            };
            let c = if side == 0 || side == 3 {
                g.first_float_in_slab(s).next_up().next_up() + inset
            } else {
                g.first_float_in_slab(s + 1)
                    .next_down()
                    .next_down()
                    .next_down()
                    - inset
            };
            if side % 2 == 0 {
                ring[vi].1 = g.origin + c as f64;
            } else {
                ring[vi].0 = g.origin + c as f64;
            }
            snapped.push((vi, side, c));
        }
        let poly = single_ring_poly(ring.clone());
        assert_eq!(
            MirrorGrid::new(&poly, true),
            gy,
            "snapping moved the y grid"
        );
        assert_eq!(
            MirrorGrid::new(&poly, false),
            gx,
            "snapping moved the x grid"
        );
        assert!(snapped.len() > 2 * m, "too few snapped tips");

        let mut probes = Vec::new();
        for &(vi, side, c) in &snapped {
            let (vx, vy) = ring[vi];
            let g = if side % 2 == 0 { &gy } else { &gx };
            let sign = if side == 0 || side == 3 { -1.0 } else { 1.0 };
            // k ulps, then fractions of the eta_k lower bound 4 u R, across the slab
            // boundary, away from the tip.
            let eta_lb = 4.0 * 2f64.powi(-24) * r;
            let mut offsets: Vec<f64> = (1..=8).map(|k| k as f64 * f32_ulp(c)).collect();
            offsets.extend([0.55, 0.7, 0.85, 1.0].map(|f| f * eta_lb));
            for off in offsets {
                let across = g.origin + c as f64 + sign * off;
                if side % 2 == 0 {
                    // +x ray: left of the tip (edge not culled) and directly below/above it.
                    probes.push((vx - 0.01 * r, across));
                    probes.push((vx, across));
                    probes.push((cx - 1.05 * r, across));
                } else {
                    // +y retry: level with the tip traps the +x ray, then the x lookup runs.
                    probes.push((across, vy));
                    probes.push((across, vy - 0.01 * r));
                }
            }
        }

        for bound_mode in [0, 4] {
            let stats = assert_index_matches_linear(
                &format!("slab-boundary tips c=({cx},{cy}) R={r} m={m} scale=({sx},{sy})"),
                std::slice::from_ref(&poly),
                &probes,
                bound_mode,
                &mut totals,
            );
            assert_eq!((stats.rings_indexed_y, stats.rings_indexed_x), (1, 1));
            assert!(stats.pairs_x_indexed > 0);
        }
    }
    assert!(totals.uncertain > 0, "no probe landed in an eta_k band");
    assert!(totals.pairs_y_indexed > 0, "indexed +y retry not reached");
    println!(
        "[INDEX GATE TOTAL] slab-boundary tips: pairs={} mismatches={} uncertain={} y_indexed={}",
        totals.pairs, totals.mismatches, totals.uncertain, totals.pairs_y_indexed
    );
}

#[test]
fn test_index_gate_adversarial_scale_matrix_matches_linear() {
    // The existing adversarial suite (stars 8/64/512/2000, hole, multipolygon, specialised
    // ring) over the Gate 2 coordinate scale matrix. Stars with >= 64 vertices are indexed.
    let mut totals = IndexGateTotals::default();
    for &cx in &[1e2, 5e5, 1e7, -1e7] {
        for &r in &[1e-3, 1.0, 1e3, 1e5, 1e6] {
            let (polys, probes, _) = generate_adversarial_suite(cx, cx, r, 12345);
            let stats = assert_index_matches_linear(
                &format!("adversarial c={cx:e} R={r:e}"),
                &polys,
                &probes,
                0,
                &mut totals,
            );
            assert_eq!(
                stats.rings_indexed_y, 3,
                "stars 64/512/2000 must be indexed"
            );
        }
    }
    // The shared loop body also serves the diagnostic bound modes.
    for bound_mode in [2, 3, 4, 5] {
        let (polys, probes, _) = generate_adversarial_suite(5e5, 5e5, 1e4, 42);
        assert_index_matches_linear(
            "adversarial modes",
            &polys,
            &probes,
            bound_mode,
            &mut totals,
        );
    }
    println!(
        "[INDEX GATE TOTAL] adversarial: pairs={} mismatches={} x_indexed={} y_ray={} y_indexed={} pad_fallback={} uncertain={}",
        totals.pairs,
        totals.mismatches,
        totals.pairs_x_indexed,
        totals.pairs_y_ray,
        totals.pairs_y_indexed,
        totals.pairs_pad_fallback,
        totals.uncertain
    );
    assert!(totals.pairs_x_indexed > 0 && totals.pairs_y_indexed > 0);
}

#[test]
fn test_index_gate_coastlines_holes_multipolygons_match_linear() {
    let mut rng = SeededRng::new(20260920);
    let mut totals = IndexGateTotals::default();
    let scales = [(500_000.0, 500_000.0, 10_000.0), (-74.0, 40.7, 0.5)];

    for &(cx, cy, r) in &scales {
        for &(n_verts, n_random) in &[
            (1_000usize, 20_000usize),
            (10_000, 20_000),
            (100_000, 6_000),
        ] {
            let ring = generate_realistic_coastline_ring(cx, cy, r, n_verts, &mut rng);
            let poly = single_ring_poly(ring);
            let probes = index_gate_probes(&poly, n_random, &mut rng);
            let stats = assert_index_matches_linear(
                &format!("coastline n={n_verts} c=({cx},{cy})"),
                std::slice::from_ref(&poly),
                &probes,
                0,
                &mut totals,
            );
            assert_eq!((stats.rings_indexed_y, stats.rings_indexed_x), (1, 1));
            assert!(stats.pairs_x_indexed > 0, "indexed +x path not reached");
            assert!(stats.pairs_y_indexed > 0, "indexed +y path not reached");
            assert_eq!(stats.pairs_pad_fallback, 0, "unexpected pad fallback");
        }

        // Holes and multipolygons whose rings are all large enough to be indexed.
        let ext = generate_realistic_coastline_ring(cx, cy, r, 5_000, &mut rng);
        let hole1 = generate_realistic_coastline_ring(cx - 0.2 * r, cy, 0.08 * r, 1_000, &mut rng);
        let hole2 = generate_realistic_coastline_ring(cx + 0.2 * r, cy, 0.08 * r, 300, &mut rng);
        let island =
            generate_realistic_coastline_ring(cx + 4.0 * r, cy + r, 0.7 * r, 2_000, &mut rng);
        let lake = generate_realistic_coastline_ring(cx + 4.0 * r, cy + r, 0.1 * r, 100, &mut rng);
        let multi = MultiPolyDef {
            parts: vec![
                PolyPart {
                    rings: vec![ext, hole1.clone(), hole2],
                },
                PolyPart {
                    rings: vec![island.clone(), lake],
                },
            ],
        };
        let mut probes = index_gate_probes(&multi, 20_000, &mut rng);
        probes.extend(index_gate_probes(&single_ring_poly(hole1), 3_000, &mut rng));
        probes.extend(index_gate_probes(
            &single_ring_poly(island),
            5_000,
            &mut rng,
        ));
        let stats = assert_index_matches_linear(
            &format!("holes+multipolygon c=({cx},{cy})"),
            std::slice::from_ref(&multi),
            &probes,
            0,
            &mut totals,
        );
        assert_eq!((stats.rings_indexed_y, stats.rings_indexed_x), (5, 5));

        // Horizontal / vertical edges only, probes on the shared levels.
        let comb = single_ring_poly(generate_comb_ring(cx, cy, r, 60));
        let mut probes = index_gate_probes(&comb, 10_000, &mut rng);
        for i in 0..400 {
            let x = cx - 1.1 * r + 2.2 * r * (i as f64 / 400.0);
            for &y in &[cy - r, cy, cy + 0.5 * r, cy + r] {
                probes.push((x, y));
            }
        }
        let stats = assert_index_matches_linear(
            &format!("comb c=({cx},{cy})"),
            std::slice::from_ref(&comb),
            &probes,
            0,
            &mut totals,
        );
        assert_eq!(stats.rings_indexed_y, 1);
        assert!(stats.pairs_y_indexed > 0);
    }

    println!(
        "[INDEX GATE TOTAL] coastlines: pairs={} mismatches={} rings_indexed(y/x)={}/{} x_indexed={} y_ray={} y_indexed={} pad_fallback={} uncertain={}",
        totals.pairs,
        totals.mismatches,
        totals.rings_indexed_y,
        totals.rings_indexed_x,
        totals.pairs_x_indexed,
        totals.pairs_y_ray,
        totals.pairs_y_indexed,
        totals.pairs_pad_fallback,
        totals.uncertain
    );
}

#[test]
fn test_index_gate_threshold_and_pad_fallback() {
    let mut rng = SeededRng::new(77);
    let mut totals = IndexGateTotals::default();
    let (cx, cy, r) = (500_000.0, 500_000.0, 10_000.0);

    // Rings just below / at / above the indexing threshold T.
    for (n, expect_indexed) in [
        (INDEX_MIN_VERTICES - 1, 0u64),
        (INDEX_MIN_VERTICES, 1),
        (INDEX_MIN_VERTICES + 1, 1),
    ] {
        let poly = single_ring_poly(generate_realistic_coastline_ring(cx, cy, r, n, &mut rng));
        let probes = index_gate_probes(&poly, 20_000, &mut rng);
        let stats = assert_index_matches_linear(
            &format!("threshold n={n}"),
            std::slice::from_ref(&poly),
            &probes,
            0,
            &mut totals,
        );
        assert_eq!(
            stats.rings_indexed_y, expect_indexed,
            "threshold drifted (n={n})"
        );
        assert_eq!(stats.rings_indexed_x, expect_indexed);
        assert_eq!(stats.pairs_x_indexed > 0, expect_indexed == 1);
    }

    // Pad guard: with the pad scaled far below eta_k the kernel must refuse the index and
    // fall back to the linear scan for every pair that reaches an indexed ring.
    let poly = single_ring_poly(generate_realistic_coastline_ring(
        cx, cy, r, 5_000, &mut rng,
    ));
    let probes = index_gate_probes(&poly, 20_000, &mut rng);
    let poly_array = to_binary_array(&[make_multipoly_wkb(&poly)]);
    let probe_wkbs: Vec<Vec<u8>> = probes.iter().map(|&(x, y)| make_point_wkb(x, y)).collect();
    let probe_array = to_binary_array(&probe_wkbs);
    let cand_b = vec![0u32; probes.len()];
    let cand_p: Vec<u32> = (0..probes.len() as u32).collect();
    let (linear, _, _) = refine_states(&poly_array, &probe_array, &cand_b, &cand_p, 0, 0, 1.0);
    let (small_pad, stats, _) =
        refine_states(&poly_array, &probe_array, &cand_b, &cand_p, 0, 2, 0.01);
    assert_eq!(linear, small_pad, "pad fallback changed states");
    assert_eq!(stats.rings_indexed_y, 1);
    assert!(
        stats.pairs_pad_fallback > 0,
        "pad fallback branch not reached"
    );
    assert_eq!(stats.pairs_x_indexed, 0, "index used although eta_k > pad");
    println!(
        "[INDEX GATE] pad fallback: pairs={} pad_fallback={} x_indexed={}",
        linear.len(),
        stats.pairs_pad_fallback,
        stats.pairs_x_indexed
    );
}

/// Kernel-only microbenchmark: one coastline polygon, random probes, linear vs indexed on
/// the same dispatch path. Timings include probe flattening and buffer upload (identical
/// in all modes). Synthetic workload; not representative of real data.
#[test]
#[ignore]
fn test_index_linear_vs_indexed_microbenchmark() {
    let mut rng = SeededRng::new(99);
    let (cx, cy, r) = (-74.0, 40.7, 0.5);
    let num_probes = 200_000usize;
    let runs = 11;
    println!(
        "\n{:<9} | {:<8} | {:>12} | {:>10} | {:>10} | {:>9} | {:>8} | {:>8}",
        "Vertices", "Index", "median ms", "min ms", "B/vertex", "build ms", "y_ray %", "unc %"
    );
    for &n_verts in &[1_000usize, 10_000, 100_000] {
        let poly = single_ring_poly(generate_realistic_coastline_ring(
            cx, cy, r, n_verts, &mut rng,
        ));
        let poly_array = to_binary_array(&[make_multipoly_wkb(&poly)]);
        let probe_wkbs: Vec<Vec<u8>> = (0..num_probes)
            .map(|_| {
                make_point_wkb(
                    cx - 1.5 * r + rng.next_f64() * 3.0 * r,
                    cy - 1.5 * r + rng.next_f64() * 3.0 * r,
                )
            })
            .collect();
        let probe_array = to_binary_array(&probe_wkbs);
        let cand_b = vec![0u32; num_probes];
        let cand_p: Vec<u32> = (0..num_probes as u32).collect();

        let mut reference: Option<Vec<u8>> = None;
        for (index_mode, name) in [(0, "linear"), (1, "y-slabs"), (2, "y+x")] {
            let mut times = Vec::new();
            let mut last = None;
            for _ in 0..runs {
                let (states, stats, secs) = refine_states(
                    &poly_array,
                    &probe_array,
                    &cand_b,
                    &cand_p,
                    0,
                    index_mode,
                    1.0,
                );
                times.push(secs * 1e3);
                last = Some((states, stats));
            }
            let (states, stats) = last.unwrap();
            match &reference {
                None => reference = Some(states.clone()),
                Some(r) => assert_eq!(r, &states),
            }
            let warm = &mut times[1..];
            warm.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let unc = states.iter().filter(|&&s| s == 2).count();
            println!(
                "{:<9} | {:<8} | {:>12.2} | {:>10.2} | {:>10.2} | {:>9.2} | {:>8.3} | {:>8.3}",
                n_verts,
                name,
                warm[warm.len() / 2],
                warm[0],
                stats.index_bytes as f64 / stats.vertices as f64,
                stats.build_micros as f64 / 1e3,
                100.0 * stats.pairs_y_ray as f64 / stats.pairs as f64,
                100.0 * unc as f64 / num_probes as f64
            );
        }
    }
}
