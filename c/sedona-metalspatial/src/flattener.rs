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

use arrow_array::{Array, ArrayRef, BinaryArray, BinaryViewArray, LargeBinaryArray};
use byteorder::{BigEndian, ByteOrder, LittleEndian};

pub const STATE_OUTSIDE: u32 = 0;
pub const STATE_INSIDE: u32 = 1;
pub const STATE_UNCERTAIN: u32 = 2;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point2D {
    pub x: f32,
    pub y: f32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PolygonRecord {
    pub min_x: f32,
    pub min_y: f32,
    pub max_x: f32,
    pub max_y: f32,
    pub origin_hi_x: f32,
    pub origin_hi_y: f32,
    pub origin_lo_x: f32,
    pub origin_lo_y: f32,
    pub eta_poly: f32,
    pub part_start: u32,
    pub part_count: u32,
    pub is_valid: u32,
}

impl Default for PolygonRecord {
    fn default() -> Self {
        Self {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 0.0,
            max_y: 0.0,
            origin_hi_x: 0.0,
            origin_hi_y: 0.0,
            origin_lo_x: 0.0,
            origin_lo_y: 0.0,
            eta_poly: 0.0,
            part_start: 0,
            part_count: 0,
            is_valid: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PartRecord {
    pub ring_start: u32,
    pub ring_count: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RingRecord {
    pub vertex_start: u32,
    pub vertex_count: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecomposedPoint {
    pub hi_x: f32,
    pub hi_y: f32,
    pub lo_x: f32,
    pub lo_y: f32,
    pub is_valid: u32,
    pub _padding: u32,
}

impl Default for DecomposedPoint {
    fn default() -> Self {
        Self {
            hi_x: 0.0,
            hi_y: 0.0,
            lo_x: 0.0,
            lo_y: 0.0,
            is_valid: 0,
            _padding: 0,
        }
    }
}

/// Decompose an f64 into an f32 high and f32 low component.
/// x_hi = fl32(x)
/// x_lo = fl32(x - x_hi)
/// Error bounded by 2^-48 * |x|.
#[inline(always)]
pub fn decompose_f64(v: f64) -> (f32, f32) {
    let hi = v as f32;
    let lo = (v - (hi as f64)) as f32;
    (hi, lo)
}

/// Compute conservative eta_poly according to approved Section 2.3 & 2.4.
/// eta_poly = nextafter(2*u*R_poly + 2^-48*||o||_inf, +inf)
/// where u = 2^-24, 2^-48 is decomposition roundoff bound.
#[inline(always)]
pub fn compute_eta_poly(ox: f64, oy: f64, r_poly: f64) -> f32 {
    let u = 5.9604644775390625e-8f64; // 2^-24
    let two_neg_48 = 3.552713678800501e-15f64; // 2^-48
    let norm_o = ox.abs().max(oy.abs());
    let eta_f64 = 2.0 * u * r_poly + two_neg_48 * norm_o;
    let val_f32 = eta_f64 as f32;
    let rounded = if (val_f32 as f64) < eta_f64 {
        val_f32.next_up()
    } else {
        val_f32
    };
    rounded.next_up()
}

/// Helper to extract WKB bytes slice from an Arrow ArrayRef.
pub fn extract_wkb_slice<'a>(array: &'a ArrayRef, row: usize) -> Option<&'a [u8]> {
    if array.is_null(row) {
        return None;
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        Some(arr.value(row))
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        Some(arr.value(row))
    } else if let Some(arr) = array.as_any().downcast_ref::<BinaryViewArray>() {
        Some(arr.value(row))
    } else {
        None
    }
}

/// Internal representation of parsed raw coordinates before flattening.
#[derive(Debug)]
pub struct RawRing {
    pub vertices: Vec<(f64, f64)>,
}

#[derive(Debug)]
pub struct RawPart {
    pub rings: Vec<RawRing>,
}

#[derive(Debug)]
pub struct RawPolygon {
    pub parts: Vec<RawPart>,
}

/// Parse WKB point into (f64, f64).
/// Supports both little-endian and big-endian, and EWKB SRID flag.
pub fn parse_wkb_point(buf: &[u8]) -> Option<(f64, f64)> {
    if buf.len() < 5 {
        return None;
    }
    let is_le = match buf[0] {
        1 => true,
        0 => false,
        _ => return None,
    };
    let raw_type = if is_le {
        LittleEndian::read_u32(&buf[1..5])
    } else {
        BigEndian::read_u32(&buf[1..5])
    };

    let has_srid = (raw_type & 0x20000000) != 0;
    let base_type = raw_type & 0x1FFFFFFF;

    // Type 1 is Point, Type 1001 is PointZ, Type 2001 is PointM, Type 3001 is PointZM
    if base_type != 1 && base_type != 1001 && base_type != 2001 && base_type != 3001 {
        return None;
    }

    let coord_offset = if has_srid { 9 } else { 5 };
    if buf.len() < coord_offset + 16 {
        return None;
    }

    let x = if is_le {
        LittleEndian::read_f64(&buf[coord_offset..coord_offset + 8])
    } else {
        BigEndian::read_f64(&buf[coord_offset..coord_offset + 8])
    };
    let y = if is_le {
        LittleEndian::read_f64(&buf[coord_offset + 8..coord_offset + 16])
    } else {
        BigEndian::read_f64(&buf[coord_offset + 8..coord_offset + 16])
    };

    if x.is_nan() || y.is_nan() || x.is_infinite() || y.is_infinite() {
        return None;
    }

    Some((x, y))
}

/// Parse WKB Polygon or MultiPolygon into RawPolygon.
pub fn parse_wkb_polygon(buf: &[u8]) -> Option<RawPolygon> {
    if buf.len() < 5 {
        return None;
    }
    let mut cursor = 0;
    let is_le = match buf[cursor] {
        1 => true,
        0 => false,
        _ => return None,
    };
    cursor += 1;

    let raw_type = if is_le {
        LittleEndian::read_u32(&buf[cursor..cursor + 4])
    } else {
        BigEndian::read_u32(&buf[cursor..cursor + 4])
    };
    cursor += 4;

    let has_srid = (raw_type & 0x20000000) != 0;
    if has_srid {
        if buf.len() < cursor + 4 {
            return None;
        }
        cursor += 4;
    }

    let base_type = raw_type & 0x1FFFFFFF;

    if base_type == 3 || base_type == 1003 || base_type == 2003 || base_type == 3003 {
        // Single Polygon
        let stride = match base_type {
            1003 | 2003 => 24, // X, Y, Z or M
            3003 => 32,        // X, Y, Z, M
            _ => 16,           // X, Y
        };
        let part = parse_wkb_polygon_part(buf, &mut cursor, is_le, stride)?;
        Some(RawPolygon { parts: vec![part] })
    } else if base_type == 6 || base_type == 1006 || base_type == 2006 || base_type == 3006 {
        // MultiPolygon
        if buf.len() < cursor + 4 {
            return None;
        }
        let num_polys = if is_le {
            LittleEndian::read_u32(&buf[cursor..cursor + 4])
        } else {
            BigEndian::read_u32(&buf[cursor..cursor + 4])
        } as usize;
        cursor += 4;

        let mut parts = Vec::with_capacity(num_polys);
        for _ in 0..num_polys {
            if buf.len() < cursor + 5 {
                return None;
            }
            let p_le = match buf[cursor] {
                1 => true,
                0 => false,
                _ => return None,
            };
            cursor += 1;
            let p_type = if p_le {
                LittleEndian::read_u32(&buf[cursor..cursor + 4])
            } else {
                BigEndian::read_u32(&buf[cursor..cursor + 4])
            };
            cursor += 4;
            let p_base = p_type & 0x1FFFFFFF;
            if p_base != 3 && p_base != 1003 && p_base != 2003 && p_base != 3003 {
                return None;
            }
            let stride = match p_base {
                1003 | 2003 => 24,
                3003 => 32,
                _ => 16,
            };
            let part = parse_wkb_polygon_part(buf, &mut cursor, p_le, stride)?;
            parts.push(part);
        }
        Some(RawPolygon { parts })
    } else {
        None
    }
}

fn parse_wkb_polygon_part(
    buf: &[u8],
    cursor: &mut usize,
    is_le: bool,
    stride: usize,
) -> Option<RawPart> {
    if buf.len() < *cursor + 4 {
        return None;
    }
    let num_rings = if is_le {
        LittleEndian::read_u32(&buf[*cursor..*cursor + 4])
    } else {
        BigEndian::read_u32(&buf[*cursor..*cursor + 4])
    } as usize;
    *cursor += 4;

    let mut rings = Vec::with_capacity(num_rings);
    for _ in 0..num_rings {
        if buf.len() < *cursor + 4 {
            return None;
        }
        let num_points = if is_le {
            LittleEndian::read_u32(&buf[*cursor..*cursor + 4])
        } else {
            BigEndian::read_u32(&buf[*cursor..*cursor + 4])
        } as usize;
        *cursor += 4;

        if buf.len() < *cursor + num_points * stride {
            return None;
        }

        let mut vertices = Vec::with_capacity(num_points);
        for _ in 0..num_points {
            let x = if is_le {
                LittleEndian::read_f64(&buf[*cursor..*cursor + 8])
            } else {
                BigEndian::read_f64(&buf[*cursor..*cursor + 8])
            };
            let y = if is_le {
                LittleEndian::read_f64(&buf[*cursor + 8..*cursor + 16])
            } else {
                BigEndian::read_f64(&buf[*cursor + 8..*cursor + 16])
            };
            *cursor += stride;

            if x.is_nan() || y.is_nan() || x.is_infinite() || y.is_infinite() {
                return None;
            }
            vertices.push((x, y));
        }

        // If closed, retain vertices without duplicate closing vertex if num_points > 1
        if vertices.len() > 1 && vertices.first() == vertices.last() {
            vertices.pop();
        }

        if vertices.len() < 3 {
            return None;
        }

        rings.push(RawRing { vertices });
    }

    Some(RawPart { rings })
}

/// Flattens a batch of build polygon geometries into GPU-ready buffers.
pub fn flatten_build_polygons(
    array: &ArrayRef,
) -> (
    Vec<PolygonRecord>,
    Vec<PartRecord>,
    Vec<RingRecord>,
    Vec<Point2D>,
) {
    let num_rows = array.len();
    let mut poly_records = Vec::with_capacity(num_rows);
    let mut part_records = Vec::new();
    let mut ring_records = Vec::new();
    let mut vertex_array = Vec::new();

    for row in 0..num_rows {
        let wkb_bytes = match extract_wkb_slice(array, row) {
            Some(bytes) => bytes,
            None => {
                poly_records.push(PolygonRecord::default());
                continue;
            }
        };

        let raw_poly = match parse_wkb_polygon(wkb_bytes) {
            Some(p) if !p.parts.is_empty() => p,
            _ => {
                poly_records.push(PolygonRecord::default());
                continue;
            }
        };

        // Compute global bounding box across all parts
        let mut min_x = f64::INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        let mut total_verts = 0;

        for part in &raw_poly.parts {
            for ring in &part.rings {
                for &(vx, vy) in &ring.vertices {
                    min_x = min_x.min(vx);
                    min_y = min_y.min(vy);
                    max_x = max_x.max(vx);
                    max_y = max_y.max(vy);
                    total_verts += 1;
                }
            }
        }

        if total_verts < 3 || min_x > max_x || min_y > max_y {
            poly_records.push(PolygonRecord::default());
            continue;
        }

        // Bounding box center as origin
        let ox = (min_x + max_x) * 0.5;
        let oy = (min_y + max_y) * 0.5;

        // Radius R_poly = max_i ||v_i - o||_inf
        let mut r_poly = 0.0f64;
        for part in &raw_poly.parts {
            for ring in &part.rings {
                for &(vx, vy) in &ring.vertices {
                    let dx = (vx - ox).abs();
                    let dy = (vy - oy).abs();
                    r_poly = r_poly.max(dx.max(dy));
                }
            }
        }

        let eta_poly = compute_eta_poly(ox, oy, r_poly);
        let (o_hi_x, o_lo_x) = decompose_f64(ox);
        let (o_hi_y, o_lo_y) = decompose_f64(oy);

        let part_start = part_records.len() as u32;
        let part_count = raw_poly.parts.len() as u32;

        for part in raw_poly.parts {
            let ring_start = ring_records.len() as u32;
            let ring_count = part.rings.len() as u32;

            for ring in part.rings {
                let vertex_start = vertex_array.len() as u32;
                let vertex_count = ring.vertices.len() as u32;

                for (vx, vy) in ring.vertices {
                    let rx = (vx - ox) as f32;
                    let ry = (vy - oy) as f32;
                    vertex_array.push(Point2D { x: rx, y: ry });
                }

                ring_records.push(RingRecord {
                    vertex_start,
                    vertex_count,
                });
            }

            part_records.push(PartRecord {
                ring_start,
                ring_count,
            });
        }

        poly_records.push(PolygonRecord {
            min_x: min_x as f32,
            min_y: min_y as f32,
            max_x: max_x as f32,
            max_y: max_y as f32,
            origin_hi_x: o_hi_x,
            origin_hi_y: o_hi_y,
            origin_lo_x: o_lo_x,
            origin_lo_y: o_lo_y,
            eta_poly,
            part_start,
            part_count,
            is_valid: 1,
        });
    }

    (poly_records, part_records, ring_records, vertex_array)
}

/// Flattens a batch of probe geometries into DecomposedPoint buffer.
pub fn flatten_probe_points(array: &ArrayRef) -> Vec<DecomposedPoint> {
    let num_rows = array.len();
    let mut probe_points = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let wkb_bytes = match extract_wkb_slice(array, row) {
            Some(bytes) => bytes,
            None => {
                probe_points.push(DecomposedPoint::default());
                continue;
            }
        };

        match parse_wkb_point(wkb_bytes) {
            Some((x, y)) => {
                let (hi_x, lo_x) = decompose_f64(x);
                let (hi_y, lo_y) = decompose_f64(y);
                probe_points.push(DecomposedPoint {
                    hi_x,
                    hi_y,
                    lo_x,
                    lo_y,
                    is_valid: 1,
                    _padding: 0,
                });
            }
            None => {
                probe_points.push(DecomposedPoint::default());
            }
        }
    }

    probe_points
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::BinaryArray;
    use std::sync::Arc;

    fn make_point_wkb(x: f64, y: f64) -> Vec<u8> {
        let mut buf = vec![1u8]; // Little endian
        buf.extend_from_slice(&1u32.to_le_bytes()); // Point
        buf.extend_from_slice(&x.to_le_bytes());
        buf.extend_from_slice(&y.to_le_bytes());
        buf
    }

    fn make_poly_wkb(rings: &[&[(f64, f64)]]) -> Vec<u8> {
        let mut buf = vec![1u8]; // Little endian
        buf.extend_from_slice(&3u32.to_le_bytes()); // Polygon
        buf.extend_from_slice(&(rings.len() as u32).to_le_bytes());
        for ring in rings {
            buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
            for &(x, y) in *ring {
                buf.extend_from_slice(&x.to_le_bytes());
                buf.extend_from_slice(&y.to_le_bytes());
            }
        }
        buf
    }

    #[test]
    fn test_hi_lo_decomposition_precision() {
        let coords = [10.5f64, 1234567.89012345, -987654.32109876, 1e7 + 0.123456];
        for &c in &coords {
            let (hi, lo) = decompose_f64(c);
            let reconstructed = (hi as f64) + (lo as f64);
            let err = (c - reconstructed).abs();
            let bound = 2.0f64.powi(-48) * c.abs();
            assert!(err <= bound + 1e-16, "Error {err} exceeded bound {bound} for {c}");
        }
    }

    #[test]
    fn test_eta_poly_rounded_up() {
        let ox = 500000.0f64;
        let oy = 600000.0f64;
        let r_poly = 1000.0f64;
        let eta = compute_eta_poly(ox, oy, r_poly);

        let u = 5.9604644775390625e-8f64;
        let two_neg_48 = 3.552713678800501e-15f64;
        let exact_eta = 2.0 * u * r_poly + two_neg_48 * 600000.0f64;

        assert!((eta as f64) >= exact_eta, "eta {eta} must be >= exact {exact_eta}");
        assert!(eta > 0.0);
    }

    #[test]
    fn test_flatten_valid_and_invalid_geometries() {
        let p1 = make_poly_wkb(&[&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]]);
        let corrupt = vec![1u8, 3, 0, 0, 0]; // truncated
        let empty = vec![1u8, 3, 0, 0, 0, 0, 0, 0, 0]; // 0 rings
        let nan_poly = make_poly_wkb(&[&[
            (f64::NAN, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 0.0),
        ]]);

        let array: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(p1.as_slice()),
            Some(corrupt.as_slice()),
            None,
            Some(empty.as_slice()),
            Some(nan_poly.as_slice()),
        ]));

        let (polys, parts, rings, verts) = flatten_build_polygons(&array);
        assert_eq!(polys.len(), 5);
        assert_eq!(polys[0].is_valid, 1);
        assert_eq!(polys[0].part_count, 1);
        assert_eq!(polys[1].is_valid, 0);
        assert_eq!(polys[2].is_valid, 0);
        assert_eq!(polys[3].is_valid, 0);
        assert_eq!(polys[4].is_valid, 0);

        assert_eq!(parts.len(), 1);
        assert_eq!(rings.len(), 1);
        assert_eq!(verts.len(), 4); // 4 vertices (closing vertex deduplicated)
    }

    #[test]
    fn test_flatten_probe_points() {
        let pt1 = make_point_wkb(5.0, 5.0);
        let pt_nan = make_point_wkb(f64::NAN, 1.0);
        let not_pt = make_poly_wkb(&[&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]);

        let array: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(pt1.as_slice()),
            Some(pt_nan.as_slice()),
            None,
            Some(not_pt.as_slice()),
        ]));

        let probes = flatten_probe_points(&array);
        assert_eq!(probes.len(), 4);
        assert_eq!(probes[0].is_valid, 1);
        assert_eq!(probes[0].hi_x, 5.0);
        assert_eq!(probes[1].is_valid, 0);
        assert_eq!(probes[2].is_valid, 0);
        assert_eq!(probes[3].is_valid, 0);
    }
}
