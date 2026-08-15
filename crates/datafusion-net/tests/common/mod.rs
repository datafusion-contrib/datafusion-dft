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

//! Shared test support: a minimal MaxMind DB (`.mmdb`) file written from
//! scratch so geolocation tests are offline and carry no data licensing
//! baggage. The database maps the single network `1.1.1.0/24` to a Sydney,
//! Australia city record.
//!
//! The format (<https://maxmind.github.io/MaxMind-DB/>) is a binary search
//! tree over address bits, a 16-zero-byte separator, a data section, and
//! metadata after a marker. Field encoding: a control byte carries the type
//! in the top 3 bits (0 = extended: real type is 7 + the following byte) and
//! the payload size in the low 5 bits.

use std::path::Path;

/// One tree node per bit of the 1.1.1.0/24 prefix
const NODE_COUNT: u32 = 24;
/// The network encoded in the search tree
const PREFIX: [u8; 3] = [1, 1, 1];

fn control(type_num: u8, size: usize) -> u8 {
    assert!(size < 29, "sizes needing extra length bytes not supported");
    (type_num << 5) | size as u8
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    out.push(control(2, s.len()));
    out.extend_from_slice(s.as_bytes());
}

fn write_double(out: &mut Vec<u8>, value: f64) {
    out.push(control(3, 8));
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_map_header(out: &mut Vec<u8>, entries: usize) {
    out.push(control(7, entries));
}

/// Writes a uint16 (type 5) or uint32 (type 6) with minimal-length encoding
fn write_uint(out: &mut Vec<u8>, type_num: u8, value: u32) {
    let bytes = value.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count();
    out.push(control(type_num, bytes.len() - skip));
    out.extend_from_slice(&bytes[skip..]);
}

/// Writes a uint64 (extended type 9)
fn write_uint64(out: &mut Vec<u8>, value: u64) {
    let bytes = value.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count();
    out.push(control(0, bytes.len() - skip));
    out.push(9 - 7);
    out.extend_from_slice(&bytes[skip..]);
}

/// Writes an array (extended type 11) header
fn write_array_header(out: &mut Vec<u8>, entries: usize) {
    out.push(control(0, entries));
    out.push(11 - 7);
}

/// The single city record, at offset 0 of the data section
fn city_record() -> Vec<u8> {
    let mut d = Vec::new();
    write_map_header(&mut d, 3);
    write_string(&mut d, "city");
    write_map_header(&mut d, 1);
    write_string(&mut d, "names");
    write_map_header(&mut d, 1);
    write_string(&mut d, "en");
    write_string(&mut d, "Sydney");
    write_string(&mut d, "country");
    write_map_header(&mut d, 2);
    write_string(&mut d, "iso_code");
    write_string(&mut d, "AU");
    write_string(&mut d, "names");
    write_map_header(&mut d, 1);
    write_string(&mut d, "en");
    write_string(&mut d, "Australia");
    write_string(&mut d, "location");
    write_map_header(&mut d, 3);
    write_string(&mut d, "latitude");
    write_double(&mut d, -33.8688);
    write_string(&mut d, "longitude");
    write_double(&mut d, 151.2093);
    write_string(&mut d, "time_zone");
    write_string(&mut d, "Australia/Sydney");
    d
}

/// A chain of 24 nodes routing the bits of [`PREFIX`]. At each node the
/// on-prefix branch continues to the next node (the final one points at data
/// offset 0, encoded as `NODE_COUNT + 16`); the off-prefix branch is the
/// "no data" sentinel `NODE_COUNT`. Record size 24 bits: each node is two
/// 3-byte big-endian records.
fn search_tree() -> Vec<u8> {
    let mut tree = Vec::new();
    for i in 0..NODE_COUNT as usize {
        let bit = (PREFIX[i / 8] >> (7 - i % 8)) & 1;
        let matched = if i == NODE_COUNT as usize - 1 {
            NODE_COUNT + 16
        } else {
            i as u32 + 1
        };
        let (left, right) = if bit == 0 {
            (matched, NODE_COUNT)
        } else {
            (NODE_COUNT, matched)
        };
        tree.extend_from_slice(&left.to_be_bytes()[1..]);
        tree.extend_from_slice(&right.to_be_bytes()[1..]);
    }
    tree
}

fn metadata() -> Vec<u8> {
    let mut m = Vec::new();
    write_map_header(&mut m, 9);
    write_string(&mut m, "binary_format_major_version");
    write_uint(&mut m, 5, 2);
    write_string(&mut m, "binary_format_minor_version");
    write_uint(&mut m, 5, 0);
    write_string(&mut m, "build_epoch");
    write_uint64(&mut m, 1_735_689_600); // 2025-01-01T00:00:00Z
    write_string(&mut m, "database_type");
    write_string(&mut m, "GeoLite2-City");
    write_string(&mut m, "description");
    write_map_header(&mut m, 1);
    write_string(&mut m, "en");
    write_string(&mut m, "Test database");
    write_string(&mut m, "ip_version");
    write_uint(&mut m, 5, 4);
    write_string(&mut m, "languages");
    write_array_header(&mut m, 1);
    write_string(&mut m, "en");
    write_string(&mut m, "node_count");
    write_uint(&mut m, 6, NODE_COUNT);
    write_string(&mut m, "record_size");
    write_uint(&mut m, 5, 24);
    m
}

/// Writes the test database to `path`. `1.1.1.0/24` resolves to Sydney,
/// Australia (`AU`, -33.8688, 151.2093, `Australia/Sydney`); every other
/// address is absent.
pub fn write_test_mmdb(path: &Path) {
    let mut buf = search_tree();
    buf.extend_from_slice(&[0u8; 16]); // data section separator
    buf.extend_from_slice(&city_record());
    buf.extend_from_slice(b"\xab\xcd\xefMaxMind.com"); // metadata marker
    buf.extend_from_slice(&metadata());
    std::fs::write(path, buf).unwrap();
}
