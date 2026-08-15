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

//! Minimal legacy pcap (`.pcap`) file writer, primarily used to generate
//! test fixtures

use std::io::{self, Write};

const MAGIC_MICROSECONDS: u32 = 0xa1b2_c3d4;
const VERSION_MAJOR: u16 = 2;
const VERSION_MINOR: u16 = 4;
const SNAPLEN: u32 = 65535;

/// Writes frames to `writer` in the legacy pcap format with microsecond
/// timestamp precision
pub struct PcapWriter<W: Write> {
    writer: W,
}

impl<W: Write> PcapWriter<W> {
    /// Creates a writer and writes the pcap global header. `link_type` is a
    /// value from the pcap link type registry (see [`crate::decode::link_type`])
    pub fn new(mut writer: W, link_type: u32) -> io::Result<Self> {
        writer.write_all(&MAGIC_MICROSECONDS.to_le_bytes())?;
        writer.write_all(&VERSION_MAJOR.to_le_bytes())?;
        writer.write_all(&VERSION_MINOR.to_le_bytes())?;
        // thiszone and sigfigs, both unused
        writer.write_all(&0i32.to_le_bytes())?;
        writer.write_all(&0u32.to_le_bytes())?;
        writer.write_all(&SNAPLEN.to_le_bytes())?;
        writer.write_all(&link_type.to_le_bytes())?;
        Ok(Self { writer })
    }

    /// Writes a single un-truncated frame with the given capture timestamp
    pub fn write_packet(&mut self, ts_micros: i64, data: &[u8]) -> io::Result<()> {
        let ts_sec = (ts_micros / 1_000_000) as u32;
        let ts_usec = (ts_micros % 1_000_000) as u32;
        let len = data.len() as u32;
        self.writer.write_all(&ts_sec.to_le_bytes())?;
        self.writer.write_all(&ts_usec.to_le_bytes())?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(data)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
