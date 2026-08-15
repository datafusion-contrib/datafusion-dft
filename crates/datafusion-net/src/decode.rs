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

//! Decodes raw frames into the column values of [`crate::schema::packet_schema`].
//!
//! Decoding never fails: frames that cannot be parsed (unknown link types,
//! truncated packets, non-IP traffic) produce a [`DecodedPacket`] with null
//! (`None`) values for the layers that could not be decoded.

use etherparse::{
    ether_type, EtherType, IpNumber, LinkSlice, NetSlice, SlicedPacket, TransportSlice, VlanSlice,
};

/// Link types (from the pcap/pcapng spec) that we know how to decode
pub mod link_type {
    /// BSD loopback encapsulation: 4 byte protocol family header then IP
    pub const NULL: u32 = 0;
    /// Ethernet
    pub const ETHERNET: u32 = 1;
    /// Raw IP, no link layer header
    pub const RAW: u32 = 101;
    /// OpenBSD loopback encapsulation, same shape as NULL
    pub const LOOP: u32 = 108;
}

/// Column values decoded from a single frame. Fields are `None` when the
/// corresponding layer was absent or could not be parsed. The payload is
/// borrowed from the frame to avoid copying it when the column is not
/// projected.
#[derive(Debug, Default)]
pub struct DecodedPacket<'a> {
    pub eth_src: Option<String>,
    pub eth_dst: Option<String>,
    pub ethertype: Option<String>,
    pub vlan: Option<u16>,
    pub ip_version: Option<u8>,
    pub src_ip: Option<String>,
    pub dst_ip: Option<String>,
    pub ttl: Option<u8>,
    pub protocol: Option<String>,
    pub src_port: Option<u16>,
    pub dst_port: Option<u16>,
    pub tcp_flags: Option<String>,
    pub tcp_seq: Option<u32>,
    pub tcp_ack: Option<u32>,
    pub tcp_window: Option<u16>,
    pub payload: Option<&'a [u8]>,
}

/// Decodes a single frame captured on a link of type `link_type`
pub fn decode_frame(link_type: u32, data: &[u8]) -> DecodedPacket<'_> {
    let sliced = match link_type {
        link_type::ETHERNET => SlicedPacket::from_ethernet(data).ok(),
        link_type::NULL | link_type::LOOP => {
            if data.len() >= 4 {
                SlicedPacket::from_ip(&data[4..]).ok()
            } else {
                None
            }
        }
        link_type::RAW => SlicedPacket::from_ip(data).ok(),
        _ => None,
    };
    let Some(sliced) = sliced else {
        return DecodedPacket::default();
    };
    let mut decoded = DecodedPacket::default();

    if let Some(LinkSlice::Ethernet2(eth)) = &sliced.link {
        decoded.eth_src = Some(format_mac(&eth.source()));
        decoded.eth_dst = Some(format_mac(&eth.destination()));
        decoded.ethertype = Some(ether_type_name(eth.ether_type()));
    }

    match &sliced.vlan() {
        Some(VlanSlice::SingleVlan(v)) => decoded.vlan = Some(v.vlan_identifier().into()),
        Some(VlanSlice::DoubleVlan(v)) => decoded.vlan = Some(v.outer.vlan_identifier().into()),
        None => {}
    }

    let ip_protocol = match &sliced.net {
        Some(NetSlice::Ipv4(v4)) => {
            let header = v4.header();
            decoded.ip_version = Some(4);
            decoded.src_ip = Some(header.source_addr().to_string());
            decoded.dst_ip = Some(header.destination_addr().to_string());
            decoded.ttl = Some(header.ttl());
            decoded.payload = Some(v4.payload().payload);
            Some(header.protocol())
        }
        Some(NetSlice::Ipv6(v6)) => {
            let header = v6.header();
            decoded.ip_version = Some(6);
            decoded.src_ip = Some(header.source_addr().to_string());
            decoded.dst_ip = Some(header.destination_addr().to_string());
            decoded.ttl = Some(header.hop_limit());
            decoded.payload = Some(v6.payload().payload);
            Some(v6.payload().ip_number)
        }
        _ => None,
    };

    match &sliced.transport {
        Some(TransportSlice::Tcp(tcp)) => {
            decoded.protocol = Some("tcp".to_string());
            decoded.src_port = Some(tcp.source_port());
            decoded.dst_port = Some(tcp.destination_port());
            decoded.tcp_flags = Some(tcp_flags_string(tcp));
            decoded.tcp_seq = Some(tcp.sequence_number());
            decoded.tcp_ack = Some(tcp.acknowledgment_number());
            decoded.tcp_window = Some(tcp.window_size());
            decoded.payload = Some(tcp.payload());
        }
        Some(TransportSlice::Udp(udp)) => {
            decoded.protocol = Some("udp".to_string());
            decoded.src_port = Some(udp.source_port());
            decoded.dst_port = Some(udp.destination_port());
            decoded.payload = Some(udp.payload());
        }
        Some(TransportSlice::Icmpv4(icmp)) => {
            decoded.protocol = Some("icmp".to_string());
            decoded.payload = Some(icmp.payload());
        }
        Some(TransportSlice::Icmpv6(icmp)) => {
            decoded.protocol = Some("icmpv6".to_string());
            decoded.payload = Some(icmp.payload());
        }
        None => {
            // Transport layer was not parsed (e.g. an IP protocol etherparse
            // does not decode); fall back to the IP header's protocol number
            decoded.protocol = ip_protocol.map(ip_number_name);
        }
    }

    decoded
}

fn format_mac(mac: &[u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}

fn ether_type_name(ether_type: EtherType) -> String {
    match ether_type {
        ether_type::IPV4 => "ipv4".to_string(),
        ether_type::IPV6 => "ipv6".to_string(),
        ether_type::ARP => "arp".to_string(),
        other => format!("0x{:04x}", other.0),
    }
}

fn ip_number_name(ip_number: IpNumber) -> String {
    match ip_number.keyword_str() {
        Some(keyword) => keyword.to_lowercase(),
        None => ip_number.0.to_string(),
    }
}

fn tcp_flags_string(tcp: &etherparse::TcpSlice) -> String {
    let mut flags = Vec::new();
    if tcp.fin() {
        flags.push("FIN");
    }
    if tcp.syn() {
        flags.push("SYN");
    }
    if tcp.rst() {
        flags.push("RST");
    }
    if tcp.psh() {
        flags.push("PSH");
    }
    if tcp.ack() {
        flags.push("ACK");
    }
    if tcp.urg() {
        flags.push("URG");
    }
    if tcp.ece() {
        flags.push("ECE");
    }
    if tcp.cwr() {
        flags.push("CWR");
    }
    flags.join("|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use etherparse::PacketBuilder;

    const SRC_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];
    const DST_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x02];

    #[test]
    fn test_decode_tcp_ipv4() {
        let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
            .ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
            .tcp(443, 51000, 1000, 1024)
            .syn()
            .ack(7);
        let payload = b"hello";
        let mut frame = Vec::with_capacity(builder.size(payload.len()));
        builder.write(&mut frame, payload).unwrap();

        let decoded = decode_frame(link_type::ETHERNET, &frame);
        assert_eq!(decoded.eth_src.as_deref(), Some("02:00:00:00:00:01"));
        assert_eq!(decoded.eth_dst.as_deref(), Some("02:00:00:00:00:02"));
        assert_eq!(decoded.ethertype.as_deref(), Some("ipv4"));
        assert_eq!(decoded.ip_version, Some(4));
        assert_eq!(decoded.src_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(decoded.dst_ip.as_deref(), Some("10.0.0.2"));
        assert_eq!(decoded.ttl, Some(64));
        assert_eq!(decoded.protocol.as_deref(), Some("tcp"));
        assert_eq!(decoded.src_port, Some(443));
        assert_eq!(decoded.dst_port, Some(51000));
        assert_eq!(decoded.tcp_flags.as_deref(), Some("SYN|ACK"));
        assert_eq!(decoded.tcp_seq, Some(1000));
        assert_eq!(decoded.tcp_ack, Some(7));
        assert_eq!(decoded.tcp_window, Some(1024));
        assert_eq!(decoded.payload, Some(payload.as_slice()));
    }

    #[test]
    fn test_decode_udp_ipv6() {
        let builder = PacketBuilder::ethernet2(SRC_MAC, DST_MAC)
            .ipv6([1u8; 16], [2u8; 16], 32)
            .udp(53, 5353);
        let payload = b"dns";
        let mut frame = Vec::with_capacity(builder.size(payload.len()));
        builder.write(&mut frame, payload).unwrap();

        let decoded = decode_frame(link_type::ETHERNET, &frame);
        assert_eq!(decoded.ethertype.as_deref(), Some("ipv6"));
        assert_eq!(decoded.ip_version, Some(6));
        assert_eq!(decoded.ttl, Some(32));
        assert_eq!(decoded.protocol.as_deref(), Some("udp"));
        assert_eq!(decoded.src_port, Some(53));
        assert_eq!(decoded.dst_port, Some(5353));
        assert_eq!(decoded.tcp_flags, None);
        assert_eq!(decoded.payload, Some(payload.as_slice()));
    }

    #[test]
    fn test_decode_garbage_yields_nulls() {
        let decoded = decode_frame(link_type::ETHERNET, &[0x01, 0x02, 0x03]);
        assert_eq!(decoded.eth_src, None);
        assert_eq!(decoded.src_ip, None);
        assert_eq!(decoded.protocol, None);
        assert_eq!(decoded.payload, None);
    }

    #[test]
    fn test_decode_unknown_link_type_yields_nulls() {
        let decoded = decode_frame(9999, &[0u8; 64]);
        assert_eq!(decoded.eth_src, None);
        assert_eq!(decoded.src_ip, None);
    }

    #[test]
    fn test_decode_raw_ip_link_type() {
        let builder = PacketBuilder::ipv4([192, 168, 1, 1], [192, 168, 1, 2], 64).udp(1000, 2000);
        let mut frame = Vec::with_capacity(builder.size(0));
        builder.write(&mut frame, &[]).unwrap();

        let decoded = decode_frame(link_type::RAW, &frame);
        assert_eq!(decoded.eth_src, None);
        assert_eq!(decoded.src_ip.as_deref(), Some("192.168.1.1"));
        assert_eq!(decoded.protocol.as_deref(), Some("udp"));
    }
}
