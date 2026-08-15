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

//! `dns_query` scalar UDF: decodes DNS messages from packet payloads.
//!
//! [`DnsQueryUdf`] takes a binary payload (typically the `payload` column of
//! UDP port 53 packets — DNS over UDP fits in a single datagram, so no
//! reassembly is needed) and returns a struct with `is_response`, `name`,
//! `query_type`, `response_code`, and `answers` fields:
//!
//! ```sql
//! SELECT dns_query(payload)['name'] AS name, count(*) AS queries
//! FROM pcap('capture.pcap')
//! WHERE dst_port = 53
//! GROUP BY name ORDER BY queries DESC;
//! ```
//!
//! `name` and `query_type` come from the first question; `response_code` and
//! `answers` are NULL for queries. Answers include A/AAAA addresses and
//! CNAME/NS/PTR names; other record types are skipped. Payloads that do not
//! parse as DNS yield a NULL struct.

use std::{
    net::{Ipv4Addr, Ipv6Addr},
    sync::{Arc, LazyLock},
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, ListBuilder, NullBufferBuilder, StringBuilder,
            StructArray,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{cast::as_binary_array, exec_err, Result},
    logical_expr::{
        ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
};

use crate::NET_DOC_SECTION;

/// `SHOW FUNCTIONS` documentation for `dns_query`
static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        NET_DOC_SECTION,
        "Decodes a DNS message from a packet payload (typically UDP port 53), \
         returning a struct with is_response (Boolean), name and query_type \
         (from the first question), response_code (NULL for queries), and \
         answers (a list of A/AAAA addresses and CNAME/NS/PTR names, NULL for \
         queries). Payloads that do not parse as DNS yield a NULL struct.",
        "dns_query(payload)",
    )
    .with_argument(
        "payload",
        "Binary packet payload, e.g. the payload column of a DNS packet",
    )
    .with_sql_example(
        "SELECT dns_query(payload)['name'] AS name, count(*) AS queries \
         FROM pcap('capture.pcap') WHERE dst_port = 53 GROUP BY name ORDER BY queries DESC",
    )
    .with_related_udf("tls_sni")
    .build()
});

/// Fields of the struct returned by `dns_query`
fn dns_fields() -> Fields {
    Fields::from(vec![
        Field::new("is_response", DataType::Boolean, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("query_type", DataType::Utf8, true),
        Field::new("response_code", DataType::Utf8, true),
        Field::new(
            "answers",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ])
}

/// Scalar UDF that decodes a DNS message from a binary payload
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DnsQueryUdf {
    signature: Signature,
}

impl Default for DnsQueryUdf {
    fn default() -> Self {
        Self {
            // Pure parse of the input bytes
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::LargeBinary]),
                    TypeSignature::Exact(vec![DataType::BinaryView]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DnsQueryUdf {
    fn name(&self) -> &str {
        "dns_query"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(dns_fields()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("dns_query expects a single binary argument");
        }
        let payloads = to_binary_array(&args.args[0].to_array(args.number_rows)?)?;
        let payloads = as_binary_array(&payloads)?;

        let mut is_response = BooleanBuilder::new();
        let mut name = StringBuilder::new();
        let mut query_type = StringBuilder::new();
        let mut response_code = StringBuilder::new();
        let mut answers = ListBuilder::new(StringBuilder::new());
        let mut validity = NullBufferBuilder::new(payloads.len());

        for row in 0..payloads.len() {
            let message = if payloads.is_null(row) {
                None
            } else {
                parse_dns(payloads.value(row))
            };
            match message {
                Some(message) => {
                    is_response.append_value(message.is_response);
                    name.append_value(&message.name);
                    query_type.append_value(&message.query_type);
                    response_code.append_option(message.response_code.as_deref());
                    match message.answers {
                        Some(values) => {
                            for value in values {
                                answers.values().append_value(value);
                            }
                            answers.append(true);
                        }
                        None => answers.append_null(),
                    }
                    validity.append_non_null();
                }
                None => {
                    is_response.append_null();
                    name.append_null();
                    query_type.append_null();
                    response_code.append_null();
                    answers.append_null();
                    validity.append_null();
                }
            }
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(is_response.finish()),
            Arc::new(name.finish()),
            Arc::new(query_type.finish()),
            Arc::new(response_code.finish()),
            Arc::new(answers.finish()),
        ];
        let structs = StructArray::new(dns_fields(), arrays, validity.finish());
        Ok(ColumnarValue::Array(Arc::new(structs)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

/// Normalizes the input (BinaryView / LargeBinary) to Binary so a single
/// code path handles every accepted binary type
fn to_binary_array(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Binary {
        Ok(Arc::clone(array))
    } else {
        Ok(datafusion::arrow::compute::cast(array, &DataType::Binary)?)
    }
}

/// A decoded DNS message (the parts surfaced by `dns_query`)
#[derive(Debug, PartialEq)]
struct DnsMessage {
    is_response: bool,
    name: String,
    query_type: String,
    /// NULL for queries
    response_code: Option<String>,
    /// NULL for queries; may be empty for answerless responses
    answers: Option<Vec<String>>,
}

fn read_u16(data: &[u8], pos: usize) -> Option<u16> {
    Some(u16::from_be_bytes([*data.get(pos)?, *data.get(pos + 1)?]))
}

/// Parses a DNS message. Returns `None` when the bytes do not look like DNS
/// (this is a heuristic — any bytes *could* be DNS — so malformed names,
/// impossible counts, and unknown opcodes are all rejected).
fn parse_dns(data: &[u8]) -> Option<DnsMessage> {
    if data.len() < 12 {
        return None;
    }
    let flags = read_u16(data, 2)?;
    let opcode = (flags >> 11) & 0xF;
    // QUERY, IQUERY, and STATUS; anything else is likely not DNS
    if opcode > 2 {
        return None;
    }
    let qdcount = read_u16(data, 4)?;
    let ancount = read_u16(data, 6)?;
    if qdcount == 0 || qdcount > 4 || ancount > 128 {
        return None;
    }
    let is_response = flags & 0x8000 != 0;

    let mut pos = 12usize;
    let name = read_name(data, &mut pos)?;
    let qtype = read_u16(data, pos)?;
    pos += 4; // qtype + qclass
    for _ in 1..qdcount {
        read_name(data, &mut pos)?;
        pos += 4;
    }
    if pos > data.len() {
        return None;
    }

    let (response_code, answers) = if is_response {
        let rcode = flags & 0xF;
        let mut answers = Vec::new();
        for _ in 0..ancount {
            read_name(data, &mut pos)?; // owner name
            let rtype = read_u16(data, pos)?;
            pos += 8; // type + class + ttl
            let rdlen = read_u16(data, pos)? as usize;
            pos += 2;
            let rdata = data.get(pos..pos + rdlen)?;
            match rtype {
                // A
                1 if rdlen == 4 => {
                    let octets: [u8; 4] = rdata.try_into().ok()?;
                    answers.push(Ipv4Addr::from(octets).to_string());
                }
                // AAAA
                28 if rdlen == 16 => {
                    let octets: [u8; 16] = rdata.try_into().ok()?;
                    answers.push(Ipv6Addr::from(octets).to_string());
                }
                // NS, CNAME, PTR: rdata is a (possibly compressed) name
                2 | 5 | 12 => {
                    let mut rdata_pos = pos;
                    answers.push(read_name(data, &mut rdata_pos)?);
                }
                // Other record types are skipped
                _ => {}
            }
            pos += rdlen;
        }
        (Some(rcode_name(rcode)), Some(answers))
    } else {
        (None, None)
    };

    Some(DnsMessage {
        is_response,
        name,
        query_type: qtype_name(qtype),
        response_code,
        answers,
    })
}

/// Reads a DNS name at `*pos`, following compression pointers, and advances
/// `*pos` past the name's in-place bytes
fn read_name(data: &[u8], pos: &mut usize) -> Option<String> {
    let mut labels: Vec<String> = Vec::new();
    let mut cursor = *pos;
    let mut jumped = false;
    let mut jumps = 0u8;
    loop {
        let len = *data.get(cursor)? as usize;
        if len == 0 {
            if !jumped {
                *pos = cursor + 1;
            }
            break;
        }
        if len & 0xC0 == 0xC0 {
            // Compression pointer to an earlier name
            let target = ((len & 0x3F) << 8) | *data.get(cursor + 1)? as usize;
            if !jumped {
                *pos = cursor + 2;
            }
            jumped = true;
            jumps += 1;
            if jumps > 5 {
                return None;
            }
            cursor = target;
            continue;
        }
        if len > 63 {
            return None;
        }
        let label = data.get(cursor + 1..cursor + 1 + len)?;
        // DNS labels are ASCII; rejecting anything else guards against
        // non-DNS payloads that happen to parse structurally
        if !label.iter().all(|b| b.is_ascii_graphic()) {
            return None;
        }
        labels.push(String::from_utf8_lossy(label).into_owned());
        if labels.iter().map(|l| l.len() + 1).sum::<usize>() > 255 {
            return None;
        }
        cursor += 1 + len;
    }
    if labels.is_empty() {
        // The root domain
        return Some(".".to_string());
    }
    Some(labels.join("."))
}

fn qtype_name(qtype: u16) -> String {
    match qtype {
        1 => "A".to_string(),
        2 => "NS".to_string(),
        5 => "CNAME".to_string(),
        6 => "SOA".to_string(),
        12 => "PTR".to_string(),
        15 => "MX".to_string(),
        16 => "TXT".to_string(),
        28 => "AAAA".to_string(),
        33 => "SRV".to_string(),
        65 => "HTTPS".to_string(),
        255 => "ANY".to_string(),
        other => other.to_string(),
    }
}

fn rcode_name(rcode: u16) -> String {
    match rcode {
        0 => "NOERROR".to_string(),
        1 => "FORMERR".to_string(),
        2 => "SERVFAIL".to_string(),
        3 => "NXDOMAIN".to_string(),
        4 => "NOTIMP".to_string(),
        5 => "REFUSED".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A query for example.com A
    fn example_query() -> Vec<u8> {
        let mut m = vec![
            0x12, 0x34, // id
            0x01, 0x00, // flags: RD
            0x00, 0x01, // qdcount
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // an/ns/ar
        ];
        m.extend_from_slice(b"\x07example\x03com\x00");
        m.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]); // A, IN
        m
    }

    #[test]
    fn test_parse_query() {
        let message = parse_dns(&example_query()).unwrap();
        assert!(!message.is_response);
        assert_eq!(message.name, "example.com");
        assert_eq!(message.query_type, "A");
        assert_eq!(message.response_code, None);
        assert_eq!(message.answers, None);
    }

    #[test]
    fn test_parse_response_with_compressed_answers() {
        let mut m = vec![
            0x12, 0x34, // id
            0x81, 0x80, // flags: response, RD, RA, NOERROR
            0x00, 0x01, // qdcount
            0x00, 0x02, // ancount
            0x00, 0x00, 0x00, 0x00, // ns/ar
        ];
        m.extend_from_slice(b"\x07example\x03com\x00");
        m.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]); // A, IN
                                                        // CNAME answer, owner name is a pointer to offset 12
        m.extend_from_slice(&[0xC0, 0x0C]); // pointer to question name
        m.extend_from_slice(&[0x00, 0x05, 0x00, 0x01]); // CNAME, IN
        m.extend_from_slice(&[0x00, 0x00, 0x00, 0x3C]); // ttl
        m.extend_from_slice(&[0x00, 0x06]); // rdlength
        m.extend_from_slice(b"\x03www\xC0\x0C"); // www.<pointer to example.com>
                                                 // A answer for the cname target (pointer to the www label)
        m.extend_from_slice(&[0xC0, 0x21]); // pointer to "www.example.com"
        m.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]); // A, IN
        m.extend_from_slice(&[0x00, 0x00, 0x00, 0x3C]); // ttl
        m.extend_from_slice(&[0x00, 0x04]); // rdlength
        m.extend_from_slice(&[93, 184, 216, 34]);

        let message = parse_dns(&m).unwrap();
        assert!(message.is_response);
        assert_eq!(message.name, "example.com");
        assert_eq!(message.response_code.as_deref(), Some("NOERROR"));
        assert_eq!(
            message.answers,
            Some(vec![
                "www.example.com".to_string(),
                "93.184.216.34".to_string()
            ])
        );
    }

    #[test]
    fn test_parse_nxdomain_response() {
        let mut m = example_query();
        m[2] = 0x81;
        m[3] = 0x83; // response, NXDOMAIN
        let message = parse_dns(&m).unwrap();
        assert!(message.is_response);
        assert_eq!(message.response_code.as_deref(), Some("NXDOMAIN"));
        assert_eq!(message.answers, Some(vec![]));
    }

    #[test]
    fn test_garbage_is_none() {
        assert_eq!(parse_dns(b""), None);
        assert_eq!(parse_dns(b"hello world, definitely not dns"), None);
        assert_eq!(parse_dns(&[0xFF; 64]), None);
        // Truncated mid-name
        assert_eq!(parse_dns(&example_query()[..14]), None);
    }

    #[test]
    fn test_pointer_loop_rejected() {
        let mut m = vec![
            0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        // A name that is a pointer to itself
        m.extend_from_slice(&[0xC0, 0x0C]);
        m.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
        assert_eq!(parse_dns(&m), None);
    }

    #[test]
    fn test_return_type_is_struct() {
        let udf = DnsQueryUdf::default();
        assert_eq!(
            udf.return_type(&[DataType::Binary]).unwrap(),
            DataType::Struct(dns_fields())
        );
    }
}
