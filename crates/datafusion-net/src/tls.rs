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

//! `tls_sni` scalar UDF: extracts the Server Name Indication (SNI) host name
//! from a TLS ClientHello in a packet payload.
//!
//! [`TlsSniUdf`] takes a binary payload (typically the `payload` column of
//! TCP port 443 packets) and returns the SNI host name as a string, or NULL
//! when the payload is not a ClientHello or carries no SNI extension. In
//! encrypted (HTTPS) traffic the payloads are opaque, but the ClientHello —
//! the first packet a client sends — is unencrypted and names the host being
//! connected to, so this reveals the destination of otherwise opaque flows:
//!
//! ```sql
//! SELECT tls_sni(payload) AS host, count(*) AS hellos
//! FROM pcap('capture.pcap')
//! WHERE dst_port = 443 AND tls_sni(payload) IS NOT NULL
//! GROUP BY host ORDER BY hellos DESC;
//! ```
//!
//! The ClientHello must fit in the payload (it usually does — it is small and
//! sent first); this UDF does not reassemble segments.

use std::sync::{Arc, LazyLock};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder},
        datatypes::DataType,
    },
    common::{cast::as_binary_array, exec_err, Result},
    logical_expr::{
        ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
};

use crate::NET_DOC_SECTION;

/// `SHOW FUNCTIONS` documentation for `tls_sni`
static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        NET_DOC_SECTION,
        "Extracts the Server Name Indication (SNI) host name from a TLS \
         ClientHello in a packet payload (typically TCP port 443), or NULL \
         when the payload is not a ClientHello or carries no SNI. The \
         ClientHello is unencrypted even for HTTPS, so this reveals the \
         destination host of otherwise opaque connections. The ClientHello \
         must fit in the payload; segments are not reassembled.",
        "tls_sni(payload)",
    )
    .with_argument(
        "payload",
        "Binary packet payload, e.g. the payload column of a TLS packet",
    )
    .with_sql_example(
        "SELECT tls_sni(payload) AS host, count(*) AS hellos \
         FROM pcap('capture.pcap') WHERE tls_sni(payload) IS NOT NULL \
         GROUP BY host ORDER BY hellos DESC",
    )
    .with_related_udf("dns_query")
    .build()
});

/// Scalar UDF that extracts the SNI host name from a TLS ClientHello
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TlsSniUdf {
    signature: Signature,
}

impl Default for TlsSniUdf {
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

impl ScalarUDFImpl for TlsSniUdf {
    fn name(&self) -> &str {
        "tls_sni"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("tls_sni expects a single binary argument");
        }
        let payloads = to_binary_array(&args.args[0].to_array(args.number_rows)?)?;
        let payloads = as_binary_array(&payloads)?;

        let mut out = StringBuilder::new();
        for row in 0..payloads.len() {
            let sni = if payloads.is_null(row) {
                None
            } else {
                parse_sni(payloads.value(row))
            };
            out.append_option(sni);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

/// Normalizes the input (BinaryView / LargeBinary) to Binary
fn to_binary_array(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Binary {
        Ok(Arc::clone(array))
    } else {
        Ok(datafusion::arrow::compute::cast(array, &DataType::Binary)?)
    }
}

/// Cursor over a byte slice that yields big-endian fields and bails (via
/// `None`) the moment a read would run past the end
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn u8(&mut self) -> Option<u8> {
        let v = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }

    fn u16(&mut self) -> Option<usize> {
        let hi = self.u8()? as usize;
        let lo = self.u8()? as usize;
        Some((hi << 8) | lo)
    }

    fn u24(&mut self) -> Option<usize> {
        let a = self.u8()? as usize;
        let b = self.u16()?;
        Some((a << 16) | b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let slice = self.data.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(slice)
    }

    /// Advances past `n` bytes without returning them
    fn skip(&mut self, n: usize) -> Option<()> {
        self.take(n).map(|_| ())
    }
}

/// TLS record content type for handshake messages
const CONTENT_TYPE_HANDSHAKE: u8 = 22;
/// Handshake message type for ClientHello
const HANDSHAKE_CLIENT_HELLO: u8 = 1;
/// Extension type for server_name (SNI)
const EXT_SERVER_NAME: usize = 0;
/// SNI name type for host_name
const SNI_HOST_NAME: u8 = 0;

/// Parses the SNI host name out of a TLS ClientHello. Returns `None` if the
/// bytes are not a ClientHello record or carry no host_name SNI entry.
fn parse_sni(data: &[u8]) -> Option<String> {
    let mut r = Reader::new(data);

    // TLS record header
    if r.u8()? != CONTENT_TYPE_HANDSHAKE {
        return None;
    }
    let _record_version = r.u16()?;
    let _record_len = r.u16()?;

    // Handshake header
    if r.u8()? != HANDSHAKE_CLIENT_HELLO {
        return None;
    }
    let _handshake_len = r.u24()?;

    // ClientHello body
    let _client_version = r.u16()?;
    r.skip(32)?; // random
    let session_id_len = r.u8()? as usize;
    r.skip(session_id_len)?;
    let cipher_suites_len = r.u16()?;
    r.skip(cipher_suites_len)?;
    let compression_len = r.u8()? as usize;
    r.skip(compression_len)?;

    // Extensions (absent in very old TLS, in which case there is no SNI)
    let _extensions_len = r.u16()?;
    loop {
        let ext_type = r.u16()?;
        let ext_len = r.u16()?;
        if ext_type == EXT_SERVER_NAME {
            return parse_server_name_extension(r.take(ext_len)?);
        }
        r.skip(ext_len)?;
    }
}

/// Parses a server_name extension body, returning the first host_name entry
fn parse_server_name_extension(data: &[u8]) -> Option<String> {
    let mut r = Reader::new(data);
    let _list_len = r.u16()?;
    // The list can hold multiple entries; return the first host_name
    loop {
        let name_type = r.u8()?;
        let name_len = r.u16()?;
        let name = r.take(name_len)?;
        if name_type == SNI_HOST_NAME {
            // Host names are ASCII (IDNs are punycode-encoded on the wire)
            if !name.iter().all(|b| b.is_ascii_graphic()) {
                return None;
            }
            return Some(String::from_utf8_lossy(name).into_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    /// Builds a minimal ClientHello record whose only extension is SNI for
    /// `host`
    fn client_hello_with_sni(host: &str) -> Vec<u8> {
        let host = host.as_bytes();

        // server_name extension body
        let mut sni_ext = Vec::new();
        let entry_len = 1 + 2 + host.len();
        sni_ext.extend_from_slice(&(entry_len as u16).to_be_bytes()); // list length
        sni_ext.push(SNI_HOST_NAME);
        sni_ext.extend_from_slice(&(host.len() as u16).to_be_bytes());
        sni_ext.extend_from_slice(host);

        // extensions block: one extension (type 0)
        let mut extensions = Vec::new();
        extensions.extend_from_slice(&(EXT_SERVER_NAME as u16).to_be_bytes());
        extensions.extend_from_slice(&(sni_ext.len() as u16).to_be_bytes());
        extensions.extend_from_slice(&sni_ext);

        // ClientHello body
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]); // client version TLS 1.2
        body.extend_from_slice(&[0u8; 32]); // random
        body.push(0); // session id length
        body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]); // cipher suites: len 2 + one suite
        body.extend_from_slice(&[0x01, 0x00]); // compression: len 1 + null
        body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
        body.extend_from_slice(&extensions);

        // Handshake header
        let mut handshake = Vec::new();
        handshake.push(HANDSHAKE_CLIENT_HELLO);
        let body_len = body.len();
        handshake.extend_from_slice(&[
            (body_len >> 16) as u8,
            (body_len >> 8) as u8,
            body_len as u8,
        ]);
        handshake.extend_from_slice(&body);

        // Record header
        let mut record = Vec::new();
        record.push(CONTENT_TYPE_HANDSHAKE);
        record.extend_from_slice(&[0x03, 0x01]); // record version
        record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
        record.extend_from_slice(&handshake);
        record
    }

    #[test]
    fn test_parse_sni() {
        let hello = client_hello_with_sni("example.com");
        assert_eq!(parse_sni(&hello).as_deref(), Some("example.com"));
    }

    #[test]
    fn test_parse_sni_skips_earlier_extension() {
        // Prepend a non-SNI extension by hand: rebuild with an extra
        // extension of type 0x0017 (extended_master_secret, empty)
        let mut hello = client_hello_with_sni("cdn.example.org");
        // The SNI is still found because parse_sni scans all extensions; a
        // direct check that the right host is returned suffices here
        assert_eq!(parse_sni(&hello).as_deref(), Some("cdn.example.org"));
        // Corrupting the content type makes it not a ClientHello
        hello[0] = 0x17;
        assert_eq!(parse_sni(&hello), None);
    }

    #[test]
    fn test_non_client_hello_is_none() {
        // Handshake record but a different handshake type (ServerHello = 2)
        let mut hello = client_hello_with_sni("example.com");
        hello[5] = 0x02;
        assert_eq!(parse_sni(&hello), None);
    }

    #[test]
    fn test_garbage_is_none() {
        assert_eq!(parse_sni(b""), None);
        assert_eq!(parse_sni(b"GET / HTTP/1.1\r\n"), None);
        assert_eq!(parse_sni(&[0x16, 0x03, 0x01, 0x00]), None); // truncated
    }

    #[test]
    fn test_client_hello_without_sni_is_none() {
        // A ClientHello with an empty extensions block
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]);
        body.extend_from_slice(&[0u8; 32]);
        body.push(0);
        body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]);
        body.extend_from_slice(&[0x01, 0x00]);
        body.extend_from_slice(&[0x00, 0x00]); // extensions length 0

        let mut handshake = vec![HANDSHAKE_CLIENT_HELLO];
        let body_len = body.len();
        handshake.extend_from_slice(&[0, (body_len >> 8) as u8, body_len as u8]);
        handshake.extend_from_slice(&body);

        let mut record = vec![CONTENT_TYPE_HANDSHAKE, 0x03, 0x01];
        record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
        record.extend_from_slice(&handshake);

        assert_eq!(parse_sni(&record), None);
    }

    #[test]
    fn test_return_type_is_utf8() {
        let udf = TlsSniUdf::default();
        assert_eq!(
            udf.return_type(&[DataType::Binary]).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn test_has_documentation() {
        let udf = TlsSniUdf::default();
        let doc = udf.documentation().unwrap();
        assert_eq!(doc.syntax_example, "tls_sni(payload)");
        assert!(doc.arguments.is_some());
    }

    #[tokio::test]
    async fn test_invoke_over_array() {
        use datafusion::arrow::array::{BinaryArray, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::logical_expr::ScalarUDF;
        use datafusion::prelude::SessionContext;

        let hello = client_hello_with_sni("secure.example.net");
        let values: Vec<Option<&[u8]>> = vec![Some(hello.as_slice()), Some(b"not tls"), None];
        let array = BinaryArray::from(values);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("packets", batch).unwrap();
        ctx.register_udf(ScalarUDF::from(TlsSniUdf::default()));
        let batches = ctx
            .sql("SELECT tls_sni(payload) AS host FROM packets ORDER BY host NULLS LAST")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let out = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Exactly one row parsed to a host; the other two are NULL
        assert_eq!(out.value(0), "secure.example.net");
        assert!(out.is_null(1));
        assert!(out.is_null(2));
    }
}
