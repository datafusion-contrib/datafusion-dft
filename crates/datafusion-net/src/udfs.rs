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

//! Scalar UDFs for enriching packet capture columns.
//!
//! [`ReverseDnsUdf`] (`reverse_dns`) takes an IP address string and resolves
//! it to a hostname via a reverse DNS (PTR) lookup using the system resolver:
//!
//! ```sql
//! SELECT src_ip, reverse_dns(src_ip) AS host, count(*)
//! FROM capture('en0', '', 10)
//! GROUP BY src_ip, host
//! ```
//!
//! Lookups are network I/O, so results are cached process-wide, identical
//! addresses within a batch are resolved once, and each batch of lookups is
//! bounded by a timeout. Addresses that fail to parse, fail to resolve, or
//! time out yield NULL.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{mpsc, Arc, LazyLock, Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringArray},
        datatypes::DataType,
    },
    common::{cast::as_string_array, exec_err, Result},
    logical_expr::{
        ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
};

use crate::NET_DOC_SECTION;

/// `SHOW FUNCTIONS` documentation for `reverse_dns`
static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        NET_DOC_SECTION,
        "Resolves an IP address string to a hostname via a reverse DNS (PTR) \
         lookup using the system resolver. Returns NULL when the address does \
         not parse, does not resolve, or the lookup times out. Results are \
         cached process-wide and each batch of lookups is bounded by a timeout.",
        "reverse_dns(ip)",
    )
    .with_argument("ip", "IP address string, e.g. the src_ip or dst_ip column")
    .with_sql_example(
        "SELECT dst_ip, reverse_dns(dst_ip) AS host, count(*) AS packets \
         FROM pcap('capture.pcap') GROUP BY dst_ip, host ORDER BY packets DESC",
    )
    .build()
});

/// Maximum wall-clock time spent resolving the unique addresses in a single
/// batch. A dead or slow resolver cannot hang the query longer than this;
/// unresolved addresses in the batch become NULL.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);

/// Process-wide cache of resolved hostnames. `None` is a negative cache entry
/// (the address does not resolve), avoiding repeated lookups of the same
/// address across batches and queries.
fn cache() -> &'static Mutex<HashMap<IpAddr, Option<Arc<str>>>> {
    static CACHE: OnceLock<Mutex<HashMap<IpAddr, Option<Arc<str>>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Scalar UDF that resolves an IP address string to a hostname via reverse
/// (PTR) DNS lookup
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReverseDnsUdf {
    signature: Signature,
}

impl Default for ReverseDnsUdf {
    fn default() -> Self {
        Self {
            // Accept the string types our packet columns and SQL literals use.
            // Volatile: DNS results can change over time, so the optimizer
            // must not constant-fold or dedup calls.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for ReverseDnsUdf {
    fn name(&self) -> &str {
        "reverse_dns"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("reverse_dns expects a single argument");
        }
        let array = args.args[0].to_array(args.number_rows)?;
        let resolved = resolve_array(&array)?;
        Ok(ColumnarValue::Array(resolved))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

/// Resolves each element of `array` (a string array of IP addresses) to a
/// hostname, returning a `Utf8` array of the same length
fn resolve_array(array: &ArrayRef) -> Result<ArrayRef> {
    // Normalize the input (Utf8View / LargeUtf8) to Utf8 so a single code
    // path handles every accepted string type
    let array = if array.data_type() == &DataType::Utf8 {
        Arc::clone(array)
    } else {
        datafusion::arrow::compute::cast(array, &DataType::Utf8)?
    };
    let strings = as_string_array(&array)?;

    // Collect the unique, parseable addresses that are not already cached
    let mut to_resolve: Vec<IpAddr> = Vec::new();
    {
        let cache = cache().lock().expect("reverse_dns cache poisoned");
        for value in strings.iter().flatten() {
            if let Ok(ip) = value.parse::<IpAddr>() {
                if !cache.contains_key(&ip) && !to_resolve.contains(&ip) {
                    to_resolve.push(ip);
                }
            }
        }
    }

    if !to_resolve.is_empty() {
        let results = resolve_batch(&to_resolve);
        let mut cache = cache().lock().expect("reverse_dns cache poisoned");
        for (ip, host) in results {
            cache.insert(ip, host.map(Arc::from));
        }
    }

    // Build the output array from the cache
    let cache = cache().lock().expect("reverse_dns cache poisoned");
    let hosts: StringArray = strings
        .iter()
        .map(|value| {
            let ip = value?.parse::<IpAddr>().ok()?;
            cache.get(&ip).cloned().flatten()
        })
        .map(|host| host.map(|h| h.to_string()))
        .collect();
    Ok(Arc::new(hosts))
}

/// Resolves `ips` concurrently, bounded by [`LOOKUP_TIMEOUT`]. Addresses that
/// do not resolve within the timeout are absent from the returned map (and so
/// treated as unresolved).
fn resolve_batch(ips: &[IpAddr]) -> HashMap<IpAddr, Option<String>> {
    let (tx, rx) = mpsc::channel();
    for &ip in ips {
        let tx = tx.clone();
        // Each lookup is a blocking syscall; run them on their own threads so
        // one slow address does not serialize the rest. Detached threads whose
        // result arrives after the deadline simply have their send ignored.
        thread::spawn(move || {
            let host = dns_lookup::lookup_addr(&ip).ok();
            let _ = tx.send((ip, host));
        });
    }
    drop(tx);

    let deadline = Instant::now() + LOOKUP_TIMEOUT;
    let mut results = HashMap::with_capacity(ips.len());
    while results.len() < ips.len() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match rx.recv_timeout(remaining) {
            Ok((ip, host)) => {
                results.insert(ip, host);
            }
            Err(_) => break,
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    #[test]
    fn test_return_type_is_utf8() {
        let udf = ReverseDnsUdf::default();
        assert_eq!(udf.return_type(&[DataType::Utf8]).unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_localhost_resolves() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("127.0.0.1")]));
        let out = resolve_array(&input).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        // The PTR record for 127.0.0.1 is environment dependent, but the
        // loopback address must resolve to *some* hostname on any host with a
        // working resolver
        assert!(!out.is_null(0), "127.0.0.1 should resolve to a hostname");
        assert!(!out.value(0).is_empty());
    }

    #[test]
    fn test_unparseable_and_unresolvable_are_null() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("not-an-ip"),
            // Reserved TEST-NET-1 address (RFC 5737), never has a PTR record
            Some("192.0.2.1"),
            None,
        ]));
        let out = resolve_array(&input).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(out.is_null(0), "unparseable input should be null");
        assert!(out.is_null(1), "unresolvable address should be null");
        assert!(out.is_null(2), "null input should be null");
    }

    #[test]
    fn test_repeated_address_deduped() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            "127.0.0.1",
            "127.0.0.1",
            "127.0.0.1",
        ]));
        let out = resolve_array(&input).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), out.value(1));
        assert_eq!(out.value(1), out.value(2));
    }
}
