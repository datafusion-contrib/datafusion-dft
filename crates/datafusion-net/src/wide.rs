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

//! Wide variants of the `pcap` and `capture` table functions that append
//! DNS and geolocation enrichment columns for the source and destination
//! addresses.
//!
//! - [`PcapWideFunc`] (`pcap_wide`): `pcap` plus enrichment columns
//! - [`CaptureWideFunc`] (`capture_wide`, requires the `live` feature):
//!   `capture` plus enrichment columns
//!
//! Both take the same arguments as their narrow counterparts and add, after
//! the packet columns: `src_host` / `dst_host` (reverse DNS, see
//! [`ReverseDnsUdf`]) and `src_country` / `src_city` / `src_lat` / `src_lon`
//! plus the `dst_` equivalents (from [`GeoIpUdf`] and a MaxMind-format
//! database):
//!
//! ```sql
//! SELECT dst_ip, dst_host, dst_country, count(*) AS packets
//! FROM pcap_wide('capture.pcap')
//! GROUP BY dst_ip, dst_host, dst_country
//! ORDER BY packets DESC;
//! ```
//!
//! The geolocation database comes from the `GEOIP_DB` environment variable
//! ([`crate::GEOIP_DB_ENV_VAR`], read when the function is constructed) or a
//! path passed to `new`; a database that is unconfigured, missing, or
//! unreadable never fails the query — the geolocation columns are just NULL
//! (`geoip(ip, path)['error']` explains why). Enrichment is a projection
//! over the narrow table, so unused columns still prune and only projected
//! enrichment is computed.

use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::Result,
    datasource::{provider_as_source, ViewTable},
    functions::core::expr_ext::FieldAccessor,
    logical_expr::{col, Expr, LogicalPlanBuilder, ScalarUDF},
    scalar::ScalarValue,
};

#[cfg(feature = "live")]
use crate::live::parse_capture_args;
#[cfg(feature = "live")]
use crate::CaptureTable;
use crate::{
    file::parse_pcap_args, geoip::GEOIP_DB_ENV_VAR, schema::packet_schema, GeoIpUdf, PcapTable,
    ReverseDnsUdf,
};

/// The geolocation database path used by [`Default`] constructions of the
/// wide table functions
fn geoip_db_from_env() -> Option<PathBuf> {
    std::env::var_os(GEOIP_DB_ENV_VAR).map(PathBuf::from)
}

/// Wraps `inner` (a table with the packet schema) in a view that appends the
/// enrichment columns. Without a geolocation database the geolocation
/// columns are typed NULL literals, so the wide schema is stable either way.
fn enriched_view(
    inner: Arc<dyn TableProvider>,
    geoip_db_path: Option<&PathBuf>,
) -> Result<Arc<dyn TableProvider>> {
    let mut exprs: Vec<Expr> = packet_schema()
        .fields()
        .iter()
        .map(|f| col(f.name().as_str()))
        .collect();

    let reverse_dns = Arc::new(ScalarUDF::from(ReverseDnsUdf::default()));
    exprs.push(reverse_dns.call(vec![col("src_ip")]).alias("src_host"));
    exprs.push(reverse_dns.call(vec![col("dst_ip")]).alias("dst_host"));

    let geoip = geoip_db_path.map(|path| Arc::new(ScalarUDF::from(GeoIpUdf::with_db_path(path))));
    for (ip_col, prefix) in [("src_ip", "src"), ("dst_ip", "dst")] {
        match &geoip {
            Some(geoip) => {
                let geo = geoip.call(vec![col(ip_col)]);
                exprs.push(
                    geo.clone()
                        .field("country_code")
                        .alias(format!("{prefix}_country")),
                );
                exprs.push(geo.clone().field("city").alias(format!("{prefix}_city")));
                exprs.push(geo.clone().field("latitude").alias(format!("{prefix}_lat")));
                exprs.push(geo.field("longitude").alias(format!("{prefix}_lon")));
            }
            None => {
                exprs.push(
                    Expr::Literal(ScalarValue::Utf8(None), None).alias(format!("{prefix}_country")),
                );
                exprs.push(
                    Expr::Literal(ScalarValue::Utf8(None), None).alias(format!("{prefix}_city")),
                );
                exprs.push(
                    Expr::Literal(ScalarValue::Float64(None), None).alias(format!("{prefix}_lat")),
                );
                exprs.push(
                    Expr::Literal(ScalarValue::Float64(None), None).alias(format!("{prefix}_lon")),
                );
            }
        }
    }

    let plan = LogicalPlanBuilder::scan("packets", provider_as_source(inner), None)?
        .project(exprs)?
        .build()?;
    Ok(Arc::new(ViewTable::new(plan, None)))
}

/// Table function that reads a pcap/pcapng capture file with DNS and
/// geolocation enrichment columns
#[derive(Debug)]
pub struct PcapWideFunc {
    geoip_db_path: Option<PathBuf>,
}

impl PcapWideFunc {
    /// Creates the function with `geoip_db_path` as the geolocation
    /// database; `None` yields NULL geolocation columns
    pub fn new(geoip_db_path: Option<PathBuf>) -> Self {
        Self { geoip_db_path }
    }
}

impl Default for PcapWideFunc {
    fn default() -> Self {
        Self::new(geoip_db_from_env())
    }
}

impl TableFunctionImpl for PcapWideFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = parse_pcap_args("pcap_wide", exprs)?;
        enriched_view(Arc::new(PcapTable::new(path)), self.geoip_db_path.as_ref())
    }
}

/// Table function that live-captures packets from a network interface with
/// DNS and geolocation enrichment columns
#[cfg(feature = "live")]
#[derive(Debug)]
pub struct CaptureWideFunc {
    geoip_db_path: Option<PathBuf>,
}

#[cfg(feature = "live")]
impl CaptureWideFunc {
    /// Creates the function with `geoip_db_path` as the geolocation
    /// database; `None` yields NULL geolocation columns
    pub fn new(geoip_db_path: Option<PathBuf>) -> Self {
        Self { geoip_db_path }
    }
}

#[cfg(feature = "live")]
impl Default for CaptureWideFunc {
    fn default() -> Self {
        Self::new(geoip_db_from_env())
    }
}

#[cfg(feature = "live")]
impl TableFunctionImpl for CaptureWideFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (interface, filter, duration) = parse_capture_args("capture_wide", exprs)?;
        enriched_view(
            Arc::new(CaptureTable::new(interface, filter, duration)),
            self.geoip_db_path.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;

    fn wide_provider(geoip_db_path: Option<PathBuf>) -> Arc<dyn TableProvider> {
        let func = PcapWideFunc::new(geoip_db_path);
        func.call(&[Expr::Literal(
            ScalarValue::Utf8(Some("test.pcap".to_string())),
            None,
        )])
        .unwrap()
    }

    #[test]
    fn test_wide_schema_extends_packet_schema() {
        let provider = wide_provider(None);
        let schema = provider.schema();
        // All packet columns come first, unchanged
        for (i, field) in packet_schema().fields().iter().enumerate() {
            assert_eq!(schema.field(i).name(), field.name());
            assert_eq!(schema.field(i).data_type(), field.data_type());
        }
        // Followed by the enrichment columns
        let names: Vec<_> = schema
            .fields()
            .iter()
            .skip(packet_schema().fields().len())
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            [
                "src_host",
                "dst_host",
                "src_country",
                "src_city",
                "src_lat",
                "src_lon",
                "dst_country",
                "dst_city",
                "dst_lat",
                "dst_lon"
            ]
        );
    }

    #[test]
    fn test_wide_schema_stable_with_and_without_geoip_db() {
        // The geolocation columns keep the same types whether they come from
        // geoip or from NULL literals
        let with_db = wide_provider(Some(PathBuf::from("some.mmdb"))).schema();
        let without_db = wide_provider(None).schema();
        assert_eq!(with_db.fields().len(), without_db.fields().len());
        for (a, b) in with_db.fields().iter().zip(without_db.fields().iter()) {
            assert_eq!(a.name(), b.name());
            assert_eq!(a.data_type(), b.data_type());
        }
        assert_eq!(
            with_db.field_with_name("src_country").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            with_db.field_with_name("src_lat").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[test]
    fn test_wide_rejects_bad_arguments() {
        let func = PcapWideFunc::new(None);
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("pcap_wide"), "got: {err}");
    }
}
