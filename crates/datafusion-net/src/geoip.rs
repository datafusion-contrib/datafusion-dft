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

//! IP geolocation scalar UDF.
//!
//! [`GeoIpUdf`] (`geoip`) takes an IP address string and looks it up in a
//! MaxMind-format (`.mmdb`) geolocation database such as [GeoLite2-City],
//! returning a struct of location fields:
//!
//! ```sql
//! SELECT src_ip,
//!        geoip(src_ip)['country_code'] AS country,
//!        geoip(src_ip)['city'] AS city
//! FROM pcap('capture.pcap')
//! ```
//!
//! A single MaxMind-format database file serves every field — there is no
//! per-field database. A City-schema database ([GeoLite2-City] free with a
//! MaxMind account, or the commercial GeoIP2-City) populates all fields; a
//! Country-schema database (GeoLite2-Country) populates only `country_code`
//! and `country`, leaving the rest NULL; other schemas (ASN, ISP, ...) have
//! none of these fields and yield all-NULL values.
//!
//! The database path is taken from the optional second argument
//! (`geoip(ip, '/path/GeoLite2-City.mmdb')`), or, for the single-argument
//! form, from the `GEOIP_DB` environment variable ([`GEOIP_DB_ENV_VAR`]) read
//! when the UDF is constructed. Opened databases are cached process-wide.
//!
//! Addresses that fail to parse or have no entry in the database yield a
//! NULL struct. A database that is missing, unreadable, or unconfigured does
//! not fail the query: the location fields are NULL and the struct's `error`
//! field carries the reason (it is NULL on success), so
//! `geoip(ip)['error']` surfaces what went wrong.
//!
//! [GeoLite2-City]: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data

use std::{
    collections::HashMap,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, OnceLock},
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float64Builder, NullBufferBuilder, StringArray, StringBuilder,
            StructArray,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{cast::as_string_array, exec_err, Result},
    logical_expr::{
        ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
        Volatility,
    },
};
use maxminddb::geoip2;

use crate::NET_DOC_SECTION;

/// Environment variable consulted by [`GeoIpUdf::default`] for the database
/// path used by the single-argument form of `geoip`
pub const GEOIP_DB_ENV_VAR: &str = "GEOIP_DB";

/// `SHOW FUNCTIONS` documentation for `geoip`
static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        NET_DOC_SECTION,
        "Geolocates an IP address string using a MaxMind-format (.mmdb) \
         database, returning a struct with country_code, country, city, \
         latitude, longitude, time_zone, and error fields. A City-schema \
         database populates all fields; a Country-schema database only \
         country_code and country. The database path is the optional second \
         argument, the GEOIP_DB environment variable, or (in dft) the \
         [execution.net] geoip_db_path config. Addresses that do not parse or \
         are absent from the database yield a NULL struct; a missing or \
         unreadable database yields NULL location fields with the reason in \
         the error field rather than failing the query.",
        "geoip(ip [, db_path])",
    )
    .with_argument("ip", "IP address string, e.g. the src_ip or dst_ip column")
    .with_argument(
        "db_path",
        "Optional path to a MaxMind .mmdb database; defaults to the GEOIP_DB \
         environment variable or configured path",
    )
    .with_sql_example(
        "SELECT geoip(src_ip, '/path/GeoLite2-City.mmdb')['country_code'] AS country, \
         count(*) AS packets FROM pcap('capture.pcap') GROUP BY country ORDER BY packets DESC",
    )
    .with_related_udf("reverse_dns")
    .build()
});

/// A cached, shared handle to an opened database
type SharedReader = Arc<maxminddb::Reader<Vec<u8>>>;

/// A cached open outcome: the reader, or the message describing why the
/// database could not be opened
type ReaderResult = std::result::Result<SharedReader, Arc<str>>;

/// Process-wide cache of database open outcomes, keyed by path. Readers hold
/// the whole database in memory, so each distinct path is loaded once and
/// shared across batches and queries. Failures are cached too, so a bad path
/// costs one open attempt instead of one per row.
fn readers() -> &'static Mutex<HashMap<PathBuf, ReaderResult>> {
    static READERS: OnceLock<Mutex<HashMap<PathBuf, ReaderResult>>> = OnceLock::new();
    READERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Fields of the struct returned by `geoip`. `error` is NULL on success and
/// carries the reason when the database could not be used.
fn geoip_fields() -> Fields {
    Fields::from(vec![
        Field::new("country_code", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("time_zone", DataType::Utf8, true),
        Field::new("error", DataType::Utf8, true),
    ])
}

/// Owned location values extracted from a database record, or the reason no
/// lookup could be performed
#[derive(Default)]
struct GeoRecord {
    country_code: Option<String>,
    country: Option<String>,
    city: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    time_zone: Option<String>,
    error: Option<String>,
}

impl GeoRecord {
    fn from_city(city: &geoip2::City<'_>) -> Self {
        Self {
            country_code: city.country.iso_code.map(str::to_string),
            country: city.country.names.english.map(str::to_string),
            city: city.city.names.english.map(str::to_string),
            latitude: city.location.latitude,
            longitude: city.location.longitude,
            time_zone: city.location.time_zone.map(str::to_string),
            error: None,
        }
    }

    /// A record whose location fields are NULL and whose `error` field
    /// carries the reason
    fn from_error(error: impl Into<String>) -> Self {
        Self {
            error: Some(error.into()),
            ..Self::default()
        }
    }
}

/// Scalar UDF that geolocates an IP address string using a MaxMind-format
/// (`.mmdb`) database
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GeoIpUdf {
    signature: Signature,
    default_db: Option<PathBuf>,
}

impl GeoIpUdf {
    /// Creates the UDF with `path` as the database for the single-argument
    /// form of `geoip`
    pub fn with_db_path(path: impl Into<PathBuf>) -> Self {
        Self {
            signature: Self::make_signature(),
            default_db: Some(path.into()),
        }
    }

    fn make_signature() -> Signature {
        // Accept the string types our packet columns and SQL literals use.
        // Stable (not Immutable): results depend on the database file, which
        // can change between queries.
        Signature::one_of(
            vec![TypeSignature::String(1), TypeSignature::String(2)],
            Volatility::Stable,
        )
    }

    /// Returns the cached open outcome for `path`, attempting (and caching)
    /// the open on first use
    fn reader_for(path: &Path) -> ReaderResult {
        let mut readers = readers().lock().expect("geoip reader cache poisoned");
        if let Some(outcome) = readers.get(path) {
            return outcome.clone();
        }
        let outcome = maxminddb::Reader::open_readfile(path)
            .map(Arc::new)
            .map_err(|e| {
                Arc::from(format!(
                    "geoip failed to open database '{}': {e}",
                    path.display()
                ))
            });
        readers.insert(path.to_path_buf(), outcome.clone());
        outcome
    }
}

impl Default for GeoIpUdf {
    fn default() -> Self {
        Self {
            signature: Self::make_signature(),
            default_db: std::env::var_os(GEOIP_DB_ENV_VAR).map(PathBuf::from),
        }
    }
}

impl ScalarUDFImpl for GeoIpUdf {
    fn name(&self) -> &str {
        "geoip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(geoip_fields()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!("geoip expects one or two arguments: geoip(ip [, db_path])");
        }
        let ips = to_utf8_array(&args.args[0].to_array(args.number_rows)?)?;
        let ips = as_string_array(&ips)?;
        let paths = match args.args.get(1) {
            Some(arg) => Some(to_utf8_array(&arg.to_array(args.number_rows)?)?),
            None => None,
        };
        let paths = paths.as_ref().map(|p| as_string_array(p)).transpose()?;

        let mut country_code = StringBuilder::new();
        let mut country = StringBuilder::new();
        let mut city = StringBuilder::new();
        let mut latitude = Float64Builder::new();
        let mut longitude = Float64Builder::new();
        let mut time_zone = StringBuilder::new();
        let mut error = StringBuilder::new();
        let mut validity = NullBufferBuilder::new(ips.len());

        for row in 0..ips.len() {
            match self.lookup_row(ips, paths, row) {
                Some(record) => {
                    country_code.append_option(record.country_code);
                    country.append_option(record.country);
                    city.append_option(record.city);
                    latitude.append_option(record.latitude);
                    longitude.append_option(record.longitude);
                    time_zone.append_option(record.time_zone);
                    error.append_option(record.error);
                    validity.append_non_null();
                }
                None => {
                    country_code.append_null();
                    country.append_null();
                    city.append_null();
                    latitude.append_null();
                    longitude.append_null();
                    time_zone.append_null();
                    error.append_null();
                    validity.append_null();
                }
            }
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(country_code.finish()),
            Arc::new(country.finish()),
            Arc::new(city.finish()),
            Arc::new(latitude.finish()),
            Arc::new(longitude.finish()),
            Arc::new(time_zone.finish()),
            Arc::new(error.finish()),
        ];
        let structs = StructArray::new(geoip_fields(), arrays, validity.finish());
        Ok(ColumnarValue::Array(Arc::new(structs)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

impl GeoIpUdf {
    /// Geolocates a single row. `None` (a NULL struct) covers the row-level
    /// misses: NULL or unparseable address, NULL path argument, or an
    /// address with no database entry. Database problems — none configured,
    /// or one that cannot be opened — do not fail the query: they produce a
    /// record whose location fields are NULL and whose `error` field carries
    /// the reason.
    fn lookup_row(
        &self,
        ips: &StringArray,
        paths: Option<&StringArray>,
        row: usize,
    ) -> Option<GeoRecord> {
        if ips.is_null(row) {
            return None;
        }
        // Parse before touching the database so bad addresses are NULL even
        // when no database is configured
        let Ok(ip) = ips.value(row).parse::<IpAddr>() else {
            return None;
        };
        let path: PathBuf = match (paths, &self.default_db) {
            (Some(paths), _) if paths.is_null(row) => return None,
            (Some(paths), _) => PathBuf::from(paths.value(row)),
            (None, Some(default)) => default.clone(),
            (None, None) => {
                return Some(GeoRecord::from_error(format!(
                    "geoip has no database configured; pass a path as the second argument \
                     (e.g. geoip(ip, '/path/GeoLite2-City.mmdb')) or set the \
                     {GEOIP_DB_ENV_VAR} environment variable"
                )))
            }
        };
        let reader = match Self::reader_for(&path) {
            Ok(reader) => reader,
            Err(open_error) => return Some(GeoRecord::from_error(open_error.as_ref())),
        };
        // Lookup errors (e.g. an IPv6 address in an IPv4-only database) are
        // row-level data issues, treated as misses
        reader
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok().flatten())
            .map(|city| GeoRecord::from_city(&city))
    }
}

/// Normalizes the input (Utf8View / LargeUtf8) to Utf8 so a single code path
/// handles every accepted string type
fn to_utf8_array(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Utf8 {
        Ok(Arc::clone(array))
    } else {
        Ok(datafusion::arrow::compute::cast(array, &DataType::Utf8)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_type_is_struct() {
        let udf = GeoIpUdf::default();
        assert_eq!(
            udf.return_type(&[DataType::Utf8]).unwrap(),
            DataType::Struct(geoip_fields())
        );
    }

    #[test]
    fn test_with_db_path_sets_default() {
        let udf = GeoIpUdf::with_db_path("/some/db.mmdb");
        assert_eq!(udf.default_db, Some(PathBuf::from("/some/db.mmdb")));
    }
}
