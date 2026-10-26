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

//! Vortex output writer options for `--output` with a `.vortex` suffix.
//!
//! The layout strategy is built here instead of using
//! `vortex_file::WriteStrategyBuilder`. That builder can only override a field with a complete
//! leaf strategy, which drops zone maps. This module composes the same pipeline as
//! `WriteStrategyBuilder::build` for every column and changes only the compressor.
//!
//! Keep the pipeline in [`build_leaf`] in sync with `vortex_file::WriteStrategyBuilder::build`
//! when the pinned Vortex version changes.

use color_eyre::eyre::{eyre, Result};
use datafusion::arrow::datatypes::{DataType, Schema};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::OnceLock;
use vortex::compressor::{BtrBlocksCompressorBuilder, Scheme};
use vortex::dtype::FieldPath;
use vortex::layout::layouts::buffered::BufferedStrategy;
use vortex::layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex::layout::layouts::collect::CollectStrategy;
use vortex::layout::layouts::compressed::{CompressingStrategy, CompressorPlugin};
use vortex::layout::layouts::dict::writer::{DictLayoutOptions, DictStrategy};
use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex::layout::layouts::list::writer::ListLayoutStrategy;
use vortex::layout::layouts::repartition::{RepartitionStrategy, RepartitionWriterOptions};
use vortex::layout::layouts::table::{use_experimental_list_layout, TableStrategy};
use vortex::layout::layouts::zoned::writer::{ZonedLayoutOptions, ZonedStrategy};
use vortex::layout::LayoutStrategy;
use vortex_btrblocks::schemes::{binary, float, integer, string};
use vortex_btrblocks::{SchemeExt, ALL_SCHEMES};

/// Default row block size. Matches `vortex_file::WriteStrategyBuilder`.
const DEFAULT_ROW_BLOCK_SIZE: usize = 8192;
/// Default target size for coalesced data blocks. Matches `vortex_file::WriteStrategyBuilder`.
const DEFAULT_DATA_BLOCK_TARGET_BYTES: u64 = 1 << 20;
/// Buffer size between compression and layout emission. Matches `vortex_file::WriteStrategyBuilder`.
const BUFFER_SIZE: u64 = 2 * (1 << 20);

/// Schemes that are not part of `ALL_SCHEMES` and need the `zstd` and `pco` features.
const COMPACT_SCHEMES: &[&dyn Scheme] = &[
    &string::ZstdScheme,
    &binary::ZstdScheme,
    &integer::PcoScheme,
    &float::PcoScheme,
];

/// Parsed `--format-option` values for a `.vortex` output file.
#[derive(Debug)]
pub struct VortexWriteConfig {
    row_block_size: usize,
    data_block_target_bytes: Option<u64>,
    compact: bool,
    statistics: bool,
    columns: Vec<ColumnOverride>,
}

#[derive(Debug)]
struct ColumnOverride {
    name: String,
    encoding: Option<String>,
    dictionary: Option<bool>,
    statistics: Option<bool>,
}

impl Default for VortexWriteConfig {
    fn default() -> Self {
        Self {
            row_block_size: DEFAULT_ROW_BLOCK_SIZE,
            data_block_target_bytes: Some(DEFAULT_DATA_BLOCK_TARGET_BYTES),
            compact: false,
            statistics: true,
            columns: Vec::new(),
        }
    }
}

impl VortexWriteConfig {
    /// Parse `--format-option KEY=VALUE` pairs. The optional `format.` prefix is accepted.
    pub fn parse(options: &[(String, String)]) -> Result<Self> {
        let mut config = Self::default();
        for (key, value) in options {
            let key = key.strip_prefix("format.").unwrap_or(key);
            match key {
                "row_block_size" => {
                    let size: usize = value.parse().map_err(|_| {
                        eyre!("Invalid row_block_size '{value}': expected a positive integer")
                    })?;
                    if size == 0 {
                        return Err(eyre!(
                            "Invalid row_block_size '0': expected a positive integer"
                        ));
                    }
                    config.row_block_size = size;
                }
                "data_block_target_bytes" => {
                    config.data_block_target_bytes = match value.to_ascii_lowercase().as_str() {
                        "none" | "off" | "0" => None,
                        _ => Some(value.parse().map_err(|_| {
                            eyre!(
                                "Invalid data_block_target_bytes '{value}': expected a byte count or 'none'"
                            )
                        })?),
                    };
                }
                "compressor" => {
                    config.compact = match value.to_ascii_lowercase().as_str() {
                        "default" => false,
                        "compact" => true,
                        other => {
                            return Err(eyre!(
                                "Unknown vortex compressor '{other}': expected 'default' or 'compact'"
                            ));
                        }
                    };
                }
                "statistics_enabled" => {
                    config.statistics = parse_bool(value)?;
                }
                _ => {
                    if let Some(column) = key.strip_prefix("encoding::") {
                        if column.is_empty() {
                            return Err(eyre!(
                                "encoding requires a column name, for example encoding::id=bitpacking"
                            ));
                        }
                        config.column(column).encoding = Some(value.clone());
                    } else if let Some(column) = key.strip_prefix("dictionary_enabled::") {
                        if column.is_empty() {
                            return Err(eyre!(
                                "dictionary_enabled requires a column name, for example dictionary_enabled::id=false"
                            ));
                        }
                        let enabled = parse_bool(value)?;
                        config.column(column).dictionary = Some(enabled);
                    } else if let Some(column) = key.strip_prefix("statistics_enabled::") {
                        if column.is_empty() {
                            return Err(eyre!(
                                "statistics_enabled requires a column name, for example statistics_enabled::id=false"
                            ));
                        }
                        let enabled = parse_bool(value)?;
                        config.column(column).statistics = Some(enabled);
                    } else {
                        return Err(eyre!("Unknown vortex output format option '{key}'"));
                    }
                }
            }
        }
        Ok(config)
    }

    fn column(&mut self, name: &str) -> &mut ColumnOverride {
        if let Some(index) = self.columns.iter().position(|column| column.name == name) {
            return &mut self.columns[index];
        }
        self.columns.push(ColumnOverride {
            name: name.to_string(),
            encoding: None,
            dictionary: None,
            statistics: None,
        });
        self.columns.last_mut().expect("a column was just pushed")
    }

    /// Build the layout strategy. Every column uses the same pipeline; only the compressor and
    /// the zone-map layer differ per column, so overrides keep zone maps unless disabled.
    pub fn build_strategy(&self, schema: &Schema) -> Result<Arc<dyn LayoutStrategy>> {
        let base = if self.compact {
            BtrBlocksCompressorBuilder::default().with_compact()
        } else {
            BtrBlocksCompressorBuilder::default()
        };
        let stats_compressor: Arc<dyn CompressorPlugin> = Arc::new(base.clone().build());
        let default_data_compressor: Arc<dyn CompressorPlugin> = Arc::new(
            base.clone()
                .exclude_schemes([integer::IntDictScheme.id()])
                .build(),
        );

        let row_block_size = NonZeroUsize::new(self.row_block_size)
            .ok_or_else(|| eyre!("row_block_size must be greater than zero"))?;

        let default_leaf = build_leaf(
            Arc::clone(&default_data_compressor),
            Arc::clone(&stats_compressor),
            row_block_size,
            self.data_block_target_bytes,
            true,
            self.statistics,
        );

        let mut field_writers: HashMap<FieldPath, Arc<dyn LayoutStrategy>> = HashMap::new();
        for column in &self.columns {
            let field = schema
                .field_with_name(&column.name)
                .map_err(|_| eyre!("Unknown column '{}' in vortex format option", column.name))?;
            let kind = scheme_kind(field.data_type()).ok_or_else(|| {
                eyre!(
                    "Column '{}' has type {} and cannot use a vortex encoding override",
                    column.name,
                    field.data_type()
                )
            })?;
            let data_compressor: Arc<dyn CompressorPlugin> = match &column.encoding {
                Some(raw) => {
                    let scheme = resolve_scheme(raw, kind, &column.name)?;
                    Arc::new(
                        BtrBlocksCompressorBuilder::empty()
                            .with_new_scheme(scheme)
                            .build(),
                    )
                }
                None => Arc::clone(&default_data_compressor),
            };
            let leaf = build_leaf(
                data_compressor,
                Arc::clone(&stats_compressor),
                row_block_size,
                self.data_block_target_bytes,
                column.dictionary.unwrap_or(true),
                column.statistics.unwrap_or(self.statistics),
            );
            field_writers.insert(FieldPath::from_name(column.name.clone()), leaf);
        }

        let validity: Arc<dyn LayoutStrategy> = Arc::new(CollectStrategy::new(
            CompressingStrategy::new(flat_strategy(), Arc::clone(&stats_compressor)),
        ));

        let mut table = TableStrategy::new(validity, Arc::clone(&default_leaf))
            .with_field_writers(field_writers);

        if use_experimental_list_layout() {
            let stats_compressor = Arc::clone(&stats_compressor);
            table = table.with_list_layout_factory(
                move |list_layout: ListLayoutStrategy| -> Arc<dyn LayoutStrategy> {
                    let zoned = ZonedStrategy::new(
                        list_layout,
                        CompressingStrategy::new(flat_strategy(), Arc::clone(&stats_compressor)),
                        ZonedLayoutOptions {
                            block_size: row_block_size,
                            ..Default::default()
                        },
                    );
                    Arc::new(RepartitionStrategy::new(
                        zoned,
                        RepartitionWriterOptions {
                            block_size_minimum: 0,
                            block_len_multiple: row_block_size.get(),
                            block_size_target: None,
                            canonicalize: false,
                        },
                    ))
                },
            );
        }

        Ok(Arc::new(table))
    }
}

fn flat_strategy() -> Arc<dyn LayoutStrategy> {
    Arc::new(FlatLayoutStrategy::default())
}

/// Compose the leaf pipeline used for a single column.
///
/// This mirrors the leaf of `vortex_file::WriteStrategyBuilder::build`, including the
/// [`ZonedStrategy`] that writes zone maps. When `zone_maps` is false, the zone-map layer is
/// skipped. The outer repartition exists only to give the zone-map layer fixed-size row blocks,
/// so it is skipped too.
fn build_leaf(
    data_compressor: Arc<dyn CompressorPlugin>,
    stats_compressor: Arc<dyn CompressorPlugin>,
    row_block_size: NonZeroUsize,
    data_block_target_bytes: Option<u64>,
    dictionary: bool,
    zone_maps: bool,
) -> Arc<dyn LayoutStrategy> {
    let flat = flat_strategy();
    let chunked = ChunkedLayoutStrategy::new(Arc::clone(&flat));
    let buffered = BufferedStrategy::new(chunked, BUFFER_SIZE);
    let compressing = CompressingStrategy::new(buffered, data_compressor);
    let coalescing = RepartitionStrategy::new(
        compressing,
        RepartitionWriterOptions {
            block_size_minimum: data_block_target_bytes.unwrap_or(0),
            block_len_multiple: row_block_size.get(),
            block_size_target: data_block_target_bytes,
            canonicalize: true,
        },
    );
    let compress_then_flat =
        CompressingStrategy::new(Arc::clone(&flat), Arc::clone(&stats_compressor));

    let data: Arc<dyn LayoutStrategy> = if dictionary {
        Arc::new(DictStrategy::new(
            coalescing.clone(),
            compress_then_flat.clone(),
            coalescing,
            DictLayoutOptions::default(),
            Arc::clone(&stats_compressor),
        ))
    } else {
        Arc::new(coalescing)
    };

    if !zone_maps {
        return data;
    }

    let stats = ZonedStrategy::new(
        data,
        compress_then_flat,
        ZonedLayoutOptions {
            block_size: row_block_size,
            ..Default::default()
        },
    );

    Arc::new(RepartitionStrategy::new(
        stats,
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: row_block_size.get(),
            block_size_target: None,
            canonicalize: false,
        },
    ))
}

struct SchemeEntry {
    name: &'static str,
    kind: &'static str,
    short: &'static str,
    scheme: &'static dyn Scheme,
}

fn scheme_entries() -> &'static [SchemeEntry] {
    static ENTRIES: OnceLock<Vec<SchemeEntry>> = OnceLock::new();
    ENTRIES.get_or_init(|| {
        let mut entries = Vec::new();
        for scheme in ALL_SCHEMES {
            push_scheme_entry(&mut entries, *scheme);
        }
        for scheme in COMPACT_SCHEMES {
            push_scheme_entry(&mut entries, *scheme);
        }
        entries
    })
}

fn push_scheme_entry(entries: &mut Vec<SchemeEntry>, scheme: &'static dyn Scheme) {
    let name = scheme.scheme_name();
    let Some(rest) = name.strip_prefix("vortex.") else {
        return;
    };
    let Some((kind, short)) = rest.split_once('.') else {
        return;
    };
    entries.push(SchemeEntry {
        name,
        kind,
        short,
        scheme,
    });
}

/// Resolve a user supplied encoding name against the column kind.
///
/// Accepts the full name (`vortex.int.bitpacking`), a kind-qualified short name
/// (`int.bitpacking`), or a short name (`bitpacking`) when the column kind disambiguates it.
fn resolve_scheme(raw: &str, kind: &str, column: &str) -> Result<&'static dyn Scheme> {
    let entries = scheme_entries();

    if let Some(entry) = entries.iter().find(|entry| entry.name == raw) {
        if entry.kind != kind {
            return Err(eyre!(
                "Encoding '{raw}' is for {} columns, but column '{column}' is {kind}",
                entry.kind
            ));
        }
        return Ok(entry.scheme);
    }

    if let Some((prefix, short)) = raw.split_once('.') {
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.kind == prefix && entry.short == short)
        {
            if entry.kind != kind {
                return Err(eyre!(
                    "Encoding '{raw}' is for {} columns, but column '{column}' is {kind}",
                    entry.kind
                ));
            }
            return Ok(entry.scheme);
        }
    }

    if let Some(entry) = entries
        .iter()
        .find(|entry| entry.kind == kind && entry.short == raw)
    {
        return Ok(entry.scheme);
    }

    let mut available: Vec<&str> = entries
        .iter()
        .filter(|entry| entry.kind == kind)
        .map(|entry| entry.short)
        .collect();
    available.sort_unstable();
    available.dedup();
    Err(eyre!(
        "Unknown vortex encoding '{raw}' for {kind} column '{column}'; available encodings: {}",
        available.join(", ")
    ))
}

/// Map an Arrow type to the Vortex scheme kind prefix.
fn scheme_kind(dtype: &DataType) -> Option<&'static str> {
    match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some("int"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some("float"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some("string"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Some("binary"),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Some("decimal"),
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Some("ext"),
        _ => None,
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(eyre!("Expected a boolean, got '{other}'")),
    }
}

/// Print the available `.vortex` output format options.
pub fn print_options() {
    println!("Available vortex output format options:\n");
    println!("{:<44} {:<18} DESCRIPTION", "OPTION", "DEFAULT");
    println!(
        "{:<44} {:<18} Row block size for repartitioning and zone maps",
        "row_block_size", DEFAULT_ROW_BLOCK_SIZE
    );
    println!(
        "{:<44} {:<18} Target uncompressed bytes per data block; 'none' disables coalescing",
        "data_block_target_bytes", DEFAULT_DATA_BLOCK_TARGET_BYTES
    );
    println!(
        "{:<44} {:<18} 'default' or 'compact' (adds zstd and pco schemes)",
        "compressor", "default"
    );
    println!(
        "{:<44} {:<18} Force a compression scheme for a top-level column",
        "encoding::COLUMN", "<adaptive>"
    );
    println!(
        "{:<44} {:<18} Enable dictionary encoding for a top-level column",
        "dictionary_enabled::COLUMN", "true"
    );
    println!(
        "{:<44} {:<18} Write zone maps (statistics) for pruning",
        "statistics_enabled", "true"
    );
    println!(
        "{:<44} {:<18} Write zone maps for a top-level column",
        "statistics_enabled::COLUMN", "true"
    );

    println!("\nPass an option with: --format-option KEY=VALUE");
    println!(
        "Use KEY::COLUMN=VALUE for column-specific keys (for example, encoding::id=bitpacking)."
    );

    println!("\nAvailable encodings by column type:");
    let mut kinds: Vec<&str> = scheme_entries().iter().map(|entry| entry.kind).collect();
    kinds.sort_unstable();
    kinds.dedup();
    for kind in kinds {
        let mut shorts: Vec<&str> = scheme_entries()
            .iter()
            .filter(|entry| entry.kind == kind)
            .map(|entry| entry.short)
            .collect();
        shorts.sort_unstable();
        shorts.dedup();
        println!("  {kind}: {}", shorts.join(", "));
    }
}
