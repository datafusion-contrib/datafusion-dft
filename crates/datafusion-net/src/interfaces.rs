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

//! `interfaces` table function: lists the system's network capture
//! interfaces, one row per interface, similar to `tshark -D`. Useful for
//! discovering what to pass to the `capture` table function:
//!
//! ```sql
//! SELECT name, description, addresses FROM interfaces() WHERE is_up;
//! ```
//!
//! Listing interfaces does not require elevated privileges (unlike opening
//! one for capture).

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanBuilder, ListBuilder, RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    common::{plan_err, DataFusionError, Result},
    prelude::Expr,
};
use pcap::ConnectionStatus;

/// Schema of the `interfaces` table function: one row per interface
pub fn interfaces_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new(
            "addresses",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("is_up", DataType::Boolean, true),
        Field::new("is_running", DataType::Boolean, true),
        Field::new("is_loopback", DataType::Boolean, true),
        Field::new("is_wireless", DataType::Boolean, true),
        Field::new("connection_status", DataType::Utf8, true),
    ]))
}

/// Table function that lists the system's network capture interfaces
#[derive(Debug, Default)]
pub struct InterfacesFunc {}

impl TableFunctionImpl for InterfacesFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("interfaces takes no arguments");
        }
        let schema = interfaces_schema();
        let batch = interfaces_batch(&schema)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

/// Falls back to a static, human-readable description for well-known
/// interface name patterns when `pcap` doesn't supply one itself (libpcap
/// leaves `desc` empty for most interfaces on macOS and Linux).
fn static_description(name: &str) -> Option<String> {
    let prefix = name.trim_end_matches(|c: char| c.is_ascii_digit());
    let desc = match prefix {
        "lo" => "Loopback interface",
        "en" => "Ethernet or Wi-Fi adapter",
        "utun" => "Utility tunnel interface (VPN, Back to My Mac, etc.)",
        "ap" => "Wi-Fi Access Point (Personal Hotspot / Instant Hotspot)",
        "awdl" => "Apple Wireless Direct Link (AirDrop, AirPlay)",
        "llw" => "Low-Latency WLAN interface (AWDL companion interface)",
        "anpi" => "Apple Network Platform Interface (Wi-Fi/Bluetooth coexistence)",
        "bridge" => "Virtual bridge interface",
        "gif" => "Generic tunnel interface (IPv6-in-IPv4)",
        "stf" => "6to4 tunnel interface",
        "vmnet" => "Virtual Machine network interface",
        "vnic" => "Parallels/VMware virtual network interface",
        "ppp" => "Point-to-Point Protocol interface",
        "wlan" => "Wireless LAN interface",
        "eth" => "Ethernet interface",
        "docker" => "Docker virtual bridge interface",
        "veth" => "Virtual Ethernet interface",
        _ => return linux_predictable_description(name),
    };
    Some(desc.to_string())
}

/// Decodes Linux's systemd/udev "predictable network interface names"
/// scheme (`en*`/`wl*`/`ww*` followed by a location code), e.g. `enp191s0`,
/// `eno1`, `ens33`, `enx001122334455`. See:
/// <https://www.freedesktop.org/software/systemd/man/latest/systemd.net-naming-scheme.html>
fn linux_predictable_description(name: &str) -> Option<String> {
    let (label, rest) = [
        ("en", "Ethernet"),
        ("wl", "Wireless LAN"),
        ("ww", "Mobile broadband (WWAN)"),
    ]
    .into_iter()
    .find_map(|(type_prefix, label)| name.strip_prefix(type_prefix).map(|rest| (label, rest)))?;

    if rest.is_empty() {
        return None;
    }

    if let Some((bus, slot, function)) = parse_pci_location(rest) {
        return Some(match function {
            Some(f) => format!("{label} interface (PCI bus {bus}, slot {slot}, function {f})"),
            None => format!("{label} interface (PCI bus {bus}, slot {slot})"),
        });
    }
    if let Some(slot) = rest
        .strip_prefix('s')
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Some(format!("{label} interface (hotplug slot {slot})"));
    }
    if let Some(index) = rest
        .strip_prefix('o')
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Some(format!("{label} interface (on-board device #{index})"));
    }
    if rest.starts_with('x') {
        return Some(format!("{label} interface (named by MAC address)"));
    }
    Some(format!("{label} interface"))
}

/// Parses the PCI-geographic-location component of a predictable interface
/// name: `p<bus>s<slot>[f<function>][d<dev_id>]`, e.g. `p191s0` -> bus 191,
/// slot 0. Ignores an optional trailing `d<dev_id>`.
fn parse_pci_location(rest: &str) -> Option<(u32, u32, Option<u32>)> {
    let rest = rest.strip_prefix('p')?;
    let (bus, rest) = split_leading_digits(rest);
    let bus: u32 = bus.parse().ok().filter(|_| !bus.is_empty())?;
    let rest = rest.strip_prefix('s')?;
    let (slot, rest) = split_leading_digits(rest);
    let slot: u32 = slot.parse().ok().filter(|_| !slot.is_empty())?;
    let function = rest
        .strip_prefix('f')
        .and_then(|r| split_leading_digits(r).0.parse().ok());
    Some((bus, slot, function))
}

fn split_leading_digits(s: &str) -> (&str, &str) {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    s.split_at(end)
}

/// Builds the single record batch of interfaces from `pcap`'s device list
fn interfaces_batch(schema: &SchemaRef) -> Result<RecordBatch> {
    let devices = pcap::Device::list().map_err(|e| {
        DataFusionError::External(format!("interfaces failed to list devices: {e}").into())
    })?;

    let mut name = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut addresses = ListBuilder::new(StringBuilder::new());
    let mut is_up = BooleanBuilder::new();
    let mut is_running = BooleanBuilder::new();
    let mut is_loopback = BooleanBuilder::new();
    let mut is_wireless = BooleanBuilder::new();
    let mut connection_status = StringBuilder::new();

    for device in devices {
        name.append_value(&device.name);
        let desc = device
            .desc
            .clone()
            .or_else(|| static_description(&device.name));
        description.append_option(desc.as_deref());
        for address in &device.addresses {
            addresses.values().append_value(address.addr.to_string());
        }
        addresses.append(true);
        is_up.append_value(device.flags.is_up());
        is_running.append_value(device.flags.is_running());
        is_loopback.append_value(device.flags.is_loopback());
        is_wireless.append_value(device.flags.is_wireless());
        connection_status.append_value(match device.flags.connection_status {
            ConnectionStatus::Unknown => "unknown",
            ConnectionStatus::Connected => "connected",
            ConnectionStatus::Disconnected => "disconnected",
            ConnectionStatus::NotApplicable => "not_applicable",
        });
    }

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(name.finish()),
            Arc::new(description.finish()),
            Arc::new(addresses.finish()),
            Arc::new(is_up.finish()),
            Arc::new(is_running.finish()),
            Arc::new(is_loopback.finish()),
            Arc::new(is_wireless.finish()),
            Arc::new(connection_status.finish()),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_static_description_linux_predictable_names() {
        assert_eq!(
            static_description("enp191s0").as_deref(),
            Some("Ethernet interface (PCI bus 191, slot 0)")
        );
        assert_eq!(
            static_description("enp2s0f1").as_deref(),
            Some("Ethernet interface (PCI bus 2, slot 0, function 1)")
        );
        assert_eq!(
            static_description("eno1").as_deref(),
            Some("Ethernet interface (on-board device #1)")
        );
        assert_eq!(
            static_description("ens33").as_deref(),
            Some("Ethernet interface (hotplug slot 33)")
        );
        assert_eq!(
            static_description("enx001122334455").as_deref(),
            Some("Ethernet interface (named by MAC address)")
        );
        assert_eq!(
            static_description("wlp3s0").as_deref(),
            Some("Wireless LAN interface (PCI bus 3, slot 0)")
        );
        assert_eq!(
            static_description("wwp0s20f3").as_deref(),
            Some("Mobile broadband (WWAN) interface (PCI bus 0, slot 20, function 3)")
        );
    }

    #[test]
    fn test_static_description_macos_style_names_unaffected() {
        assert_eq!(
            static_description("en0").as_deref(),
            Some("Ethernet or Wi-Fi adapter")
        );
        assert_eq!(
            static_description("lo0").as_deref(),
            Some("Loopback interface")
        );
        assert_eq!(
            static_description("utun0").as_deref(),
            Some("Utility tunnel interface (VPN, Back to My Mac, etc.)")
        );
    }

    #[test]
    fn test_call_rejects_arguments() {
        let func = InterfacesFunc::default();
        let args = vec![Expr::Literal(
            datafusion::scalar::ScalarValue::Utf8(Some("en0".to_string())),
            None,
        )];
        let err = func.call(&args).unwrap_err();
        assert!(err.to_string().contains("takes no arguments"));
    }

    #[tokio::test]
    async fn test_interfaces_lists_devices() {
        let ctx = SessionContext::new();
        ctx.register_udtf("interfaces", Arc::new(InterfacesFunc::default()));
        let batches = ctx
            .sql("SELECT name, is_loopback, connection_status FROM interfaces()")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Any host running the tests has at least one interface
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows >= 1, "expected at least one interface");
    }

    #[tokio::test]
    async fn test_interfaces_projection_and_filter() {
        let ctx = SessionContext::new();
        ctx.register_udtf("interfaces", Arc::new(InterfacesFunc::default()));
        // Projection, filtering, and unnesting the address list all compose
        let batches = ctx
            .sql(
                "SELECT name, unnest(addresses) AS address \
                 FROM interfaces() WHERE is_up ORDER BY name",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // No assertion on contents: an interface may legitimately have no
        // addresses, this only must not error
        let _ = batches;
    }
}
