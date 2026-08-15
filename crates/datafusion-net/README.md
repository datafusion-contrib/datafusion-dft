# datafusion-net

DataFusion table functions for querying network packet captures with SQL,
similar to wireshark / tshark.

## Table functions

### `pcap(path)`

Reads a pcap or pcapng capture file as a table:

```sql
SELECT src_ip, dst_ip, protocol, length
FROM pcap('capture.pcap')
WHERE dst_port = 443;
```

### `capture(interface [, bpf_filter [, duration_secs]])`

Requires the `live` feature (links against libpcap). Streams live-captured
packets from a network interface:

```sql
-- Stream until 100 packets have been captured
SELECT * FROM capture('en0') LIMIT 100;

-- Apply a BPF filter
SELECT * FROM capture('en0', 'tcp port 443') LIMIT 10;

-- Capture for 10 seconds; the stream terminates, so aggregations work
SELECT src_ip, count(*) FROM capture('en0', '', 10) GROUP BY src_ip;
```

Without a duration the source is unbounded: use a `LIMIT` or the query
streams until cancelled. Live capture requires elevated privileges (sudo,
`cap_net_raw`+`cap_net_admin` on Linux, or ChmodBPF on macOS).

### `pcap_wide(path)` and `capture_wide(interface [, bpf_filter [, duration_secs]])`

Wide variants of `pcap` and `capture` (same arguments, `capture_wide`
requires the `live` feature) that append DNS and geolocation enrichment
columns for the source and destination addresses: `src_host` / `dst_host`
(reverse DNS) and `src_country`, `src_city`, `src_lat`, `src_lon` plus the
`dst_` equivalents (from the `geoip` machinery and a MaxMind-format
database):

```sql
SELECT dst_ip, dst_host, dst_country, count(*) AS packets
FROM pcap_wide('capture.pcap')
GROUP BY dst_ip, dst_host, dst_country
ORDER BY packets DESC;
```

The geolocation database comes from the `GEOIP_DB` environment variable or a
path passed to `PcapWideFunc::new` / `CaptureWideFunc::new` (see the `geoip`
section below for which database populates which fields). A database that is
unconfigured, missing, or unreadable never fails the query — the geolocation
columns are just `NULL` (use `geoip(ip, path)['error']` to see why).
Enrichment is a projection over the narrow table, so unused columns still
prune and only projected enrichment is computed.

### `interfaces()`

Requires the `live` feature. Lists the system's network capture interfaces
(similar to `tshark -D`), one row per interface — useful for discovering
what to pass to `capture`. Listing does not require elevated privileges:

```sql
SELECT name, description, addresses, connection_status
FROM interfaces()
WHERE is_up AND NOT is_loopback;
```

Columns: `name` (`Utf8`), `description` (`Utf8`), `addresses`
(`List<Utf8>`), `is_up`, `is_running`, `is_loopback`, `is_wireless`
(`Boolean`), and `connection_status` (`Utf8`: `connected`, `disconnected`,
`unknown`, or `not_applicable`).

### `tcp_conversations(path)`

Aggregates a capture file into one row per TCP connection (like wireshark's
"Statistics → Conversations" or `tshark -z conv,tcp`) — useful for spotting
slow handshakes, retransmission-heavy flows, and connections that never
closed cleanly:

```sql
SELECT dst_ip, dst_port, handshake_rtt_ms, retransmissions_fwd, state
FROM tcp_conversations('capture.pcap')
ORDER BY retransmissions_fwd DESC;
```

Direction is from the connection initiator's perspective: `src_*` is the
endpoint that sent the first SYN (or, for a capture that starts
mid-connection, the first endpoint seen sending); `fwd` counts
initiator → responder and `rev` the reverse.

| Column                                        | Type                     | Notes                                              |
| --------------------------------------------- | ------------------------ | -------------------------------------------------- |
| `src_ip`, `src_port`, `dst_ip`, `dst_port`    | `Utf8` / `UInt16`        | Initiator and responder endpoints                  |
| `first_timestamp`, `last_timestamp`           | `Timestamp(Microsecond)` | First and last packet seen                          |
| `duration_ms`                                 | `Float64`                | Time between them                                  |
| `packets_fwd`, `packets_rev`                  | `UInt64`                 | Packet counts per direction                        |
| `bytes_fwd`, `bytes_rev`                      | `UInt64`                 | On-the-wire frame bytes per direction              |
| `retransmissions_fwd`, `retransmissions_rev`  | `UInt64`                 | Segments below the highest seen sequence end       |
| `handshake_rtt_ms`                            | `Float64`                | SYN → handshake-completing ACK; NULL if not captured |
| `state`                                       | `Utf8`                   | `active`, `half_closed`, `closed` (both FINs), or `reset` |

## Scalar functions

### `reverse_dns(ip)`

Resolves an IP address string to a hostname via a reverse DNS (PTR) lookup,
using the system resolver. Pairs naturally with the `src_ip` / `dst_ip`
columns:

```sql
SELECT dst_ip, reverse_dns(dst_ip) AS host, count(*) AS packets
FROM capture('en0', 'tcp', 10)
GROUP BY dst_ip, host
ORDER BY packets DESC;
```

Lookups are network I/O, so results are cached process-wide, identical
addresses within a batch are resolved once, and each batch of lookups is
bounded by a timeout. Addresses that fail to parse, fail to resolve, or time
out yield `NULL`.

### `geoip(ip [, db_path])`

Geolocates an IP address using a MaxMind-format (`.mmdb`) database. Returns
a struct with `country_code`, `country`, `city`, `latitude`, `longitude`,
`time_zone`, and `error` fields:

```sql
SELECT geoip(src_ip, '/path/GeoLite2-City.mmdb')['country_code'] AS country,
       count(*) AS packets
FROM pcap('capture.pcap')
GROUP BY country
ORDER BY packets DESC;
```

**Which database?** A single database file serves every field — there is no
per-field database. Which fields are populated depends on the database's
schema:

| Field                                          | City database | Country database | ASN / other |
| ---------------------------------------------- | ------------- | ---------------- | ----------- |
| `country_code`, `country`                      | ✓             | ✓                | `NULL`      |
| `city`, `latitude`, `longitude`, `time_zone`   | ✓             | `NULL`           | `NULL`      |

Use a City-schema database for full coverage: [GeoLite2-City] (free with a
MaxMind account) or the commercial GeoIP2-City. A Country-schema database
(GeoLite2-Country) is smaller if only country-level fields are needed.

The single-argument form uses the database from the `GEOIP_DB` environment
variable (read when the UDF is constructed), or the path passed to
`GeoIpUdf::with_db_path`. Opened databases are cached process-wide.

Addresses that fail to parse or have no entry in the database yield a `NULL`
struct. A database that is missing, unreadable, or unconfigured does not
fail the query: the location fields are `NULL` and the `error` field (which
is `NULL` on success) carries the reason:

```sql
SELECT geoip(src_ip)['error'] FROM pcap('capture.pcap') LIMIT 1;
-- geoip failed to open database '/path/GeoLite2-City.mmdb': ...
```

[GeoLite2-City]: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data

### `dns_query(payload)`

Decodes a DNS message from a packet payload (typically UDP port 53 — DNS
over UDP fits in a single datagram, so no reassembly is needed). Returns a
struct with `is_response` (`Boolean`), `name` and `query_type` (`Utf8`, from
the first question), `response_code` (`Utf8`, NULL for queries), and
`answers` (`List<Utf8>`, NULL for queries). Answers include A/AAAA addresses
and CNAME/NS/PTR names; other record types are skipped. Payloads that do not
parse as DNS yield a NULL struct.

```sql
SELECT dns_query(payload)['name'] AS name, count(*) AS queries
FROM pcap('capture.pcap')
WHERE dst_port = 53
GROUP BY name ORDER BY queries DESC;
```

### `tls_sni(payload)`

Extracts the Server Name Indication (SNI) host name from a TLS ClientHello
in a packet payload (typically TCP port 443), or NULL when the payload is
not a ClientHello or carries no SNI. In HTTPS traffic the payloads are
encrypted, but the ClientHello — the first packet the client sends — is not,
so this reveals the destination host of otherwise opaque connections:

```sql
SELECT tls_sni(payload) AS host, count(*) AS hellos
FROM pcap('capture.pcap')
WHERE tls_sni(payload) IS NOT NULL
GROUP BY host ORDER BY hellos DESC;
```

The ClientHello must fit in the payload (it usually does); this does not
reassemble segments.

## Schema

One row per captured frame. Layers that cannot be decoded (non-IP traffic,
truncated frames, unknown link types) yield null columns rather than errors.

| Column           | Type                     | Notes                                    |
| ---------------- | ------------------------ | ---------------------------------------- |
| `timestamp`      | `Timestamp(Microsecond)` | Capture timestamp                        |
| `frame_number`   | `UInt64`                 | 1-based position in the capture          |
| `length`         | `UInt32`                 | Original frame length on the wire        |
| `capture_length` | `UInt32`                 | Bytes actually captured                  |
| `eth_src`        | `Utf8`                   | Source MAC                               |
| `eth_dst`        | `Utf8`                   | Destination MAC                          |
| `ethertype`      | `Utf8`                   | `ipv4`, `ipv6`, `arp`, or hex value      |
| `vlan`           | `UInt16`                 | VLAN identifier (outer tag if double)    |
| `ip_version`     | `UInt8`                  | 4 or 6                                   |
| `src_ip`         | `Utf8`                   |                                          |
| `dst_ip`         | `Utf8`                   |                                          |
| `ttl`            | `UInt8`                  | Hop limit for IPv6                       |
| `protocol`       | `Utf8`                   | `tcp`, `udp`, `icmp`, `icmpv6`, ...      |
| `src_port`       | `UInt16`                 |                                          |
| `dst_port`       | `UInt16`                 |                                          |
| `tcp_flags`      | `Utf8`                   | e.g. `SYN\|ACK`                          |
| `tcp_seq`        | `UInt32`                 |                                          |
| `tcp_ack`        | `UInt32`                 |                                          |
| `tcp_window`     | `UInt16`                 | Receive window size (unscaled)           |
| `payload_length` | `UInt32`                 | Transport payload bytes                  |
| `payload`        | `Binary`                 | Only materialized when projected         |

## Usage

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();
ctx.register_udtf("pcap", Arc::new(datafusion_net::PcapFunc::default()));
// With the `live` feature:
// ctx.register_udtf("capture", Arc::new(datafusion_net::CaptureFunc::default()));
```
