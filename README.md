# fs2ndjson

Convert a Firestore managed export (`gcloud firestore export` output — the
binary LevelDB-log `output-*` files) into **one NDJSON file per collection**,
ready for OLAP loading (DuckDB, BigQuery, ClickHouse, Athena, ...).

Firestore does not export JSON; managed exports are LevelDB-log files wrapping
protobuf `EntityProto` records. This tool decodes them with **zero
dependencies** (Python 3.8+ standard library only).

## Features

- **Streaming** — converts a 470 MB export (115k documents) in ~40 s
- **Full type support**: strings, signed 64-bit ints, doubles, booleans, null,
  timestamps (microsecond precision, ISO 8601 or epoch µs), nested maps,
  arrays (including the `__int__`-style scalar wrapper quirk), bytes
  (utf-8/base64), empty arrays
- **Metadata columns** on every row for joins and lineage:
  `__id__`, `__path__`, `__parent__`
- **Safe by default**: unknown value encodings are surfaced as
  `{"__unknown_meaning__": ...}`, never silently dropped; NaN/±Infinity become
  JSON-safe strings; user fields colliding with metadata columns are preserved
- **Rejects Datastore backups** explicitly (they use a different property
  layout) instead of misparsing them
- Emits `_manifest.json` with per-collection row counts

## Usage

```bash
gcloud firestore export gs://<bucket>          # produces the export
gcloud storage cp -r gs://<bucket>/<prefix> .  # download it

python3 fs2ndjson.py <export_prefix>           # writes <export_prefix>/ndjson/
```

Options:

```
-o, --out-dir DIR        output directory (default: <export root>/ndjson)
--timestamps iso|epoch_us  timestamp rendering (default: iso)
--bytes auto|utf8|base64  byte value handling (default: auto)
```

Then load straight into DuckDB:

```sql
SELECT * FROM read_ndjson_auto('ndjson/posts.ndjson');
```

## Tests

```bash
python3 test_fs2ndjson.py
```

Builds synthetic export files covering every value type plus edge cases
(negative int64s, NaN/±Infinity, 100 KB records spanning LevelDB blocks,
subcollection paths, Datastore-layout detection) and asserts on the output.

## Limitations

- Firestore managed exports only (Datastore backups are detected and rejected).
- geopoint (`meaning 9`) and reference (`meaning 18`) decoding is best-effort;
  it follows the observed format conventions but was not verifiable against
  the export this tool was developed on. Every other type was validated on
  real data.
- Record CRC32C values are not verified (speed over paranoia; structurally
  invalid blocks are skipped).

## License

MIT — see LICENSE.
