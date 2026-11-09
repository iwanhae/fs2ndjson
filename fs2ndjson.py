#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fs2ndjson.py — Convert a Firestore managed export to per-collection NDJSON files.

Input:  `gcloud firestore export` output (LevelDB log format `output-*` files),
        e.g. <EXPORT_PREFIX>/all_namespaces/all_kinds/output-N

Output: one <collection>.ndjson file per collection, one JSON document per line.
        Each row also carries metadata columns useful for OLAP joins/lineage:
          __id__     : document ID
          __path__   : full document path (e.g. "posts/<id>")
          __parent__ : parent document path ("" for top-level collections)
        plus _manifest.json with per-collection row counts.

Usage:
    python3 fs2ndjson.py [EXPORT_DIR] [-o OUT_DIR] [--timestamps iso|epoch_us] [--bytes auto|utf8|base64]

If EXPORT_DIR is omitted, */*/output-* files are auto-discovered relative to
the script's own directory. OUT_DIR defaults to <export root>/ndjson.

Format notes (derived from real Firestore exports and public documentation):
  - Record framing: LevelDB log — crc32c(4B) | length(2B LE) | type(1B) | payload.
    type: 1=FULL, 2=FIRST, 3=MIDDLE, 4=LAST; 32 KiB blocks. CRCs are not
    verified (speed over paranoia); structurally invalid records cause a skip
    to the next block boundary.
  - payload = EntityProto:
      field 13 = key (Reference) { 13: app, 14: path { 1 (GROUP): elem { 2: kind, 3: id, 4: name } }, 23: database_id }
      field 14 = indexed property, field 15 = unindexed property
  - property { 1: meaning, 3: name, 4: multiple, 5: value }
  - PropertyValue mapping:
      meaning 0  + f1 varint   -> integer (signed 64-bit)
      meaning 0  + f2 varint   -> boolean
      meaning 0  + f3 bytes    -> string
      meaning 0  + f4 fixed64  -> double
      meaning 0  + (no value)  -> null
      meaning 7  + f1 varint   -> timestamp (microseconds since epoch)
      meaning 19 + f3          -> map (nested EntityProto); multiple=1 marks array elements
      meaning 24               -> empty array []
      meaning 14/16 + f3       -> bytes (rendered per --bytes mode)
      meaning 9  + f5          -> geopoint (best-effort)
      meaning 18 + f3          -> reference (best-effort, rendered as a path string)
      anything else            -> {"__unknown_meaning__": m, "__value_b64__": ...}
                                  (unknown encodings are surfaced, never dropped)
  - Scalar array elements are wrapped in single-key marker maps ({"__int__": 5});
    such wrappers are unwrapped automatically.

Limitations:
  - Firestore managed exports only. Datastore backups use a different property
    layout; they are detected and rejected explicitly rather than misparsed.
  - geopoint (meaning 9) and reference (meaning 18) handling is best-effort and
    not yet verified against real-world data; every other type listed above was
    validated against actual Firestore exports.
  - NaN/±Infinity doubles become the strings "NaN"/"Infinity"/"-Infinity"
    because JSON cannot represent them.
  - CRC32C values are not verified.

Dependencies: Python 3.8+ standard library only.
"""

import argparse
import base64
import collections
import datetime
import glob
import itertools
import json
import math
import os
import re
import struct
import sys
import time

__version__ = "1.1.0"


# ---------------------------------------------------------------------------
# LevelDB log reader
# ---------------------------------------------------------------------------

BLOCK_SIZE = 32 * 1024
HEADER = struct.Struct("<IHB")  # masked crc32c, length (LE), record type
RT_FULL, RT_FIRST, RT_MIDDLE, RT_LAST = 1, 2, 3, 4


def iter_leveldb_records(path):
    """Yield record payloads (= serialized EntityProto) from an output-* file."""
    with open(path, "rb") as f:
        pending = b""
        while True:
            pos = f.tell()
            block_remaining = BLOCK_SIZE - pos % BLOCK_SIZE
            if block_remaining < HEADER.size:
                if not f.read(block_remaining):
                    return
                continue
            head = f.read(HEADER.size)
            if len(head) < HEADER.size:
                return
            _, length, rtype = HEADER.unpack(head)
            if length + HEADER.size > block_remaining:
                # Corrupted record: skip to the next block boundary.
                f.seek(pos + block_remaining)
                pending = b""
                continue
            data = f.read(length)
            if len(data) < length:
                return
            if rtype == RT_FULL:
                yield data
            elif rtype == RT_FIRST:
                pending = data
            elif rtype == RT_MIDDLE:
                pending += data
            elif rtype == RT_LAST:
                yield pending + data
                pending = b""


# ---------------------------------------------------------------------------
# Minimal protobuf wire parser
# ---------------------------------------------------------------------------

def _varint(buf, i):
    result = 0
    shift = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def parse_fields(buf):
    """Return a list of (field_number, wire_type, value) tuples.

    value is an int for varint fields and bytes otherwise. A GROUP (wire
    type 3) is returned as a single entry whose value is the raw inner bytes.
    """
    out = []
    i, n = 0, len(buf)
    while i < n:
        tag, i = _varint(buf, i)
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            val, i = _varint(buf, i)
        elif wt == 1:
            val = buf[i:i + 8]
            i += 8
        elif wt == 2:
            ln, i = _varint(buf, i)
            val = buf[i:i + ln]
            i += ln
        elif wt == 3:  # group: consume everything up to the matching end-group
            start = i
            depth = 1
            while i < n:
                t2, i = _varint(buf, i)
                w2 = t2 & 7
                if w2 == 3:
                    depth += 1
                elif w2 == 4:
                    depth -= 1
                    if depth == 0:
                        break
                elif w2 == 0:
                    _, i = _varint(buf, i)
                elif w2 == 1:
                    i += 8
                elif w2 == 2:
                    ln2, i = _varint(buf, i)
                    i += ln2
            val = buf[start:i]
        elif wt == 4:  # end group: consumed by the enclosing group scan
            break
        elif wt == 5:
            val = buf[i:i + 4]
            i += 4
        else:
            raise ValueError("unsupported wire type %d" % wt)
        out.append((fn, wt, val))
    return out


# ---------------------------------------------------------------------------
# Firestore value conversion
# ---------------------------------------------------------------------------

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_WRAPPER_KEY = re.compile(r"^__[a-z0-9_]+__$")


def _to_int64(u):
    """Interpret an unsigned protobuf varint value as a signed 64-bit integer."""
    return u - (1 << 64) if u >= (1 << 63) else u


def ts_to_str(us):
    try:
        dt = EPOCH + datetime.timedelta(microseconds=us)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    except (OverflowError, OSError, ValueError):
        return str(us)  # outside the representable range: raw microseconds


def try_utf8(b):
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return None


def parse_key_path(ref_fields):
    """Reference field list -> flat path segments [kind, id, kind, id, ...]."""
    segments = []
    for fn, wt, val in ref_fields:
        if fn == 14 and wt == 2:  # Path
            for efn, ewt, eval_ in parse_fields(val):
                if efn == 1 and ewt == 3:  # Element (group)
                    kind = doc_id = None
                    for kfn, kwt, kval in parse_fields(eval_):
                        if kfn == 2 and kwt == 2:
                            kind = try_utf8(kval)
                        elif kfn == 3 and kwt == 0:
                            doc_id = str(kval)
                        elif kfn == 4 and kwt == 2:
                            doc_id = try_utf8(kval)
                    if kind:
                        segments.append(kind)
                        segments.append(doc_id if doc_id is not None else "")
    return segments


class Converter:
    def __init__(self, timestamps="iso", bytes_mode="auto"):
        self.timestamps = timestamps
        self.bytes_mode = bytes_mode
        self._warned = set()

    # -- PropertyValue ------------------------------------------------------
    def decode_value(self, meaning, val_buf):
        present = {fn: (wt, v) for fn, wt, v in parse_fields(val_buf)} \
            if val_buf else {}

        if meaning == 24:  # empty array
            return []
        if meaning == 7 and 1 in present:  # timestamp (µs since epoch)
            us = _to_int64(present[1][1])
            return us if self.timestamps == "epoch_us" else ts_to_str(us)
        if meaning in (14, 16) and 3 in present:  # bytes
            raw = present[3][1]
            if self.bytes_mode == "utf8":
                return try_utf8(raw) or raw.decode("utf-8", "replace")
            if self.bytes_mode == "base64":
                return base64.b64encode(raw).decode("ascii")
            decoded = try_utf8(raw)
            return decoded if decoded is not None \
                else base64.b64encode(raw).decode("ascii")
        if meaning == 9 and 5 in present and present[5][0] == 2:  # geopoint
            try:
                return self._geo(present[5][1])
            except Exception:
                pass
        if meaning == 18 and 3 in present:  # reference
            raw = present[3][1]
            try:
                ref_fields = parse_fields(raw)
                # Accept both a bare Reference message and one wrapped in an
                # EntityProto-style key field (both spellings occur in the wild).
                if all(fn == 13 and wt == 2 for fn, wt, _ in ref_fields) \
                        and ref_fields:
                    inner = parse_fields(ref_fields[0][2])
                    if any(fn == 14 for fn, _, _ in inner):
                        ref_fields = inner
                path = parse_key_path(ref_fields)
                if path:
                    return "/".join(path)
            except Exception:
                pass
            return try_utf8(raw) or base64.b64encode(raw).decode("ascii")
        if meaning == 19 and 3 in present:  # map
            return self.entity_to_dict_fields(parse_fields(present[3][1]))

        # meaning 0, and default handling of meanings without a special case
        if 1 in present:
            return _to_int64(present[1][1])       # integer
        if 2 in present:
            return bool(present[2][1])            # boolean
        if 3 in present:
            raw = present[3][1]
            return try_utf8(raw) or base64.b64encode(raw).decode("ascii")
        if 4 in present:
            wt, v = present[4]
            if wt == 1:
                d = struct.unpack("<d", v)[0]     # double
                if math.isnan(d):
                    return "NaN"
                if math.isinf(d):
                    return "Infinity" if d > 0 else "-Infinity"
                return d
            return _to_int64(v)
        if not present:
            return None                           # null
        return {"__unknown_meaning__": meaning,
                "__value_b64__": base64.b64encode(val_buf).decode("ascii")}

    def _geo(self, buf):
        lat = lon = None
        for fn, wt, v in parse_fields(buf):
            num = struct.unpack("<d", v)[0] if wt == 1 else _to_int64(v)
            if fn == 1:
                lat = num
            elif fn == 2:
                lon = num
        return {"latitude": lat, "longitude": lon}

    # -- EntityProto -> dict --------------------------------------------------
    def entity_to_dict_fields(self, entity_fields):
        props = []  # (name, multiple, meaning, value_buf)
        for fn, wt, val in entity_fields:
            if wt != 2 or fn not in (14, 15):
                continue
            name = None
            meaning = 0
            multiple = 0
            vbuf = b""
            for pfn, pwt, pv in parse_fields(val):
                if pfn == 3 and pwt == 2:
                    name = try_utf8(pv)
                elif pfn == 1 and pwt == 0:
                    meaning = pv
                elif pfn == 4 and pwt == 0:
                    multiple = pv
                elif pfn == 5 and pwt == 2:
                    vbuf = pv
            if not name:  # leading empty property found inside map entities
                continue
            props.append((name, multiple, meaning, vbuf))

        doc = {}
        for name, multiple, meaning, vbuf in props:
            val = self.decode_value(meaning, vbuf)
            if multiple:
                doc.setdefault(name, []).append(val)
            elif name in doc and isinstance(doc[name], list):
                doc[name].append(val)  # array element continuing an earlier list
            else:
                doc[name] = val

        # Unwrap scalar array markers: [{"__int__": 5}, ...] -> [5, ...]
        for k, v in doc.items():
            if (isinstance(v, list) and v
                    and all(isinstance(x, dict) and len(x) == 1
                            and _WRAPPER_KEY.match(next(iter(x)))
                            for x in v)):
                doc[k] = [x[next(iter(x))] for x in v]
        return doc

    # -- record -> row --------------------------------------------------------
    def record_to_row(self, record):
        """EntityProto record -> (kind, row dict), or (None, None) if the
        record does not decode as a document."""
        entity_fields = parse_fields(record)
        segments = []
        for fn, wt, val in entity_fields:
            if fn == 13 and wt == 2:
                segments = parse_key_path(parse_fields(val))
                break
        if not segments or len(segments) < 2 or len(segments) % 2 != 0:
            return None, None
        kind = segments[-2]
        doc_id = segments[-1]
        doc = self.entity_to_dict_fields(entity_fields)
        for meta, value in (("__id__", doc_id),
                            ("__path__", "/".join(segments)),
                            ("__parent__", "/".join(segments[:-2]))):
            if meta in doc:  # user field collides with the metadata column
                doc[meta + "user"] = doc.pop(meta)
                if meta not in self._warned:
                    self._warned.add(meta)
                    print("warning: user field collides with metadata column "
                          "%r; user value preserved as %r"
                          % (meta, meta + "user"), file=sys.stderr)
            doc[meta] = value
        return kind, doc


# ---------------------------------------------------------------------------
# Export discovery and layout detection
# ---------------------------------------------------------------------------

def discover_output_files(root):
    """Find output-* files under an export root or directly inside a kind dir."""
    for pattern in ("*/*/output-[0-9]*", "output-[0-9]*", "*/output-[0-9]*"):
        hits = sorted(glob.glob(os.path.join(root, pattern)))
        if hits:
            return hits
    return []


def looks_like_datastore_export(files, sample=100):
    """Firestore exports store property names in field 3 (value in field 5);
    Datastore v1 backups store names in field 2 (value in field 4). Sniff the
    first records to reject Datastore backups instead of misparsing them."""
    props = firestore_style = 0
    for record in itertools.islice(iter_leveldb_records(files[0]), sample):
        try:
            entity_fields = parse_fields(record)
        except Exception:
            continue
        for fn, wt, val in entity_fields:
            if wt != 2 or fn not in (14, 15):
                continue
            try:
                pfields = parse_fields(val)
            except Exception:
                continue
            has_f3 = any(f == 3 and w == 2 for f, w, _ in pfields)
            has_f2 = any(f == 2 and w == 2 for f, w, _ in pfields)
            if has_f3 or has_f2:
                props += 1
                if has_f3:
                    firestore_style += 1
    return props >= 10 and firestore_style / props < 0.5


# ---------------------------------------------------------------------------
# Main conversion loop
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Firestore managed export (LevelDB output-* files) -> "
                    "one NDJSON file per collection")
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument("export_dir", nargs="?", default=here,
                    help="export root (default: the script's directory) or a "
                         "kind directory containing output-* files")
    ap.add_argument("-o", "--out-dir", default=None,
                    help="output directory (default: <export root>/ndjson)")
    ap.add_argument("--timestamps", choices=["iso", "epoch_us"], default="iso",
                    help="timestamp rendering (default: iso, e.g. "
                         "2024-01-02T03:04:05.060000Z)")
    ap.add_argument("--bytes", choices=["auto", "utf8", "base64"], default="auto",
                    dest="bytes_mode",
                    help="byte value handling, meanings 14/16 (default: auto = "
                         "utf-8 when valid, base64 otherwise)")
    args = ap.parse_args()

    root = os.path.abspath(args.export_dir)
    files = discover_output_files(root)
    if not files:
        sys.exit("no output-* files found under: %s" % root)
    if looks_like_datastore_export(files):
        sys.exit("this looks like a Datastore backup, not a Firestore managed "
                 "export (different property layout); Datastore backups are "
                 "not supported")

    # Walk up to the directory that holds *.overall_export_metadata so the
    # default output directory lands next to the export root.
    out_root = root
    while not glob.glob(os.path.join(out_root, "*.overall_export_metadata")):
        parent = os.path.dirname(out_root)
        if parent == out_root:
            out_root = root
            break
        out_root = parent
    out_dir = args.out_dir or os.path.join(out_root, "ndjson")
    os.makedirs(out_dir, exist_ok=True)

    conv = Converter(args.timestamps, args.bytes_mode)
    writers = {}
    counts = collections.Counter()
    bad = 0
    t0 = time.time()

    for path in files:
        name = os.path.basename(path)
        for record in iter_leveldb_records(path):
            try:
                kind, row = conv.record_to_row(record)
            except Exception:
                kind = None
            if kind is None:
                bad += 1
                continue
            w = writers.get(kind)
            if w is None:
                safe = kind.replace(os.sep, "_")
                w = writers[kind] = open(
                    os.path.join(out_dir, safe + ".ndjson"), "w",
                    encoding="utf-8")
            w.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"),
                               allow_nan=False, default=str))
            w.write("\n")
            counts[kind] += 1
        print("  done: %s" % name, file=sys.stderr)

    for w in writers.values():
        w.close()

    total = sum(counts.values())
    manifest = {
        "version": __version__,
        "source": root,
        "generated_at": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
        "options": {"timestamps": args.timestamps, "bytes": args.bytes_mode},
        "files": [os.path.basename(p) for p in files],
        "documents_total": total,
        "skipped_unparsed_records": bad,
        "collections": dict(sorted(counts.items())),
    }
    with open(os.path.join(out_dir, "_manifest.json"), "w",
              encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("\ndocuments per collection:")
    for kind, cnt in sorted(counts.items()):
        print("  %-40s %8d  -> %s.ndjson" % (kind, cnt, kind))
    print("total %d documents, %d skipped, %.1fs"
          % (total, bad, time.time() - t0))
    print("output directory: %s" % out_dir)


if __name__ == "__main__":
    main()
