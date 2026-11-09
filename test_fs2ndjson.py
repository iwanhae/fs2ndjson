#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_fs2ndjson.py — synthetic round-trip tests for fs2ndjson.py.

Builds Firestore-style export files (LevelDB log framing + EntityProto records)
that force every supported value type and edge case, converts them, and asserts
on the decoded rows. Run:  python3 test_fs2ndjson.py
"""

import base64
import datetime
import json
import math
import os
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fs2ndjson as fs

BLOCK = 32 * 1024

# --- minimal protobuf encoder -------------------------------------------------
MASK64 = (1 << 64) - 1


def wv(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def tag(f, w):
    return wv(f << 3 | w)


def ld(f, b):
    return tag(f, 2) + wv(len(b)) + b


def vi(f, n):
    return tag(f, 0) + wv(n & MASK64)  # negative -> 10-byte two's complement


def fx(f, d):
    return tag(f, 1) + struct.pack("<d", d)


def grp(f, b):
    return tag(f, 3) + b + tag(f, 4)


def key(app, elems, dbid="db"):
    body = ld(13, app.encode()) if app else b""
    body += ld(14, b"".join(
        grp(1, ld(2, k.encode()) +
            (vi(3, i) if isinstance(i, int) else ld(4, i.encode())))
        for k, i in elems))
    if dbid:
        body += ld(23, dbid.encode())
    return ld(13, body)


def prop(name, value, meaning=0, multiple=0, raw=False):
    body = (vi(1, meaning) if meaning else b"") \
        + ld(3, name.encode()) + vi(4, multiple) + ld(5, value)
    return ld(15 if raw else 14, body)


V_INT = lambda n: vi(1, n)
V_BOOL = lambda b: vi(2, 1 if b else 0)
V_STR = lambda s: ld(3, s.encode())
V_DBL = lambda d: fx(4, d)
V_TS = lambda us: vi(1, us)
V_MAP = lambda inner: ld(3, inner)


def wrapped_scalar(marker, value_msg):
    """Array scalar wrapper: map entity with a single raw marker property."""
    return ld(15, ld(3, marker.encode()) + vi(4, 0) + ld(5, value_msg))


def write_export(path, entities):
    """Serialize entities with proper LevelDB log framing (32 KiB blocks)."""
    with open(path, "wb") as f:
        for payload in entities:
            maxc = BLOCK - 7
            if len(payload) <= maxc:
                chunks = [(1, payload)]
            else:
                chunks = [(2, payload[:maxc])]
                rest = payload[maxc:]
                while len(rest) > maxc:
                    chunks.append((3, rest[:maxc]))
                    rest = rest[maxc:]
                chunks.append((4, rest))
            for t, c in chunks:
                pos = f.tell()
                rem = BLOCK - pos % BLOCK
                if rem < 7:
                    f.write(b"\x00" * rem)
                f.write(struct.pack("<IHB", 0, len(c), t) + c)


def convert(entities):
    """Frame entities to a temp file and convert them with the Converter."""
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "output-0")
        write_export(path, entities)
        assert not fs.looks_like_datastore_export([path])
        conv = fs.Converter()
        rows = []
        for record in fs.iter_leveldb_records(path):
            kind, row = conv.record_to_row(record)
            assert kind is not None, "record failed to decode"
            rows.append((kind, row))
        return rows


def iso(us):
    dt = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt + datetime.timedelta(microseconds=us)) \
        .strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def test_all_types():
    ts_past = -252460800000000       # 1962-02-ish (negative varint path)
    ts_now = 1786042393926000        # 2026-08-06
    ts_far = 253402300800000000      # beyond year 9999 -> raw fallback
    # Reference stored as a bare Reference message (field 13 app + 14 path)
    ref_body = ld(13, b"proj") + ld(14,
                                    grp(1, ld(2, b"users") + ld(4, b"u1")))

    doc = key("proj", [("alltypes", "doc1")]) + b"".join([
        prop("s", V_STR("héllo wörld ✓")),
        prop("empty_str", V_STR("")),
        prop("neg", V_INT(-5)),
        prop("big", V_INT(1 << 62)),
        prop("d", V_DBL(3.25)),
        prop("nan", V_DBL(float("nan"))),
        prop("inf", V_DBL(float("inf"))),
        prop("ninf", V_DBL(float("-inf"))),
        prop("b_true", V_BOOL(True)),
        prop("b_false", V_BOOL(False)),
        prop("n", b""),  # meaning 0 with no value fields -> null
        prop("ts_past", V_TS(ts_past), meaning=7),
        prop("ts_now", V_TS(ts_now), meaning=7),
        prop("ts_far", V_TS(ts_far), meaning=7),
        prop("m", V_MAP(  # nested map, 3 levels deep
            key(None, [], dbid=None) +
            prop("x", V_INT(1)) +
            prop("inner", V_MAP(
                key(None, [], dbid=None) +
                prop("deep", V_STR("leaf"))), meaning=19)), meaning=19),
        prop("arr_str", V_STR("a"), multiple=1),
        prop("arr_str", V_STR("b"), multiple=1),
        prop("arr_int", V_MAP(wrapped_scalar("__int__", V_INT(5))),
             meaning=19, multiple=1),
        prop("arr_int", V_MAP(wrapped_scalar("__int__", V_INT(685))),
             meaning=19, multiple=1),
        prop("arr_map", V_MAP(key(None, [], dbid=None) + prop("x", V_INT(1))),
             meaning=19, multiple=1),
        prop("arr_map", V_MAP(key(None, [], dbid=None) + prop("x", V_INT(2))),
             meaning=19, multiple=1),
        prop("empty_arr", b"", meaning=24),
        prop("bytes_utf8", V_STR("plain text"), meaning=16),
        prop("bytes_bin", ld(3, b"\xff\xfe\x00"), meaning=14),
        prop("geo", ld(5, fx(1, 37.5) + fx(2, 127.04)), meaning=9),
        prop("ref", ld(3, ref_body), meaning=18),
        prop("ref_wrapped", ld(3, key("proj", [("users", "u2")], dbid=None)),
             meaning=18),  # EntityProto-wrapped spelling of the same thing
        prop("__id__", V_STR("user-value")),  # metadata column collision
    ])

    big_payload = "x" * 100_000  # forces FIRST/MIDDLE/LAST record reassembly
    big = key("proj", [("bigdocs", "big1")]) + prop("blob", V_STR(big_payload))

    kind, row = convert([doc])[0]
    assert kind == "alltypes"
    assert row["s"] == "héllo wörld ✓" and row["empty_str"] == ""
    assert row["neg"] == -5 and isinstance(row["neg"], int)
    assert row["big"] == 1 << 62
    assert row["d"] == 3.25
    assert row["nan"] == "NaN" and row["inf"] == "Infinity" \
        and row["ninf"] == "-Infinity"
    assert row["b_true"] is True and row["b_false"] is False
    assert row["n"] is None
    assert row["ts_past"] == iso(ts_past), row["ts_past"]
    assert row["ts_now"] == iso(ts_now)
    assert row["ts_far"] == str(ts_far)  # overflow fallback
    assert row["m"] == {"x": 1, "inner": {"deep": "leaf"}}
    assert row["arr_str"] == ["a", "b"]
    assert row["arr_int"] == [5, 685], row["arr_int"]
    assert row["arr_map"] == [{"x": 1}, {"x": 2}]
    assert row["empty_arr"] == []
    assert row["bytes_utf8"] == "plain text"
    assert row["bytes_bin"] == base64.b64encode(b"\xff\xfe\x00").decode()
    assert row["geo"] == {"latitude": 37.5, "longitude": 127.04}
    assert row["ref"] == "users/u1"
    assert row["ref_wrapped"] == "users/u2"
    # metadata columns + collision handling
    assert row["__id__"] == "doc1" and row["__id__user"] == "user-value"
    assert row["__path__"] == "alltypes/doc1" and row["__parent__"] == ""
    # the whole row must be strict-JSON serializable
    json.dumps(row, allow_nan=False)

    kind, row = convert([big])[0]
    assert kind == "bigdocs" and len(row["blob"]) == 100_000
    print("ok  all types, arrays, NaN/Inf, collision, 100 KB record reassembly")


def test_paths_and_ids():
    numeric = key("proj", [("posts", 42)]) + prop("title", V_STR("numeric id"))
    sub = key("proj", [("users", "u1"), ("posts", "p2")]) \
        + prop("body", V_STR("subcollection doc"))

    (k1, r1), (k2, r2) = convert([numeric, sub])
    assert (k1, r1["__id__"], r1["__path__"]) == ("posts", "42", "posts/42")
    assert k2 == "posts"  # file is per collection group (leaf kind)
    assert r2["__parent__"] == "users/u1"
    assert r2["__path__"] == "users/u1/posts/p2"
    print("ok  numeric document IDs, subcollection paths, __parent__")


def test_datastore_detection():
    # Datastore v1 layout: name in field 2, value in field 4.
    ds_entity = key("ds", [("posts", "p1")]) + ld(14, ld(2, b"title")
                                                   + ld(4, ld(3, b"hello")))
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "output-0")
        write_export(path, [ds_entity] * 20)
        assert fs.looks_like_datastore_export([path])
    print("ok  Datastore v1 layout detected and rejected")


def test_discovery():
    with tempfile.TemporaryDirectory() as td:
        root = os.path.join(td, "20260101T000000_00000")
        os.makedirs(os.path.join(root, "all_namespaces", "all_kinds"))
        open(os.path.join(root, "20260101T000000_00000.overall_export_metadata"),
             "w").close()
        write_export(os.path.join(root, "all_namespaces", "all_kinds",
                                  "output-0"), [b""])
        files = fs.discover_output_files(root)
        assert len(files) == 1 and files[0].endswith("output-0")
    print("ok  export discovery from root directory")


if __name__ == "__main__":
    test_all_types()
    test_paths_and_ids()
    test_datastore_detection()
    test_discovery()
    print("\nALL TESTS PASSED")
