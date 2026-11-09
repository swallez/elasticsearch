#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow"]
# ///
"""
Benchmarks ingestion of NYC yellow taxi trips (Parquet) into a local, unauthenticated Elasticsearch
(http://localhost:9200) with two ingestion paths, on two kinds of indices:

  * ndjson : classic `POST /{index}/_bulk` with newline-delimited JSON, one request per batch
  * arrow  : `POST /_arrow/{index}/_bulk` with Arrow IPC streams holding several record batches each, sent with chunked
             transfer encoding (one HTTP chunk per record batch), so that the server can coalesce and pipeline its bulk requests

No request body exceeds --max-request-mb (default 100, in decimal MB, which stays under the 100 MiB default of the server's
`http.max_content_length`, that applies to streamed requests too). When a batch would make a request too large, the request is
closed early: ndjson requests then hold fewer rows, and the file is sent as several Arrow streams.

  * standard : regular index (index.mode=standard)
  * columnar : columnar index (index.mode=columnar)

--batch-size takes a comma-separated list: it is the number of rows per bulk request for ndjson, and per record batch (and per
HTTP chunk) for arrow. Every batch size is run for every combination.

For every combination, the target index is deleted and recreated, then the file is read and ingested. Two durations are reported:

  * total  : end-to-end, i.e. reading the Parquet file, building the request bodies and sending them
  * ingest : only sending the requests, once the bodies are fully built in memory

Usage:
    ./benchmark.py [--runs 3] [--batch-size 1000,10000,50000] [--limit N] [--paths ndjson,arrow] [--modes standard,columnar]
"""

import argparse
import datetime
import http.client
import json
import statistics
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
DATA_FILE = Path(__file__).resolve().parent / "data" / "yellow_tripdata_2026-01.parquet"
ES_URL = "http://localhost:9200"
ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"


# ---------------------------------------------------------------------------------------------------------------------
# Elasticsearch client (stdlib only, one persistent connection)

class Es:
    def __init__(self, url):
        parsed = urlparse(url)
        self.host, self.port = parsed.hostname, parsed.port or 9200
        self.conn = http.client.HTTPConnection(self.host, self.port, timeout=600)

    def request(self, method, path, body=None, content_type="application/json", accept="application/json", chunked=False):
        """`body` is bytes, or a list of bytes sent as HTTP chunks when `chunked` is true."""
        headers = {"Accept": accept}
        if body is not None:
            headers["Content-Type"] = content_type
        try:
            self.conn.request(method, path, body=body, headers=headers, encode_chunked=chunked)
            resp = self.conn.getresponse()
            data = resp.read()
        except (http.client.HTTPException, OSError):
            # Reconnect once on a stale connection
            self.conn.close()
            self.conn = http.client.HTTPConnection(self.host, self.port, timeout=600)
            self.conn.request(method, path, body=body, headers=headers, encode_chunked=chunked)
            resp = self.conn.getresponse()
            data = resp.read()
        return resp.status, data

    def json(self, method, path, body=None):
        status, data = self.request(method, path, json.dumps(body) if body is not None else None)
        if status >= 300:
            raise RuntimeError(f"{method} {path} failed with {status}: {data[:1000]!r}")
        return json.loads(data) if data else {}


# ---------------------------------------------------------------------------------------------------------------------
# Data

def download(url, target: Path):
    if target.exists() and target.stat().st_size > 0:
        print(f"Using existing {target} ({target.stat().st_size / 1e6:.1f} MB)")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}")
    tmp = target.with_suffix(".part")
    start = time.perf_counter()
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    tmp.rename(target)
    print(f"Downloaded {target.stat().st_size / 1e6:.1f} MB in {time.perf_counter() - start:.1f}s")


def es_type(t: pa.DataType) -> str:
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_integer(t):
        return "long" if t.bit_width == 64 or pa.types.is_unsigned_integer(t) and t.bit_width >= 32 else "integer"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return "date"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "keyword"
    raise ValueError(f"Unsupported Parquet type {t}")


def index_body(schema: pa.Schema, mode: str, batch_indexing: bool):
    settings = {"index.mode": mode, "number_of_shards": 1, "number_of_replicas": 0}
    if batch_indexing:
        # Required by the Arrow endpoint
        settings["index.batch_indexing"] = True
    return {
        "settings": settings,
        "mappings": {"properties": {f.name: {"type": es_type(f.type)} for f in schema}},
    }


def recreate_index(es: Es, index: str, schema: pa.Schema, mode: str, batch_indexing: bool):
    status, data = es.request("DELETE", f"/{index}")
    if status not in (200, 404):
        raise RuntimeError(f"DELETE {index} failed with {status}: {data[:1000]!r}")
    es.json("PUT", f"/{index}", index_body(schema, mode, batch_indexing))


# ---------------------------------------------------------------------------------------------------------------------
# Body builders. Each returns a list of (request_path, content_type, body, row_count), where the body is bytes, or a list
# of bytes (HTTP chunks) for a chunked request

def _json_default(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError(f"Cannot serialize {type(o)}")


def build_ndjson(path: Path, index: str, batch_size: int, limit, max_bytes: int):
    """One request per batch of rows, closed early if the next document would make its body exceed `max_bytes`."""
    requests = []
    remaining = limit
    request_path = f"/{index}/_bulk"
    parts, size, count = [], 0, 0

    def flush():
        nonlocal parts, size, count
        if parts:
            requests.append((request_path, "application/x-ndjson", b"".join(parts), count))
        parts, size, count = [], 0, 0

    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
        if remaining is not None:
            if remaining <= 0:
                break
            batch = batch.slice(0, remaining)
            remaining -= batch.num_rows
        for row in batch.to_pylist():
            doc = ('{"create":{}}\n' + json.dumps({k: v for k, v in row.items() if v is not None}, default=_json_default)
                   + "\n").encode("utf-8")
            if parts and size + len(doc) > max_bytes:
                flush()
            parts.append(doc)
            size += len(doc)
            count += 1
        flush()  # a request never spans several batches
    return requests


class _ArrowStream:
    """An Arrow IPC stream being built: its schema, then record batches, then the end-of-stream marker."""

    def __init__(self, schema: pa.Schema):
        self.sink = pa.BufferOutputStream()
        self.writer = pa.ipc.new_stream(self.sink, schema)
        self.ends = []  # offset at which the bytes of each record batch end
        self.rows = 0

    def write(self, batch: pa.RecordBatch):
        self.writer.write_batch(batch)
        self.rows += batch.num_rows
        self.ends.append(self.sink.tell())

    def chunks(self):
        """The body as a list of HTTP chunks, one per record batch: the first one also holds the schema, the last one the
        end-of-stream marker, that is written when the writer is closed."""
        self.writer.close()
        data = self.sink.getvalue()
        self.ends[-1] = len(data)
        chunks, start = [], 0
        for end in self.ends:
            chunks.append(data[start:end].to_pybytes())
            start = end
        return chunks


def build_arrow(path: Path, index: str, batch_size: int, limit, max_bytes: int):
    """Requests whose body is a complete Arrow stream, as a list of chunks (one per record batch). A stream is closed before a
    record batch that wouldn't fit in `max_bytes`."""
    schema = pq.read_schema(path)
    requests = []
    request_path = f"/_arrow/{index}/_bulk"
    stream = None

    def flush():
        nonlocal stream
        if stream is not None:
            requests.append((request_path, ARROW_MEDIA_TYPE, stream.chunks(), stream.rows))
        stream = None

    remaining = limit
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
        if remaining is not None:
            if remaining <= 0:
                break
            batch = batch.slice(0, remaining)
            remaining -= batch.num_rows
        if stream is not None and stream.sink.tell() + pa.ipc.get_record_batch_size(batch) + 8 > max_bytes:
            flush()
        if stream is None:
            stream = _ArrowStream(schema)
        stream.write(batch)
        if stream.sink.tell() + 8 > max_bytes and stream.rows == batch.num_rows:
            raise RuntimeError(f"A record batch of {batch.num_rows} rows is larger than the {max_bytes}-byte request limit")
    flush()

    for _, _, chunks, _ in requests:
        if sum(len(c) for c in chunks) > max_bytes:
            raise RuntimeError(f"An Arrow request of {sum(len(c) for c in chunks)} bytes exceeds the {max_bytes}-byte limit")
    return requests


BUILDERS = {"ndjson": build_ndjson, "arrow": build_arrow}


# ---------------------------------------------------------------------------------------------------------------------
# Sending

def send(es: Es, kind: str, requests):
    for path, content_type, body, _ in requests:
        if kind == "ndjson":
            status, data = es.request("POST", path, body, content_type)
            if status >= 300:
                raise RuntimeError(f"Bulk failed with {status}: {data[:1000]!r}")
            result = json.loads(data)
            if result.get("errors"):
                first = next(i for i in result["items"] if any("error" in v for v in i.values()))
                raise RuntimeError(f"Bulk item failures, first: {json.dumps(first)[:1000]}")
        else:
            status, data = es.request("POST", path, body, content_type, accept=ARROW_MEDIA_TYPE, chunked=True)
            if status >= 300:
                raise RuntimeError(f"Arrow bulk failed with {status}: {data[:1000]!r}")
            # The response is an Arrow stream describing failed items: empty if everything was indexed
            failures = pa.ipc.open_stream(data).read_all()
            if failures.num_rows > 0:
                raise RuntimeError(f"{failures.num_rows} Arrow bulk item failures, first: {failures.slice(0, 1).to_pylist()}")


def payload_bytes(body):
    return sum(len(c) for c in body) if isinstance(body, list) else len(body)


def run_once(es: Es, kind: str, mode: str, batch_size: int, schema: pa.Schema, args):
    index = f"taxi-{kind}-{mode}"
    recreate_index(es, index, schema, mode, batch_indexing=(kind == "arrow"))

    t0 = time.perf_counter()
    requests = BUILDERS[kind](DATA_FILE, index, batch_size, args.limit, args.max_request_bytes)
    t1 = time.perf_counter()
    send(es, kind, requests)
    t2 = time.perf_counter()

    # Outside of the measured time: make documents visible and check that nothing was lost
    es.json("POST", f"/{index}/_refresh")
    count = es.json("GET", f"/{index}/_count")["count"]
    expected = sum(r[3] for r in requests)
    if count != expected:
        raise RuntimeError(f"{index}: expected {expected} documents, found {count}")

    payload_mb = sum(payload_bytes(r[2]) for r in requests) / 1e6
    return {"rows": expected, "prepare": t1 - t0, "ingest": t2 - t1, "total": t2 - t0, "payload_mb": payload_mb}


def fmt_row(cols, widths):
    return "  ".join(str(c).ljust(w) if i < 2 else str(c).rjust(w) for i, (c, w) in enumerate(zip(cols, widths)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=3, help="measured runs per combination (default 3)")
    parser.add_argument("--warmup", type=int, default=0, help="unmeasured runs per combination before measuring (default 0)")
    parser.add_argument("--batch-size", default="10000",
                        help="comma-separated rows per bulk request (ndjson) or per record batch (arrow), "
                             "e.g. 1000,10000,50000 (default 10000)")
    parser.add_argument("--max-request-mb", type=float, default=100,
                        help="maximum size of a request body, in decimal MB (default 100, below the server's 100 MiB "
                             "http.max_content_length)")
    parser.add_argument("--limit", type=int, default=None, help="only ingest the first N rows")
    parser.add_argument("--paths", default="ndjson,arrow", help="comma-separated ingestion paths (default ndjson,arrow)")
    parser.add_argument("--modes", default="standard,columnar", help="comma-separated index modes (default standard,columnar)")
    args = parser.parse_args()
    args.max_request_bytes = int(args.max_request_mb * 1_000_000)

    kinds = args.paths.split(",")
    modes = args.modes.split(",")
    batch_sizes = [int(b) for b in args.batch_size.split(",")]
    for k in kinds:
        if k not in BUILDERS:
            sys.exit(f"Unknown path {k}")

    download(DATA_URL, DATA_FILE)
    schema = pq.read_schema(DATA_FILE)
    es = Es(ES_URL)
    info = es.json("GET", "/")
    print(f"Elasticsearch {info['version']['number']} at {ES_URL}; batch sizes {batch_sizes}, runs {args.runs}\n")

    results = {}
    for mode in modes:
        for batch_size in batch_sizes:
            for kind in kinds:
                label = f"{kind}/{mode}/{batch_size}"
                for i in range(args.warmup):
                    run_once(es, kind, mode, batch_size, schema, args)
                    print(f"[{label}] warmup {i + 1}/{args.warmup} done", flush=True)
                runs = []
                for i in range(args.runs):
                    r = run_once(es, kind, mode, batch_size, schema, args)
                    runs.append(r)
                    print(
                        f"[{label}] run {i + 1}/{args.runs}: {r['rows']} rows, payload {r['payload_mb']:.0f} MB, "
                        f"total {r['total']:.2f}s (read+build {r['prepare']:.2f}s), ingest {r['ingest']:.2f}s",
                        flush=True,
                    )
                results[(kind, mode, batch_size)] = runs

    headers = ["path", "index mode", "batch size", "rows", "payload MB", "total s (med)", "ingest s (med)", "ingest docs/s",
               "ingest s (min)"]
    rows = []
    for (kind, mode, batch_size), runs in results.items():
        ingest = [r["ingest"] for r in runs]
        total = [r["total"] for r in runs]
        med_ingest = statistics.median(ingest)
        rows.append([
            kind, mode, batch_size, runs[0]["rows"], f"{runs[0]['payload_mb']:.0f}",
            f"{statistics.median(total):.2f}", f"{med_ingest:.2f}", f"{runs[0]['rows'] / med_ingest:,.0f}", f"{min(ingest):.2f}",
        ])
    widths = [max(len(str(x)) for x in col) for col in zip(headers, *rows)]
    print("\n" + fmt_row(headers, widths))
    print(fmt_row(["-" * w for w in widths], widths))
    for row in rows:
        print(fmt_row(row, widths))


if __name__ == "__main__":
    main()
