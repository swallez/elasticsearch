# Arrow bulk ingestion

The `arrow-bulk` module indexes [Apache Arrow IPC streams](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
sent to `POST /_arrow/{index}/_bulk`. The rows of the record batches are converted to ESCF batches and indexed
through the columnar batch indexing path. Notes on how the bulk requests of a stream are scheduled are below, to give the context of
the possible enhancement that follows.

## Benchmark results

Ingestion of the NYC yellow taxi trips of January 2026 (3,724,889 rows, 20 columns, no `_id` column, a 64 MB Parquet file) through
the classic `_bulk` API with ndjson and through `POST /_arrow/{index}/_bulk`, on a standard index and on a columnar index
(`index.mode: columnar`). The Arrow indices have `index.batch_indexing: true`, which this endpoint requires.

Two durations are measured:
* *Total* is the end-to-end time: reading the Parquet file, building the request bodies and sending them. This end-to-end measurement takes into account ndjson creation in Python, which has a significant cost.
* *Ingest* is only the sending, once the bodies are built in memory. Each figure is the median of 3 runs, after 3 unmeasured warmup
runs, and the index is deleted and recreated before every run. The runs of a combination are within 2% of each other.

| Path   | Index    | Payload | Total  | Ingest | Ingest rows/s |
|--------|----------|--------:|-------:|-------:|--------------:|
| ndjson | standard | 1721 MB | 62.49s | 38.11s |        97,733 |
| arrow  | standard |  555 MB | 21.86s | 21.68s |       171,792 |
| ndjson | columnar | 1721 MB | 53.92s | 29.49s |       126,319 |
| arrow  | columnar |  555 MB | 11.77s | 11.61s |       320,925 |

- Ingesting Arrow into a columnar index is 2.5 times faster than ndjson into a columnar index, and 3.3 times faster than ndjson into a
  standard index. End-to-end it is 5.3 times faster than ndjson into a standard index, because about 24s of the ndjson total is the
  JSON encoding done by the Python client.
- The columnar index mode is faster than the standard one for both paths: 22% for ndjson and 46% for Arrow.
- On a standard index, Arrow batches can't be indexed with the columnar mapper, so rows are materialized and indexed one document at a
  time. Arrow is still faster there, thanks to the smaller payload and to not having to parse JSON.

**How the requests are sent.** Both paths send 10,000 rows at a time. For ndjson that is one request per 10,000 rows. For Arrow the
data is sent as streams, each one a complete Arrow IPC stream sent with chunked transfer encoding, with one HTTP chunk per record batch
of 10,000 rows. No request body exceeds 100 MB, as the HTTP layer applies `http.max_content_length` (100 MiB by default) to the total
size of streamed requests too, and answers `413` when it is exceeded. The 555 MB of the file is therefore sent as 6 streams (five of
about 99 MB and one of 55 MB). Sending one request per record batch instead was slower (Arrow ingest of 30.4s into a standard index
and 21.4s into a columnar index, with 373 requests). This is consistent with the overlapping of the bulk requests of a stream described
below, but the cause was not isolated.

**Caveats.**

- This is a single node, with a single shard and no replicas, started with `./gradlew run`: a 512 MB heap, 2 processors,
  assertions disabled (`-da -dsa`), and the client on the same machine. With assertions enabled, the translog keeps every operation and
  a stack trace in memory and the node runs out of heap, so assertions must be disabled. The figures are good to compare the four
  paths, but they are not absolute throughputs. Run `bin/elasticsearch` directly for these.
- The file has no `_id` column, so the bulk requests of a stream overlap, see below. With an `_id` column they run one at a time.
- The "read and build" time of the Arrow path is about 0.15s, which is too low to include a full decode of the Parquet file. Compare
  the *Ingest* durations of the two paths, and consider the Arrow *Total* as the sending time plus a small constant.
- Only one data set was used, with 20 columns of numbers, timestamps and short strings.

**Reproducing.** The script is `dev-tools/arrow-parquet-test/benchmark.py`. It downloads the file if needed, assumes an Elasticsearch
without authentication at `http://localhost:9200`, and needs [`uv`](https://docs.astral.sh/uv/) to install its dependency:

```
./dev-tools/arrow-parquet-test/benchmark.py --warmup 3
```

Use `--limit N` to ingest only the first rows, `--batch-size 1000,10000,50000` to compare batch sizes, and `--paths` and `--modes` to
select combinations.

## How the bulk requests of a stream are scheduled

- **Coalescing**: rows of small record batches are accumulated per target index, and sent as a bulk request when there are enough
  of them, or when the stream ends (`bulk_rows`, `bulk_bytes`, by default scaled with the shards of the target, see `ArrowBulkParser`
  and `TargetShards`).
- **Overlapping**: several bulk requests of a stream run at the same time (up to `ArrowBulkAction.MAX_IN_FLIGHT_BULKS`), and the
  next data is requested as soon as no bulk request waits to be sent, so that parsing overlaps with indexing (see `BulkPipeline`).
- **Order**: this overlapping is only done when the stream has **no `_id` column**. Documents with an id can overwrite each other
  (`op_type=index`) or conflict (`create`), and the outcome depends on the order in which the bulk requests run, so with an `_id`
  column they run one at a time, in the order of the stream. Without ids, the node generates them and the rows are independent.
- **Threads**: the data of a request is parsed and converted to bulk requests by the thread of the HTTP channel that received it
  (a Netty I/O thread), which also serves other connections. Converting takes about 2 ms for 10k rows, see the possible improvement
  below.

## Possible future enhancement: overlap the bulk requests of streams that have an `_id` column

**Status: not planned.** Bulk ingestion through this endpoint is expected to be mostly timestamped data (logs, metrics, events)
without `_id`, which already gets the overlapping. This is recorded in case workloads with natural keys become important.

**The cost today.** In a benchmark on a laptop (single node, 12 columns, record batches of 10000 rows), one stream without ids
was indexed at about 690k rows/s, and the same stream with an `_id` column at about 245k rows/s.

**The idea.** Ordering only matters between bulk requests that have a document id in common, so most streams with unique ids could
overlap like the ones without ids, with exactly the same results. At runtime:

- When a bulk request is created, compute a compact set of its ids (for example the 64-bit hashes of the id strings). Rows without
  an id (null) never conflict.
- Keep sending bulk requests in the order of the stream, but only send the next one if none of its ids is in a bulk request that is
  still running. Otherwise it waits for the bulk request that it shares an id with to complete. Bulk requests that share an id keep
  their order, and the others overlap.
- A hash collision only causes a false conflict, which means less overlap and never a wrong result.
- Memory is bounded by the bulk requests that run (at most `MAX_IN_FLIGHT_BULKS`, with their capped sizes), and not by the size of
  the stream. A stream of billions of ids doesn't need a global set of ids.
- Within a single bulk request, documents with the same id are already executed in order by the engine.

**What to do and test if it is implemented.**

- Give `BulkPipeline` a predicate that says whether the next bulk request can be sent now. Unit-test head-of-line blocking and the
  order of bulk requests that conflict.
- Compute the ids when a bulk request is flushed, outside the lock that guards the state of the request.
- Integration tests: the same id in several bulk requests runs them one at a time, as today; unique ids overlap up to the limit.
- Benchmark streams with ids before and after, with unique ids, and with some repeated ids.

**Alternatives that were considered.**

- A cheap proof when ids are strictly increasing: nothing repeats, one comparison per row. It only helps sorted keys.
- A request parameter with which the client promises that ids are unique. It costs nothing, but if the promise is wrong the winner of
  duplicates silently depends on timing, so it shouldn't be the only mechanism.
- A Bloom filter over the whole stream: unbounded memory, and a false positive still needs the sequential fallback.

**What it can't do.** It is about the order of the rows of one stream. It can't make the result deterministic relative to other
clients that write the same ids at the same time, which is not something that running bulk requests in order could do either.

## Possible future improvement: parse the data in another thread than the one of the HTTP channel

**Status: tried, and not kept.** The thread of the HTTP channel also serves other connections, and parsing and converting the data
blocks it: about 2 ms for 10k rows in a profile, more for bigger record batches. (The JSON bulk API also parses on this thread, and
converting is lighter than parsing JSON.) Parsing in a thread of the `write_coordination` pool, the one that coordinates bulk requests,
was implemented and measured, and then reverted.

**What was measured** on a laptop (single node), by comparing parsing in the `write_coordination` pool with parsing on the channel
thread:

- Throughput of streams was about 10% lower with forked parsing, probably because of the hand-off between threads for every HTTP chunk
  (this wasn't verified).
- The p99 latency of other connections served by the same threads was about 3 times lower.
- The effect on the worst latencies was inconclusive.

**Why it was reverted.** It costs about 10% of the throughput of streams without ids for a benefit that is only seen in the latency
of other connections that share the thread, and it adds code.

**If it is done again.**

- Make it a dynamic cluster setting, as `indices.batch_indexing` is, so that it can be turned off without restarting a node. A request
  would decide at its start, so that all its data is handled in the same way.
- Run a task per chunk with `AbstractRunnable` on the `write_coordination` pool. If the task is rejected, release the chunk and fail the
  request, with the usual 429. The next chunk is only requested after the previous one was handled, which is what makes tasks of a
  request run one at a time and in order.
- When the client disconnects, close the request in a task that can't be rejected (`isForceExecution`), so that what it holds is
  released and the thread of the channel doesn't wait for a parsing that runs. The state of the request is already guarded by a lock.
- Tests that were useful: check the thread that sent the first bulk request (an action filter that records it) with the setting on and
  off, switch the setting while running, and run the whole integration suite with the setting off, to keep that path tested.

**A further improvement that was considered.** Request the next chunk when the processing of the current one starts, so that the
network read overlaps with the parsing and recovers a part of the cost above. It is more involved than it looks:

- The next chunk can arrive while the current one is still being parsed, so chunks would have to be processed one at a time and in
  order by an explicit per-request serial executor (for example a `ThrottledTaskRunner` that runs one task at a time), including when
  a task is rejected.
- `BulkPipeline` must tell "a chunk arrived" (another one can be requested) from "a chunk was parsed" (it was the last one). Recording
  that the last chunk was received before parsing it would make the request finish before its data was converted.
- The decision to read ahead must not block the thread of the channel, which can't take the lock that is held while parsing.
- The memory that is held grows by one chunk.
