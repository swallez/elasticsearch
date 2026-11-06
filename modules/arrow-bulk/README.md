# Arrow bulk ingestion

The `arrow-bulk` module indexes [Apache Arrow IPC streams](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
sent to `POST /_arrow/{index}/_bulk`. The rows of the record batches are converted to ESCF batches and indexed
through the columnar batch indexing path. Notes on how the bulk requests of a stream are scheduled are below, to give the context of
the possible enhancement that follows.

## How the bulk requests of a stream are scheduled

- **Coalescing**: rows of small record batches are accumulated per target index, and sent as a bulk request when there are enough
  of them, or when the stream ends (`bulk_rows`, `bulk_bytes`, by default scaled with the shards of the target, see `ArrowBulkParser`
  and `TargetShards`).
- **Overlapping**: several bulk requests of a stream run at the same time (up to `ArrowBulkAction.MAX_IN_FLIGHT_BULKS`), and the
  next data is requested as soon as no bulk request waits to be sent, so that parsing overlaps with indexing (see `BulkPipeline`).
- **Order**: this overlapping is only done when the stream has **no `_id` column**. Documents with an id can overwrite each other
  (`op_type=index`) or conflict (`create`), and the outcome depends on the order in which the bulk requests run, so with an `_id`
  column they run one at a time, in the order of the stream. Without ids, the node generates them and the rows are independent.

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
