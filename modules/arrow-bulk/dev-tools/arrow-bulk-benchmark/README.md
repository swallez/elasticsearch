# Arrow bulk benchmark scripts

Scratch tools used to measure `POST /_arrow/{index}/_bulk` (coalescing, pipelining, shard-aware sizing). Not part of the build.

- `Gen.java`, `GenBig.java`: write the same rows as an Arrow IPC stream and as NDJSON. `gen.sh` runs `Gen` and expects a `cp.txt` file
  with the Arrow classpath (machine-specific, not kept).
- `bench.py`: creates indices and streams Arrow data to a node, reporting rows/s. `ab_fork.py`, `ab_shards.py`, `exp_big.py` and
  `exp_shards.py` are one-off A/B experiments built on the same pattern.
- `env.sh`: curl helpers (`create`, `arrow`, `json`, `count`) for manual runs.
- `analyze.sh`, `analyze2.sh`: group async-profiler collapsed stacks by stage (conversion, parsing, routing, shard work).
- `FsyncBench.java`: measures the cost of translog fsyncs.

Run against a node **without assertions** and with the real processor count (not `./gradlew run`), or the numbers are misleading.
Credentials in the scripts are the dev-cluster defaults. Several scripts expect the generated data files in the working directory.
