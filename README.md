# kvstore

An embedded C++20 key-value store with durable writes and disk-backed point reads.
Each of 16 shards has a WAL, a bounded ordered memtable, and immutable sorted
tables. The engine uses POSIX I/O and has no external library dependencies.

See [Architecture](docs/architecture.md) for the storage engine, local MapReduce
runner, Spark-to-Iceberg pipeline, and benchmark results.

```cpp
kvstore::KVStore store("data/mydb.db");  // Opens or creates a database directory.
store.Set("hello", "world");
auto value = store.Get("hello");
store.Delete("hello");
store.Compact();                       // Flush writes and merge sorted files.
```

## Storage and recovery

The checksummed `METADATA` records the format version, shard count, routing
algorithm, and memory budget. New databases group keys into 16 shards using
rendezvous consistent hashing. Each key goes to the shard with the highest
score for that key. The shard's checksummed `MANIFEST` records the committed
table generation, last flushed sequence, and any merged base table. For
generation `n`, the active write-ahead log (WAL) is `(n+1).wal`.

WAL and table records use little-endian sequence (8 bytes), operation (1 byte),
key and value lengths (4 bytes each), and absolute expiry in Unix seconds
(8 bytes, zero for no expiry), followed by key bytes, value bytes, and an IEEE
CRC32 over the preceding bytes. Operation IDs are 1 for Set, 2 for Delete, and
3 for Set with TTL. Keys and values are binary-safe and limited to 1 MiB and
256 MiB respectively. The newest record decides visibility, so deletes and
expired values mask older values. Nonpositive TTLs expire immediately.

Each write appends and syncs its shard WAL before applying the mutation to
memory. Use `ShardForKey()` to group updates and `SetBatch()` to submit a
batch for one shard with a single append sync. A multi-entry batch must fit
that shard's write buffer. A crash before the call returns can leave some
or all of the records recoverable.

A flush syncs the sorted table and next WAL, syncs their directory
entries, and publishes the manifest with a rename and directory sync. Only
then can it unlink the covered WAL. Startup validates committed tables and
ignores unpublished tables and retired WALs. It repairs an incomplete active
WAL tail or a final record with a bad CRC. Invalid record headers, earlier CRC
mismatches, missing committed files, and read errors fail startup.

A failed WAL append is rolled back and synced when possible. If rollback or
manifest publication is uncertain, close and reopen the store before making
further requests. An unacknowledged mutation can survive such a failure.
Durability depends on the filesystem and storage honoring sync operations.

The default 16 MiB memtable budget is divided among the shards. To set it at
creation, pass a byte count as the third constructor argument. The budget also
caps ordinary WAL replay per shard. A single record above its shard allowance
uses temporary memory and flushes synchronously. Point reads check the memtable
and then sorted tables, probing on-disk key ranges and a sparse index. `Size`,
`Items`, and `Range` read each shard in turn. The vector APIs allocate memory
for returned results. Once a shard has eight sorted files, its next write
automatically attempts to merge them. `Compact()` flushes pending writes and
merges files on demand. The merge publishes its replacement before removing the
old files.

`WalPaths()` returns the active paths in shard order, `WalBytes()` reports their
aggregate size, and `DatabasePath()` returns the database directory. A sibling
`<db_path>.lock` prevents concurrent opens. Join callers before destroying
an instance and reopen the database in any child process that uses it after
`fork()`.

### Shard recovery and backups

The default mode requires every shard to recover. Open an existing store with
`--degraded` to keep point reads and writes working on healthy shards if one
fails. Use the `shards` command to list quarantined shards, meaning shards taken
out of service. A read on a quarantined shard reports an error. Whole-store
scans and snapshots require all shards.

```bash
./build/kvstore_cli --checkpoint data/store backups/store-copy
./build/kvstore_cli data/store --degraded
./build/kvstore_cli --restore-shard data/store backups/store-copy 3
```

Close the other process using the database before running the checkpoint or
restore command. A checkpoint is a consistent, verified copy of all shards.
Store it on separate storage to protect against device failure. Restoring shard
3 replaces that shard with the copy from the checkpoint and leaves other shards
alone. The previous files stay on disk for diagnosis. Writes to shard 3 made
after the checkpoint are lost when you restore it. A restored database
requires a binary that supports its shard pointer and recorded format.

To recover after host loss, ensure the original process can no longer accept
writes. Copy the checkpoint to the replacement host and point the application
at a working copy. Recovery reaches the checkpoint's capture point.

## Offline migration

Close the source database before migrating and supply an unused destination.
`--migrate` replays a legacy `<legacy_db_path>.wal` into a new disk-backed
database. It preserves mutation order and expiration times, skips an
incomplete trailing record, and rejects a complete record with a bad checksum.

```bash
./build/kvstore_cli --migrate data/legacy.db data/new.db
./build/kvstore_cli data/new.db
```

`--rehash` copies live rows and their expiration times from a disk-backed
database using modulo routing into a new database using rendezvous hashing.

```bash
./build/kvstore_cli --rehash data/old-disk-store data/rendezvous-store
```

Both commands validate the destination before making it available and preserve
the source. Stop writes to the source before switching the application.
Reconcile writes made to the destination before switching back. An interrupted
migration may leave a `.kvstore-stage-*` directory beside the destination.

## Build and run

Needs a C++20 compiler (GCC 11+ / Clang 14+) and CMake 3.20+ on Linux or macOS.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

`kvstore_test` runs the stress and crash-recovery suite: concurrent
writes/reads, a compaction while readers are active, a simulated crash with a
torn WAL record, and a restart that re-validates everything. It ends with
`=== ALL TESTS PASSED ===`. Its assertions remain enabled in Release builds.
On Linux, CTest also runs `kvstore_failure_test`: deterministic POSIX failure
injection, every truncation boundary in a TTL record, concurrent writes with
flushes, allocation failures, and process exits around manifest publication,
WAL retirement, and offline migration.

## CLI

`kvstore_cli` is an interactive shell (optional db path argument):

```bash
./build/kvstore_cli data/mydb.db
```

```
> set hello world
OK
> get hello
world
> setttl session 60 abc   # value "abc", expires in 60s
OK
> keys
hello
session
> scan a z                # keys in [a, z), end exclusive
hello = world
> del hello
OK
> compact
OK
> exit
```

Other commands: `size`, `help`.

## Word count and lakehouse export

Both integrations use a snapshot, a consistent view of the database at one
time. The cursor streams rows and pauses writes until it finishes. Close the
CLI before opening the database from a separate process.

```bash
./build/kvstore_word_count --database data/store results.tsv
```

See [Spark-to-Iceberg export](lakehouse/README.md#export-an-existing-database)
for exporting live rows and querying versions of the resulting table.

## Benchmark

`kvstore_bench` measures point reads, durable writes, mixed traffic, compaction
under mixed traffic, and reopening a populated store. Hot-key runs favor four
keys by default. Use `--hot-keys N` to change that count. Each trial creates an
isolated database and verifies the final data after reopening. Writes use the
engine's normal `fdatasync` path.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
mkdir -p build/benchmarks/baseline build/benchmarks/candidate
./build/kvstore_bench --smoke --mode latency --json build/benchmarks/baseline/results.json
./build/kvstore_bench --smoke --mode latency --json build/benchmarks/candidate/results.json
python3 benchmarks/compare.py build/benchmarks/baseline build/benchmarks/candidate \
  --output build/benchmarks/comparison.html
```

Use the same parameters when comparing revisions. Drop `--smoke` for the full
suite, or use `--help` for workload, thread count, seed, trial count, and sample
size controls. Select `--mode throughput` to omit per-operation latency sampling
(the default). To measure the cost of sampling, use the same binary and workload
settings for both modes and pass `--compare-modes` to the report script.

The mixed workload defaults to 80% reads. Use `--read-percent 95` for a 95/5
mix. Use `--batch-size N` with `--workload mixed` to give each worker one shard
for writes and collect up to N updates per call. Set `--batch-size 1` to
benchmark individual writes with the same shard assignment. Add
`--distribution hot` to direct the workers' writes to the shards that hold the
hot read keys.
See the [current 95/5 results](docs/architecture.md#benchmarks-and-monitoring)
and [reproduction command](docs/architecture.md#reproduce-the-read-heavy-benchmark).

## Local monitoring

Run `./monitor` from the repository root. It starts Prometheus and Grafana,
then opens the [local launcher](http://127.0.0.1:3001/). Click **Start KVStore**
to start the app with metrics enabled and run one minute of sample traffic
against a separate demo database. The launcher automatically connects to a
KVStore instance already serving metrics on port 9108. Click **Open Grafana**
to see the live metrics after a few scrapes. The default Grafana login is
`admin` / `admin`.

The launcher uses Python 3, CMake, and local Prometheus and Grafana binaries.
If the binaries are unpacked outside the system install locations, supply
their distribution directories once. The launcher saves those paths for
later runs.

```bash
PROMETHEUS_DIR=/path/to/prometheus-distribution GRAFANA_DIR=/path/to/grafana-distribution ./monitor
```

Use `./monitor --no-open` to print the URL without opening a browser. Ctrl-C
stops the processes started by the launcher. The demo database, logs, and
monitoring data are under `build/monitoring/local/`.

### Manual setup and Docker Compose

The CLI can attach an in-process recorder and expose Prometheus metrics on
loopback. The embedded engine opens no listener. Keys, values and database
paths stay out of the metrics.

```bash
./build/kvstore_cli /tmp/kvstore-demo/store --metrics-port 9108
# From another terminal
curl -fsS http://127.0.0.1:9108/metrics
```

Start the complete local stack with Docker Compose. The Compose CLI uses its
own database volume and serves metrics only on the Compose network.

```bash
docker compose -f monitoring/compose.yml up --build -d
docker compose -f monitoring/compose.yml attach kvstore
```

Run `set hello world`, `get hello`, `get absent`, and `compact` repeatedly
across several scrapes. Detach from the CLI with Ctrl-P, Ctrl-Q.
Open [KVStore live](http://127.0.0.1:3000/d/kvstore-live/kvstore-live) with
the initial credentials `admin` / `admin`. Use the time range, rate window
and operation filter to drill into counts and mean, p50, p95 and p99 latency.
The summary cards show totals across data operations. Expected lookup misses
count toward throughput and have their own outcome. The dashboard shows WAL
syncs, manual and automatic compaction, automatic shard flushes, sorted-table
writes, point lookups that reach sorted tables, and CLI resource use.

The database footprint is a best-effort gauge. The exporter omits it when
files change during the scan or the scan exceeds its entry or time budget.
Sorted-table write metrics include merge output. The exporter also records
which shards are quarantined.

The [Prometheus Alerts page](http://127.0.0.1:9090/alerts) lists the same local
rules shown on the dashboard. The rules cover sustained data errors,
missing telemetry, low disk space, and quarantined shards. Both services keep their data in named
volumes, and Prometheus retains 30 days of samples.

```bash
docker compose -f monitoring/compose.yml down
```

For embedded use, pass a shared `kvstore::Metrics` recorder to the
`KVStore` constructor. For benchmark comparisons, set
`--engine-metrics off` or `--engine-metrics on` and use
`benchmarks/compare.py --compare-engine-metrics` to compare the two runs.

To exercise the dashboard and alerts with local Prometheus and Grafana
binaries, set `PROMETHEUS_DIR` and `GRAFANA_DIR` to their distribution
directories. Run these checks from the repository root.

```bash
"$PROMETHEUS_DIR/promtool" test rules monitoring/alerts.test.yml
python3 monitoring/smoke.py --cli ./build/kvstore_cli \
  --prometheus "$PROMETHEUS_DIR/prometheus" --grafana "$GRAFANA_DIR" \
  --output /tmp/kvstore-monitoring-results
```

The smoke test drives durable writes and reads, interrupts scraping while
checking reads and writes through the same CLI, and exercises the firing
and resolution of both telemetry and error alerts. It stops its processes
and removes its temporary database. The requested output directory holds
the Grafana query results and process logs.

## Sanitizers

```bash
# data races
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread kvstore.cpp metrics.cpp main.cpp -o kvstore_tsan && ./kvstore_tsan

# memory / undefined behavior
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -pthread kvstore.cpp metrics.cpp main.cpp -o kvstore_asan && ./kvstore_asan
```
