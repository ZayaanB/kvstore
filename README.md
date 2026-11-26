# kvstore

An embedded C++20 key-value store with durable writes and disk-backed point reads.
Each of 16 shards has a WAL, a bounded ordered memtable, and immutable sorted
tables. The engine uses POSIX I/O and has no external library dependencies.

See [Architecture](docs/architecture.md) for the engine, batch runner, lakehouse
demo, monitoring stack, and current integration boundaries.

```cpp
kvstore::KVStore store("data/mydb.db");  // Opens or creates a database directory.
store.Set("hello", "world");
auto value = store.Get("hello");
store.Delete("hello");
store.Compact();                       // Flush writes and merge sorted files.
```

## Storage and recovery

Version 1 stores the shard count, hash identifier, and memory budget in
`METADATA`. A shard is one of 16 groups of keys. New databases use rendezvous
consistent hashing. The engine gives each shard a repeatable score for a key
and picks the highest score. Existing databases keep their recorded routing.
The shard's checksummed `MANIFEST` lists the newest
committed generation and last flushed sequence. After compaction, it can also
select a merged base table. If the newest generation is `n`, the active WAL is
`(n+1).wal`. The current engine reads databases created before merged tables
were introduced. Older binaries cannot open shards whose manifests select a
merged base or databases that use rendezvous hashing.

WAL and table records use little-endian sequence (8 bytes), operation (1 byte),
key and value lengths (4 bytes each), and absolute expiry in Unix seconds
(8 bytes, zero for no expiry), followed by key bytes, value bytes, and an IEEE
CRC32 over the preceding bytes. Operation IDs are 1 for Set, 2 for Delete, and
3 for Set with TTL. Keys and values are binary-safe and limited to 1 MiB and
256 MiB respectively. The newest record decides visibility, so deletes and
expired values mask older values. Nonpositive TTLs expire immediately.

Each write appends and syncs its shard WAL before applying the mutation to
memory. A flush syncs the sorted table and next WAL, syncs their directory
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
for returned results. `Compact()` flushes pending writes, merges multiple sorted
files in each shard, and removes older versions. The merge publishes its
replacement before removing the old files.

Use `WalPaths()` in place of `WalPath()`. It returns the current WALs in shard
order. A flush can change those paths. Use `WalBytes()` for aggregate active WAL size and `DatabasePath()` for
the database directory. A sibling `<db_path>.lock` prevents concurrent opens.
Join callers before destroying an instance and reopen the database in any
child process that uses it after `fork()`.

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
after the checkpoint are lost when you restore it.

For manual failover after a host loss, take the original service out of service
so it cannot also accept writes. Copy the verified checkpoint to the replacement
host, use `--checkpoint` to make a working copy if you want to keep the backup
unchanged, and point the application at the replacement. Automatic failover
and live replication between hosts require additional coordination.

## Offline migration

Close the legacy process before running the migration. Supply the old database
prefix, which has a sibling `<legacy_db_path>.wal`, and an unused destination.

```bash
./build/kvstore_cli --migrate data/legacy.db data/new.db
./build/kvstore_cli data/new.db
```

For an existing disk-backed database that uses the earlier modulo hashing,
use `--rehash` instead. It copies all live rows and their absolute expiration
times into a new database with rendezvous hashing. The source stays available
for rollback. Stop writes to the source before switching the application.

```bash
./build/kvstore_cli --rehash data/old-disk-store data/rendezvous-store
```

The command locks the source, streams its CRC-protected WAL in order, and
reopens the staged database to validate it before publishing the destination.
It ignores an incomplete source tail and rejects complete records with an
invalid CRC. The source WAL remains available to the legacy engine. If a
process is killed before publication, it can leave a hidden
`.kvstore-stage-*` directory beside the destination. It can be removed after
confirming the migration process has stopped. If switching back after accepting
writes on the new database, reconcile those writes before using the old one.

## Build and run

Needs a C++20 compiler (GCC 11+ / Clang 14+) and CMake 3.20+ on Linux or macOS.

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j
ctest --output-on-failure
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
./kvstore_cli data/mydb.db
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

`kvstore_bench` runs closed-loop point reads, durable writes, 80/20 mixed traffic,
compaction under mixed traffic, and reopening a populated store. Data workloads
run against uniform and hot-key distributions. Each trial creates and removes an
isolated database and verifies the final data, including WAL recovery. Writes
use the engine's normal `fdatasync` path.

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
settings for both modes and pass `--compare-modes` to the report script. Store
each report with its two raw JSON files and commands. Keep designated baselines
until replaced and retain other local runs for 30 days.

### Historical sharding benchmark

Results for the earlier in-memory engine on a 13th-gen Intel i9-13900H (20 hardware threads), Kingston NVMe SSD,
ext4, Linux, GCC 14 `-O3` (Release). Reads are 1M `Get`s per thread over a
shared 1,000-key space (run-to-run variance <2%). The baseline column is the
same binary rebuilt with `kNumShards = 1` in `kvstore.hpp`, which collapses the
sharded index into a single global reader-writer lock.

| threads | reads, 1 shard (global lock) | reads, 16 shards |
|--------:|-----------------------------:|-----------------:|
|       1 |                 9.2M ops/sec |     8.1M ops/sec |
|       2 |                 8.1M ops/sec |    13.7M ops/sec |
|       4 |                 8.0M ops/sec |    22.7M ops/sec |
|       8 |                 7.5M ops/sec |    30.9M ops/sec |
|      16 |                 7.6M ops/sec |    40.3M ops/sec |

Three things worth noticing:

- **Sharding is worth ~5x on contended reads** (7.6M -> 40.3M ops/sec at 16
  threads). The single-thread results show some overhead (9.2M -> 8.1M ops/sec).
- **A single global `shared_mutex` scales negatively**: 16 threads read
  *slower* than 1 (9.2M -> 7.6M ops/sec) because every reader bounces the same
  lock word between cores. Sharding exists to fix exactly this.
- **Writes don't scale, by design** (~235 ops/sec at any thread count, either
  config): every acknowledged `Set` pays an `fdatasync` under `wal_mutex_`, so
  write throughput is the disk's flush rate, not a CPU or lock number — see
  the trade-offs section of `docs/kvstore_report_v2.html`. Group commit is the
  standard fix.

Mixed 80/20 throughput (~1,200 ops/sec, flat across threads and shard counts)
is just the write ceiling divided by the write fraction: 235 / 0.2 ≈ 1,175 —
the 20% of ops that fsync dominate everything else. Compacting 1,000 live keys
takes ~11 ms.

## Local monitoring

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
syncs, explicit compaction, automatic shard flushes, sorted-table writes,
point lookups that reach sorted tables, and CLI resource use.

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
