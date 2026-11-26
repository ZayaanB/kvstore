# kvstore

An embedded C++20 key-value store with durable writes and disk-backed point reads.
Each of 16 shards has a WAL, a bounded ordered memtable, and immutable sorted
tables. The engine uses POSIX I/O and has no external library dependencies.

```cpp
kvstore::KVStore store("data/mydb.db");  // Opens or creates a database directory.
store.Set("hello", "world");
auto value = store.Get("hello");
store.Delete("hello");
store.Compact();                       // Flush all pending memtables.
```

## Storage and recovery

Version 1 stores the shard count, hash identifier, and memory budget in
`METADATA`. The shard is 64-bit FNV-1a over unsigned key bytes, reduced modulo
16. Each `shard-N` directory has a checksummed `MANIFEST` containing the number
of committed tables and the last flushed sequence. Table generations start at
1. If a manifest lists `n` tables, recovery uses tables `1.sst` through `n.sst`
and replays `(n+1).wal`.

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
for returned results. `Compact()` flushes pending writes. Sorted tables and
their obsolete versions accumulate until table merging is implemented.

Use `WalPaths()` in place of `WalPath()`. It returns the current WALs in shard
order. A flush can change those paths. Use `WalBytes()` for aggregate active WAL size and `DatabasePath()` for
the database directory. A sibling `<db_path>.lock` prevents concurrent opens.
Join callers before destroying an instance and reopen the database in any
child process that uses it after `fork()`.

## Offline migration

Close the legacy process before running the migration. Supply the old database
prefix, which has a sibling `<legacy_db_path>.wal`, and an unused destination.

```bash
./build/kvstore_cli --migrate data/legacy.db data/new.db
./build/kvstore_cli data/new.db
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
loopback. The embedded engine opens no listener.

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
Grafana is at `http://127.0.0.1:3000` with initial credentials
`admin` / `admin`. Open **KVStore live** and select a time range, rate window,
and operation. Prometheus is at `http://127.0.0.1:9090`; its Alerts page lists
the same local rules shown on the dashboard. Both services keep their data
in named volumes, and Prometheus retains 30 days of samples.

```bash
docker compose -f monitoring/compose.yml down
```

For embedded use, pass a shared `kvstore::Metrics` recorder to the
`KVStore` constructor. For benchmark comparisons, set
`--engine-metrics off` or `--engine-metrics on` and use
`benchmarks/compare.py --compare-engine-metrics` to compare the two runs.

## Sanitizers

```bash
# data races
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread kvstore.cpp metrics.cpp main.cpp -o kvstore_tsan && ./kvstore_tsan

# memory / undefined behavior
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -pthread kvstore.cpp metrics.cpp main.cpp -o kvstore_asan && ./kvstore_asan
```
