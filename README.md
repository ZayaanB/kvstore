# kvstore

An embedded key-value store in C++20. Writes go to a binary write-ahead log and
are `fsync`ed to disk before memory changes, so acknowledged writes survive both
process crashes and power loss. Every record carries a CRC32 so recovery rejects
torn or corrupted data instead of trusting it. The index is split into 16 shards
for concurrency, and a compaction step reclaims dead log space while allowing
reads during the rewrite. No dependencies beyond the standard library (POSIX
for `fsync`).

## Design

```
  Set/Delete                         Get/Range/Size
      |                                    |
      v                                    v
  [wal_mutex] append + fsync -> db.wal   shard shared_lock
      |                                    |
      v                                    v
  shard unique_lock --> map           read from map

  index: 16 shards, each { shared_mutex, unordered_map<string,Entry> }
         shard = hash(key) % 16
  start: replay db.wal record by record
  compact: live keys -> db.wal.tmp --(atomic rename)--> db.wal
```

- **Sharded index** — each shard has its own `shared_mutex`, so reads run in
  parallel and each write updates one shard. A global WAL mutex serializes writes.
- **Write-ahead log** — each `Set`/`Delete` is a binary record (type, key len,
  value len, payload, trailing CRC32) written to `db.wal` and `fdatasync`ed
  before the in-memory map changes. Failed writes trigger a checked, synced
  rollback; an uncertain rollback disables the store until recovery.
- **Recovery** — on startup the log is replayed; each record's CRC32 is checked,
  so a torn or corrupted record stops replay cleanly instead of loading garbage.
- **Compaction** — `Compact()` holds the WAL mutex while it writes live keys to a
  temp file, `fdatasync`s it, and atomically renames it into place (syncing the
  directory). All writes wait for compaction. Readers share shard locks during
  the rewrite; the final expired-entry cleanup briefly takes exclusive shard locks.

```cpp
kvstore::KVStore store("data/mydb.db"); // opens (or creates) and recovers

store.Set("hello", "world");            // -> bool
store.Get("hello");                     // -> std::optional<std::string>
store.Delete("hello");                  // -> bool
store.Compact();                        // -> bool
```

## On-disk format

The WAL is a flat sequence of self-describing records. Each record is:

```
+--------+---------+-----------+-----+-------+--------------+--------+
| type   | key_len | value_len | key | value | expires_at   | crc32  |
| 1 byte | 4 bytes | 4 bytes   | ... | ...   | 8 bytes*     | 4 bytes|
+--------+---------+-----------+-----+-------+--------------+--------+
        \___________________ covered by crc32 ______________/
```

- **`type`** — `1` set, `2` delete, `3` set-with-ttl. A delete has no `value`.
- **`expires_at`** — Unix seconds; present only for `type == 3` (`*`).
- **`crc32`** — IEEE CRC32 over every preceding byte of the record.
- Integers use fixed little-endian encoding, independent of host byte order.

`Get`/`Set`/`Delete` are `O(1)` on one shard; `Range`/`Items`/`Size` scan all
shards and cost `O(n)` (plus an `O(n log n)` sort for ordered results).

## Durability

`SetInternal`/`Delete` hold `wal_mutex_` across the whole append-then-apply so
successful writes follow the same WAL and in-memory order. The write path is:

1. serialize the record (with its CRC) and `write()` it to the WAL;
2. `fdatasync()` the WAL so the bytes are on physical media;
3. only then mutate the shard's map and return success.

If `write` or `fdatasync` fails, the store attempts to truncate back to the
previous record boundary **and sync that rollback**. The call returns `false`.
If either rollback step fails, the store enters a failed state: writes and
compaction return `false`, and reads throw `std::runtime_error`. Close and reopen
the store to recover before continuing. A failed operation with an uncertain
rollback may appear after recovery; it was never acknowledged as successful.

Directory sync failures are checked for WAL creation and compaction replacement.
Startup also persists the parent entries of database directories. A directory
sync failure after compaction's rename leaves the store in the same failed state.
Durability assumes a filesystem and storage stack that honor successful syncs.

Recovery distinguishes read/open errors from EOF and fails startup on I/O errors
or unsuccessful tail repair. Incomplete final records and a bad CRC on the final
record are truncated and synced. A CRC mismatch before later records, an invalid
record type, or an invalid length header fails startup and preserves the WAL for
inspection. Without independently protected record framing, damaged lengths can
misidentify boundaries and make some middle corruption look like a damaged final
record or an incomplete tail.

Memory allocation can throw, including after a WAL record has become durable.
If applying that record to memory throws, the store enters the failed state and
rethrows; recovery determines the operation's outcome. Descriptor ownership is
RAII-based, including during constructor and compaction exceptions.

Database paths are resolved to an absolute location when the store is constructed,
so later changes to the process working directory cannot redirect compaction.
Empty paths and paths containing embedded NUL characters are rejected.

Instances require normal C++ lifetime coordination: join callers before destroying
the store. Do not reuse an inherited instance after `fork()`; its mutexes and
in-memory index are process-local. Do not externally replace or edit database files
while the store is open.

`Size`, `Items`, and `Range` provide rolling views across individually locked
shards, not atomic snapshots. TTLs use wall-clock Unix seconds: zero or negative
TTLs expire immediately, and TTL arithmetic overflow is rejected with `false`.

> **Format note:** records now end with a CRC32. WAL files written by an earlier
> version (no CRC) will not replay and should be recreated.

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
compaction, allocation failures, and subprocess exits at WAL/compaction crash
boundaries. These process-crash tests do not simulate actual power loss.

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

`kvstore_bench` reports read, write, mixed read/write, and compaction
throughput:

```bash
./kvstore_bench
```

Results on a 13th-gen Intel i9-13900H (20 hardware threads), Kingston NVMe SSD,
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

## Sanitizers

```bash
# data races
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread kvstore.cpp main.cpp -o kvstore_tsan && ./kvstore_tsan

# memory / undefined behavior
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -pthread kvstore.cpp main.cpp -o kvstore_asan && ./kvstore_asan
```
