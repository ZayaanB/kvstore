# kvstore

An embedded key-value store in C++20. Writes go to a binary write-ahead log and
are `fsync`ed to disk before memory changes, so acknowledged writes survive both
process crashes and power loss. Every record carries a CRC32 so recovery rejects
torn or corrupted data instead of trusting it. The index is split into 16 shards
for concurrency, and a compaction step reclaims dead log space without blocking
readers. No dependencies beyond the standard library (POSIX for `fsync`).

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
  parallel and writes only lock one shard. No global lock.
- **Write-ahead log** — each `Set`/`Delete` is a binary record (type, key len,
  value len, payload, trailing CRC32) written to `db.wal` and `fdatasync`ed
  before the in-memory map changes. A failed or partial write is truncated back
  to the last record boundary so the tail stays valid.
- **Recovery** — on startup the log is replayed; each record's CRC32 is checked,
  so a torn or corrupted record stops replay cleanly instead of loading garbage.
- **Compaction** — `Compact()` holds the WAL mutex while it writes live keys to a
  temp file, `fdatasync`s it, and atomically renames it into place (syncing the
  directory), so no acknowledged write is lost even across power loss. Readers
  never touch the WAL mutex and keep running.

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
- Integers are written in host byte order, so a WAL is single-host, not portable
  across architectures.

`Get`/`Set`/`Delete` are `O(1)` on one shard; `Range`/`Items`/`Size` scan all
shards and cost `O(n)` (plus an `O(n log n)` sort for ordered results).

## Durability

`SetInternal`/`Delete` hold `wal_mutex_` across the whole append-then-apply so the
log order and the visible in-memory state can never diverge. The write path is:

1. serialize the record (with its CRC) and `write()` it to the WAL;
2. `fdatasync()` the WAL so the bytes are on physical media;
3. only then mutate the shard's map and return success.

If the `write` or `fdatasync` fails, the WAL is `ftruncate`d back to the previous
record boundary and the call returns `false` without touching memory, so the tail
is always a run of whole, valid records. On startup `Recover()` replays records
and stops at the first one whose CRC fails or whose header/payload is short — a
torn tail from a crash — instead of loading garbage.

> **Format note:** records now end with a CRC32. WAL files written by an earlier
> version (no CRC) will not replay and should be recreated.

## Build and run

Needs a C++20 compiler (GCC 11+ / Clang 14+) and CMake 3.20+ on Linux or macOS.

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j
./kvstore_test
```

`kvstore_test` runs the stress and crash-recovery suite: concurrent
writes/reads, a compaction while readers are active, a simulated crash with a
torn WAL record, and a restart that re-validates everything. It ends with
`=== ALL TESTS PASSED ===`.

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

`kvstore_bench` reports write, mixed read/write, and compaction throughput:

```bash
./kvstore_bench
```

## Sanitizers

```bash
# data races
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread kvstore.cpp main.cpp -o kvstore_tsan && ./kvstore_tsan

# memory / undefined behavior
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -pthread kvstore.cpp main.cpp -o kvstore_asan && ./kvstore_asan
```
