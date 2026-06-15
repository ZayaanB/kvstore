# kvstore

A small embedded key-value store written from scratch in C++20. It's a from-scratch systems exercise, sharded in-memory index for concurrency, a binary write-ahead log for durability, crash recovery on startup, and a compaction routine that never blocks readers. No external dependencies, just the standard library.

## How it works

![KVStore architecture](docs/architecture.svg)

**Sharded index.** The keyspace is split across 16 independent shards (`std::unordered_map` + `std::shared_mutex` each), chosen via `std::hash<std::string> % 16`. Reads take a `shared_lock` so concurrent `Get()` calls on the same shard never block each other; writes take a `unique_lock` scoped to just that shard. There's no global database lock.

**Write-ahead log.** Every `Set` and `Delete` is serialized as a binary record (type + key length + value length + payload), written to `db.wal`, and `flush()`ed before the in-memory shard is updated. If the process dies between the flush and the in-memory update, the WAL still has the record and recovery will replay it.

**Recovery.** On startup, the WAL is read sequentially from the beginning and every record is replayed into the sharded index. If the log ends mid-record (a torn write from a crash), recovery stops cleanly at the last complete record instead of throwing.

**Compaction.** `Compact()` takes the WAL's write mutex for the duration of the snapshot and swap: it walks each shard under a `shared_lock` writing only the currently-live key/value pairs to a temp file, then closes the old log, renames the temp file into place, and reopens it for appends. Because writers append to the WAL under that same mutex, no `Set`/`Delete` can slip into the old log after its shard has been snapshotted but before the swap, so nothing acknowledged is ever dropped. Readers never take the WAL mutex, so `Get`/`Range`/`Size` proceed concurrently; writers block only for the compaction window.

The API is simple:

```cpp
kvstore::KVStore store("data/mydb.db"); // opens (or creates) and recovers

store.Set("hello", "world");            // -> bool
store.Get("hello");                     // -> std::optional<std::string>
store.Delete("hello");                  // -> bool
store.Compact();                        // -> bool
```

## Running it yourself

### Prerequisites

- A C++20 compiler (GCC ≥ 11 or Clang ≥ 14)
- CMake ≥ 3.20
- Linux or macOS

### Build and run

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j
./kvstore_test
```

`kvstore_test` runs a stress and crash-recovery suite: 4 worker threads doing concurrent writes/reads, a compaction while readers are active, a simulated crash with a torn WAL record, and a restart with full recovery validation. A successful run ends with:

=== ALL TESTS PASSED ===

### Use it interactively (CLI)

`kvstore_cli` is an interactive shell over the store (it takes an optional db path):

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

### Benchmark

`kvstore_bench` reports write, mixed read/write, and compaction throughput:

```bash
./kvstore_bench
```

### Optional: run under sanitizers

```bash
# Data race detection
g++ -std=c++20 -O1 -g -fsanitize=thread -pthread kvstore.cpp main.cpp -o kvstore_tsan
./kvstore_tsan

# Memory errors and undefined behavior
g++ -std=c++20 -O1 -g -fsanitize=address,undefined -pthread kvstore.cpp main.cpp -o kvstore_asan
./kvstore_asan
```
