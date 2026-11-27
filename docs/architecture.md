# KVStore architecture

KVStore's main job is to store and retrieve values by key. It is **embedded**, which means the C++ library runs inside the application on one machine. Recent writes stay in memory for quick reads, and the engine keeps data on disk so the database can grow beyond the write buffer. A fixed view of the data also feeds local MapReduce jobs and Spark-to-Iceberg analytics.

## System at a glance

```mermaid
flowchart LR
    App["C++ application or CLI"] --> Engine["Embedded KVStore"]
    Bench["Benchmark harness"] --> Engine
    Engine --> Storage["16 local shards<br/>Memtables, WALs, sorted tables"]
    Engine --> Snapshot["Streaming snapshot"]
    Engine --> Recorder["Optional metrics recorder"]
    Recorder --> Exporter["CLI /metrics endpoint"]
    Exporter --> Prometheus --> Grafana
    Launcher["Local monitor launcher"] -.-> App
    Launcher -.-> Grafana
    Bench --> Results["JSON benchmark runs"]
    Results --> Compare["HTML comparison"]
    Snapshot --> MR["Local MapReduce"]
    MR --> Output["Result file"]
    Snapshot --> Manifest["Export files and manifest"]
    Manifest --> Spark --> Iceberg["Iceberg table snapshots"]
```

Both data-processing paths stream a snapshot, a view of the database at a single point in time. The embedded engine, CLI, and benchmark harness have no runtime dependency on the analytics or monitoring services.

| Component | Entry point | Present behavior |
| --- | --- | --- |
| Storage engine | [kvstore.hpp](../kvstore.hpp), [kvstore.cpp](../kvstore.cpp) | Durable point operations, disk-backed reads, rolling range scans, offline WAL migration |
| Shell and exporter | [cli.cpp](../cli.cpp), [metrics_http.cpp](../metrics_http.cpp) | Interactive commands and an optional Prometheus endpoint |
| Batch computation | [mapreduce.hpp](../mapreduce.hpp), [word_count_main.cpp](../examples/word_count_main.cpp) | Single-worker mapper, disk-backed shuffle, and word count from a database or fixture |
| Analytical publication | [lakehouse/export.py](../lakehouse/export.py), [lakehouse/publish.py](../lakehouse/publish.py) | Verified database snapshots published to a local Spark/Iceberg table |
| Performance measurement | [bench.cpp](../bench.cpp), [benchmarks/compare.py](../benchmarks/compare.py) | Repeatable workload trials, latency distributions, JSON, and baseline comparison |
| Observability | [monitor](../monitor), [metrics.cpp](../metrics.cpp) | Browser-based status and app launcher, plus Prometheus metrics, Grafana panels, and alerts |

## How data flows

An application links the KVStore library or uses its command-line interface. Each key is routed to one of 16 shards. A shard keeps recent writes in memory, records them in a log on disk, and moves them into sorted files as its memory buffer fills.

```mermaid
flowchart TD
    Write["Set or SetBatch"] --> Route["Pick the shard from the key"]
    Route --> WAL["Append to the log and sync"]
    WAL --> Mem["Update the memtable"]
    Mem --> Done["Report success"]
    Mem -. "buffer full, handled by a later write" .-> Flush["Flush to a sorted table"]
    Flush -. "eight tables, handled by the next write" .-> Merge["Compact into fewer tables"]
    Read["Get"] --> Pick["Pick the shard from the key"]
    Pick --> Check["Check the memtable"]
    Check -- "not found" --> Tables["Search sorted tables, newest first"]
```

1. **Write.** The engine picks the shard, makes room in its buffer if needed, appends the change to the log, and syncs it. It then updates memory and reports success.
2. **Flush and merge.** When the next write would overfill a buffer, the engine first saves the buffer as a sorted file and starts a new log. Once a shard has eight sorted files, its next write attempts to merge them.
3. **Read.** The engine picks the same shard and checks memory first. If the key is not there, it searches that shard's sorted files from newest to oldest.
4. **Restart.** The engine uses each shard's manifest to find the current files, then replays the log to rebuild writes that had not been flushed.
5. **Damage and restore.** In degraded mode, a damaged shard is taken out of service while the others keep handling individual keys. An operator can restore that shard from a checkpoint.

Three other paths sit around the engine:

- **Analytics.** A snapshot streams the database as it was at one moment. It can feed the local MapReduce runner, or be exported and published through Spark into an Iceberg table. Writes wait while the snapshot is open.
- **Monitoring.** When metrics are enabled, the engine records operation counts and timings. The command-line interface serves them to Prometheus, and Grafana charts them.
- **Benchmarking.** A separate harness drives the engine with set workloads and saves the results as JSON.

## Design decisions

| Decision | Reason | Cost |
| --- | --- | --- |
| 16 shards | Limits damage to one part of the data, and lets reads on other shards continue during a write | Shards do not make writes parallel, because a global lock serializes them |
| Log each change before applying it | Confirmed writes can be replayed after a crash | A write waits for a disk sync |
| Buffer writes, then flush sorted files | Writes append to a log, and files stay fixed once written | A lookup may need to check several files |
| Merge at eight sorted files | Bounds the build-up of files during ordinary writes. Eight is a fixed threshold | The write that triggers a merge takes longer |
| Batches within one shard | One log and one sync cover the batch | A batch cannot span shards, and an interrupted batch can recover partially |

## Embedded storage engine

Each key identifies one value, with an optional expiration time. The C++ API accepts arbitrary bytes, so the application decides whether a value contains text, a structured record, or other data. Keys can be up to 1 MiB and values up to 256 MiB. The command-line interface accepts text commands.

Each **shard** is a group of keys stored in its own directory. For new databases, **rendezvous consistent hashing** gives each of the 16 shards a repeatable score for a key and picks the highest score. In a routing test, adding a candidate moves only keys that pick it as their new winner. Removing a candidate moves only keys it used to own. The running database keeps all 16 shards fixed. Its `METADATA` file records the format, routing, shard count, and write buffer size. [Routing implementation](../routing.hpp)

Each shard has three main parts.

- A **memtable** holds recent writes in an ordered in-memory buffer.
- A **write-ahead log (WAL)** records changes on disk so the engine can replay them after a crash.
- **Sorted tables (SSTables)** hold data flushed to disk. Keys are stored in order, and the files stay fixed once written. The shard's `MANIFEST` lists which files recovery should use.

A lock file permits one open instance per database. Opening through a symbolic link uses the same lock.

### Write and read paths

1. **Make room.** The default write buffer is 16 MiB, split across 16 shards. If a write would overfill its shard's buffer, the engine first writes that buffer to a sorted disk file. A single oversized write can still take the normal write path and flush during the call.
2. **Save the changes.** The engine appends an ordered record for each change to the WAL. Each record has a checksum, a value used to detect corruption. `Set()` requests one WAL sync per call. `SetBatch()` usually appends several records with one system call and one sync. A sync asks the operating system to make the log durable. The engine updates the buffer and reports success after the sync succeeds. If the append or sync fails, it tries to roll the WAL back to its prior size.
3. **Group batch writes by shard.** Use `ShardForKey()` first. A multi-entry batch must fit within that shard's share of the write buffer. A batch that spans shards or exceeds the limit returns `false` before logging any updates. If a key appears more than once, its last update wins.
4. **Read the newest copy.** A single-key lookup checks the write buffer first, then the disk tables from newest to oldest. The disk lookup uses each table's key range and an index that records where blocks start. A delete marker or expired entry hides older values. The engine checks the on-disk records it uses to decide whether to return the value.

A global lock makes writes and flushes take turns. A write also holds its shard's lock so readers see a completed update or batch. Other shards can keep serving single-key reads. Disk lookups and scans share another lock, and the operating system can serve repeated disk reads from its cache.

`Size()` counts rows without loading every value. `Range()` and `Items()` gather and sort their results in memory. They visit shards in turn, so a scan can reflect writes from different moments. Use a snapshot when the entire scan must reflect one moment.

### Flush and restart

A **flush** turns a shard's write buffer into a sorted file. The engine syncs the new table and creates the next WAL, then saves those directory changes. It switches the manifest to the new files and syncs the directory again before removing the old WAL. If that last sync fails, the engine requires a reopen to learn which file set survived. An unacknowledged write can survive such a failure.

On restart, the engine checks the active files and replays the WAL to rebuild recent writes. It can discard an incomplete final record or a final record with a bad checksum. Each batch uses the same record format as a single write, so an interrupted batch can leave some of its records on disk and recover partially. A bad final checksum can also discard an acknowledged record damaged after the sync. Length fields and block offsets have no separate checksums, so some corruption after startup can make a lookup miss a record or make recovery misread a record boundary. Crash durability depends on the filesystem and device honoring syncs.

### Crash test points

The crash test confirms one write, stops a child process at a chosen point, reopens the database, checks that earlier data survived, and writes again.

| Point | The process stops |
| ---: | --- |
| 1 | Partway through appending a log record |
| 2 | While writing a new sorted table |
| 3 | After syncing the new sorted table |
| 4 | After renaming the new manifest into place |
| 5 | After syncing the directory that holds the new manifest |
| 6 | Before removing the old log |
| 7 | After removing the old log |

The first point interrupts a write. The other six interrupt a flush started by `Compact()`. These tests simulate process crashes, not power loss. Interrupted merges, batches, restores, and migrations have separate tests. [Crash tests](../tests/failure_tests.cpp)

### Compaction and snapshots

**Compaction** merges sorted files so reads have fewer files to check. When a shard has eight sorted files, its next write attempts a merge. The merge happens during the write and can delay other writes and reads on that shard. A recoverable merge failure leaves the current files in use. The next attempt follows another flush. `Compact()` also flushes and merges on demand. Both paths keep the newest value and can drop deleted or expired entries when every older copy is covered. The engine syncs the merged file and switches the manifest before removing the files it replaced.

`OpenSnapshot()` streams rows one at a time so the entire result need not fit in memory. It holds the writer lock to keep all 16 shards and their expiration cutoff fixed at the capture time. Single-key reads can continue. Writes wait until streaming finishes or the snapshot is closed. Open, consume, and close it on the same thread.

### Shard isolation, restore, and failover

In the default strict mode, a shard failure can stop the whole database. In **degraded mode**, the engine takes the damaged shard out of service. The healthy shards can still read and write individual keys. Requests to read a damaged shard report an error. Whole-database scans, counts, and snapshots require every shard.

A **checkpoint** is a verified copy of the database at one point in time. It can be made through the C++ API while writes wait, or from the CLI after other processes close the database. To restore one shard, the engine verifies a replacement from a checkpoint of the same database and switches that shard to the replacement in one rename. Other shards keep their current data. The restored shard returns to its checkpoint state, so it loses changes made to that shard afterward. The old shard files stay available for diagnosis. For databases using legacy routing, restore records the new format before switching the shard so an older binary refuses to open stale files.

After host loss, an operator can bring up the latest checkpoint on another host and redirect the application once the original host can no longer accept writes. Recovery reaches the point captured by that checkpoint.

## Local MapReduce

One local worker runs three steps against a live snapshot or sample files.

1. **Map.** Read one input row at a time and emit intermediate key-value pairs. In word count, the mapper emits `(word, 1)` for each word.
2. **Shuffle.** Group pairs by the key the mapper produced. The worker sorts batches in memory and writes them to temporary files when the buffer fills. It merges a limited number of files at a time.
3. **Reduce.** Read one group at a time and combine its values. Word count adds the ones to get a total for each word.

The job's groups depend on the keys emitted by the mapper, so matching words end up together regardless of where their input rows were stored. A large group can stream from disk. Each open file reader still holds one record in memory, and callbacks manage their own memory. The temporary disk budget covers intermediate files and the staged result. The worker syncs the result before using a rename to replace the destination. [MapReduce API](../mapreduce.hpp)

## Spark and Iceberg publication

KVStore handles individual key lookups. The lakehouse pipeline uses **Spark** to process rows and **Iceberg** to track a table's schema and versions over data files. SQL queries can read any retained version. The table has named columns for keys, values, and timestamps. Keys and values keep their original bytes. A query that needs fields inside a value must decode that value first.

The Python exporter streams the database at one capture time and gives the export a sequence number. It writes chunks of newline-delimited JSON and uses base64 to carry arbitrary bytes as text. Once the stream finishes successfully, it writes a manifest that lists the chunks, row counts, and checksums. The publisher verifies the manifest and chunks before Spark checks that keys are unique and writes a full replacement table as Parquet, a column-oriented file format. Each Iceberg commit records which export it came from. That lets a retry find a completed commit and prevents an older export from replacing a newer table. [Lakehouse runbook](../lakehouse/README.md)

## Benchmarks and monitoring

`kvstore_bench` measures reads, durable writes, mixed traffic, compaction during traffic, and reopen time. Each trial starts with a fresh store and checks the data after reopening. **Throughput** is the number of completed operations per second. Each key written in a batch counts as one operation. Looking up a key that was deliberately left absent also counts as a completed read. **p99 read latency** is the time within which roughly 99 out of 100 reads finished.

The 95% read and 5% write comparison used a Release build, four threads, and 1,000 preloaded keys. For reads that target existing keys, 90% of selections favor four hot keys and the other 10% use the full keyspace. Each worker writes to one of the shards holding those hot keys. Each batch size ran three trials totaling 456,000 reads and 24,000 writes.

| Writes per call | Total ops/s | p99 read latency | p99 write call latency |
| ---: | ---: | ---: | ---: |
| 1 | 4,247 | 6.53 µs | 360.71 ms |
| 100 | 313,943 | 2.56 µs | 14.16 ms |
| 500 | 1,221,288 | 1.92 µs | 12.32 ms |
| 1,000 | 1,907,559 | 1.47 µs | 13.63 ms |

This run used an Intel i9-13900H. The 1,000 preloaded keys fit in memory. Workers collect writes during the read loop and submit them in bursts. Write call latency starts when the batch is submitted and includes time waiting for locks and syncing the WAL. Throughput also includes time spent collecting the batch. The largest batch size produced 24 timed write calls, so its p99 reflects the slowest calls in a small sample.

With one write per call, every write waits for its own sync while holding the global writer lock, so writers queue behind one another. A batch shares one sync across many updates. The tradeoff is that the caller must collect updates, and none is confirmed until the batch call returns.

The [recorded results](../benchmarks/results/read-heavy-hot-shards) note that the source tree had uncommitted changes when the build was configured. The runs with large batches are also very short: the first trial with 1,000 writes per call took under 0.1 seconds. Treat the figures as measurements of this setup, and confirm them with longer runs from a clean commit.

### Reproduce the read-heavy benchmark

Use a Release build and run the four configurations sequentially. Each uses the same hot keys, read/write ratio, and write-shard assignments.

```bash
mkdir -p build/benchmarks/read-heavy-hot-shards
for size in 1 100 500 1000; do
  ./build/kvstore_bench --workload mixed --distribution hot --read-percent 95 \
    --hot-keys 4 --threads 4 --trials 3 --ops 40000 --batch-size "$size" --mode latency \
    --json "build/benchmarks/read-heavy-hot-shards/batch-$size.json" || break
done
```

The HTML report compares matching workload parameters and checks the host, compiler, build type, shard count, and telemetry settings before showing percentage changes. Benchmark JSON records the revision at configure time. Reconfigure after changing revisions and use matching compiler flags and test conditions.

### Monitoring

Run `./monitor` to start monitoring and open the browser launcher. The CLI can expose a local `/metrics` endpoint with operation counts and timing buckets. It omits keys, values, and database paths from metric labels and avoids taking storage locks. Prometheus collects the measurements and checks four alert conditions. Grafana shows how throughput, latency, errors, and storage activity change over time. Its live charts count each `SetBatch()` call as one request. The benchmark reports each key written as an operation. [Monitoring setup](../README.md#local-monitoring)

## Limits and possible next steps

| Current limit | Possible next step |
| --- | --- |
| Concurrent `Set()` calls each sync separately | Group commit, so waiting writers share a sync |
| A global lock serializes writes and flushes | Finer-grained write locking |
| Merging runs inside a write | Background compaction |
| A lookup can search several sorted tables | Bloom filters to skip tables that cannot hold the key |
| Disk lookups and scans share one lock | Allow independent disk reads to run together |
| Recovery after host loss is manual and limited to the checkpoint | Replication with a rule for which host may write |

## Verification

CTest covers batch append and sync failures, automatic compaction, recovery after interrupted merges and shard restore, isolation of damaged shards, snapshot consistency, routing, MapReduce, and the benchmark harness. Separate integration checks exercise [the Spark/Iceberg publisher](../lakehouse/README.md#run-locally) and [the monitoring stack](../README.md#manual-setup-and-docker-compose).
