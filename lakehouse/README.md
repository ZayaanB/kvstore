# Local KVStore snapshot publication

The Python producer streams a consistent view from the C++ database, then
Spark publishes it as an Iceberg table snapshot. The export preserves binary
keys and values and includes each live row's expiration time.

## Export an existing database

Close any other process using the database before export. The engine opens an
existing database and checks every shard before it provides rows. Set up Spark
and the Iceberg runtime in [Run locally](#run-locally) before publication.

```bash
python3 lakehouse/export.py --cli ./build/kvstore_cli \
  --database data/store --warehouse /tmp/kvstore-warehouse /tmp/kvstore-export
spark-submit --master 'local[2]' --jars /tmp/kvstore-iceberg-runtime-1.10.1.jar \
  lakehouse/publish.py --warehouse /tmp/kvstore-warehouse /tmp/kvstore-export
```

Use a new output directory for each capture. The export uses the same capture
time for all rows. Values with an expiration outside Iceberg's configured year
9999 limit fail export with an error. An interrupted or incomplete capture
cannot publish a table version.

## Run locally

From the repository root, use Python 3.9+ and Java 17 or 21. Spark and Iceberg
are isolated from the C++ build.

```bash
python3 -m venv /tmp/kvstore-lakehouse-verify/venv
source /tmp/kvstore-lakehouse-verify/venv/bin/activate
python -m pip install -r lakehouse/requirements.txt
curl -fL --max-time 120 -o /tmp/kvstore-iceberg-runtime-1.10.1.jar https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 # Set to your Java 17 or 21 installation.
export SPARK_LOCAL_IP=127.0.0.1 PYTHONDONTWRITEBYTECODE=1
spark-submit --master 'local[2]' --jars /tmp/kvstore-iceberg-runtime-1.10.1.jar \
  lakehouse/verify.py --kvstore-cli ./build/kvstore_cli
```

The check deletes its temporary warehouse and export directories on exit. Add
`--work-dir /tmp/kvstore-lakehouse-demo` to retain a run in a new directory.
The verification script ends with two empty replacements, so query the first
two snapshots to see the fixture rows. To retry the second export from a new
publisher process, run:

```bash
spark-submit --master 'local[2]' --jars /tmp/kvstore-iceberg-runtime-1.10.1.jar \
  lakehouse/publish.py --warehouse /tmp/kvstore-lakehouse-demo/warehouse \
  /tmp/kvstore-lakehouse-demo/second
```

Open Spark SQL against the same warehouse:

```bash
spark-sql --master 'local[2]' --jars /tmp/kvstore-iceberg-runtime-1.10.1.jar \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///tmp/kvstore-lakehouse-demo/warehouse \
  --conf spark.sql.session.timeZone=UTC
```

```sql
SELECT count(*) FROM lake.kvstore.live;
SELECT snapshot_id, summary['kvstore.capture-sequence'] AS capture_sequence
FROM lake.kvstore.live.snapshots ORDER BY committed_at;
-- Substitute the snapshot ID with capture_sequence = '1'.
SELECT hex(key), hex(value), expires_at, captured_at
FROM lake.kvstore.live VERSION AS OF <snapshot_id>
WHERE hex(key) = '00FF0A';
```

## Export format v1

`capture_sequence(warehouse)` reserves a durable sequence in SQLite and holds
the table lock inside the context manager. Acquire and pin the source snapshot
inside that context, then exit it before streaming. Use
`new_capture(sequence, captured_at)` and call
`write_export(directory, capture, rows)` with a new directory and an iterator
of `(key_bytes, value_bytes, expires_at)` triples. The producer skips rows with
an expiration at or before capture. The adapter maps the engine's no-expiry
sentinel to `None`. The source cursor must resolve versions and tombstones,
yield each key at most once, and raise on incomplete traversal.

- A chunk is UTF-8 newline-delimited JSON with one object per row and a final LF.
  Each object contains `key_b64`, `value_b64`, and `expires_at`. Binary strings
  use canonical padded RFC 4648 base64. An empty string encodes as `""`.
  `expires_at` is either a Unix time in UTC seconds or JSON null.
- Chunks are named `chunk-00000000.jsonl`, `chunk-00000001.jsonl`, and so on.
  The default target is 8 MiB, with a larger single row in its own chunk.
  The format supports keys up to 1 MiB and values up to 256 MiB. A large row
  requires memory for its encoded and decoded representations. The manifest
  takes space proportional to the chunk count.
- `manifest.json` contains `schema_version` (1), a canonical UUID `export_id`,
  positive `capture_sequence`, `captured_at` (Unix UTC seconds), total
  `row_count`, and ordered `chunks`. Each chunk entry contains `name`,
  `bytes`, `row_count`, and lowercase `sha256` of its exact file bytes.
  An empty snapshot has no chunks and a zero row count.
- The producer syncs and checks the chunks, then installs the completion
  manifest by rename. The publisher validates and copies only the listed
  chunks into a private temporary directory and checks decoded key uniqueness
  with Spark. Iceberg writes the result as Parquet with binary `key` and
  `value`, nullable `expires_at`, and source `captured_at`. Both timestamps
  represent UTC instants.

## Publication and cleanup

Use one coordinator for this table on one local filesystem. Keep the sequence
database and stable lock file with the warehouse across restarts. Every
publisher uses that lock while comparing the capture sequence and committing.
External writers, rollbacks, and concurrent maintenance fall outside this
local workflow. Sequence gaps are normal after abandoned captures.

A full overwrite removes deleted and expired rows in one Iceberg commit.
Snapshot summaries include the export ID, capture sequence and time, and
the SHA-256 of the manifest. Keep the export directory intact until publication
is confirmed. If the outcome is uncertain, rerun the publisher with the same
directory. It looks for that export ID in Iceberg history before starting
another write. A matching commit returns the original snapshot ID, even if a
newer export is current. A different export with a sequence at or below the
current one is rejected.

Once the producer and any publisher have stopped using an export, delete its
staging directory. This includes abandoned chunks without a completion manifest.
Keep prior Iceberg snapshots for the time travel and retry window you need. Run
Iceberg maintenance while publication is idle, using `expire_snapshots` with
your cutoff and minimum generations, for example:

```sql
CALL lake.system.expire_snapshots(
  table => 'kvstore.live',
  older_than => TIMESTAMP '2026-01-01 00:00:00',
  retain_last => 5
);
```

Expired historical exports with older capture sequences are still rejected on
retry. To clean abandoned Iceberg data files, use Iceberg's
`remove_orphan_files` procedure after the retry window, with maintenance and
writers kept separate. Review its `dry_run => true` output first. Delete the
entire retained demo directory when finished with the fixture.

## Verification scope

The fixture checks byte-for-byte rows and TTLs, two full publications with
additions, overwrites, deletes, and expiration, Spark SQL count and binary
filtering, historical queries, empty replacements, corrupt and incomplete
exports, duplicate keys, stale sequences, and commit failure and recovery.
With `--kvstore-cli`, the check also exports from a real database, publishes two
versions, reads the older version after publication, and rejects a source with
missing shard data.

In the local check, the first export had 5 rows and 326 chunk bytes. The
second had 4 rows and 238 chunk bytes. Fixture capture with sequence
reservation took 0.076 s, and the first Spark SQL scan took 0.540 s. The
separate publisher process returned the second export's original snapshot
ID. The C++ Release build passed all five CTest checks:

```bash
cmake -S . -B /tmp/kvstore-lakehouse-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/kvstore-lakehouse-build --parallel 4
ctest --test-dir /tmp/kvstore-lakehouse-build --output-on-failure
```
