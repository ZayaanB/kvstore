"""Validate a completed export and publish one local Iceberg replacement."""

import argparse
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, functions as F

from export import MAX_LINE_BYTES, table_lock, validate_export

TABLE = "lake.kvstore.live"
TABLE_SCHEMA = "key BINARY, value BINARY, expires_at TIMESTAMP, captured_at TIMESTAMP"
WIRE_SCHEMA = "key_b64 STRING, value_b64 STRING, expires_at LONG"


@contextmanager
def spark_session(warehouse):
    spark = (
        SparkSession.builder.appName("KVStore snapshot publication")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse.resolve().as_uri())
        .config("spark.sql.catalog.lake.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        yield spark
    finally:
        spark.stop()


def _committed_snapshot(spark, metadata):
    matches = (
        spark.table(f"{TABLE}.snapshots")
        .where(F.col("summary")["kvstore.export-id"] == metadata["kvstore.export-id"])
        .select("snapshot_id", "summary")
        .take(2)
    )
    if not matches:
        return None
    if len(matches) != 1 or any(matches[0].summary.get(k) != v for k, v in metadata.items()):
        raise ValueError("export ID already used with different metadata or more than one snapshot")
    return matches[0].snapshot_id


def _current_sequence(spark):
    current = spark.sql(f"""
        SELECT s.summary['kvstore.capture-sequence'] AS sequence
        FROM {TABLE}.refs r LEFT JOIN {TABLE}.snapshots s ON r.snapshot_id = s.snapshot_id
        WHERE r.name = 'main'
    """).first()
    if current is None:
        return 0
    if current.sequence is None:
        raise ValueError("current snapshot was published outside the KVStore coordinator")
    return int(current.sequence)


def _validated_rows(spark, copied, manifest):
    if not manifest["chunks"]:
        return spark.createDataFrame([], TABLE_SCHEMA)
    rows = (
        spark.read.schema(WIRE_SCHEMA)
        .option("mode", "FAILFAST")
        .option("maxStringLen", MAX_LINE_BYTES)
        .json([str(copied / chunk["name"]) for chunk in manifest["chunks"]])
        .select(
            F.unbase64("key_b64").alias("key"),
            F.unbase64("value_b64").alias("value"),
            F.timestamp_seconds("expires_at").alias("expires_at"),
            F.timestamp_seconds(F.lit(manifest["captured_at"])).alias("captured_at"),
        )
    )
    counts = rows.agg(F.count("*"), F.count_distinct("key")).first()
    if counts[0] != manifest["row_count"]:
        raise ValueError("Spark row count disagrees with manifest")
    if counts[1] != counts[0]:
        raise ValueError("duplicate or null decoded key in export")
    return rows


def _commit(rows, metadata):
    writer = rows.writeTo(TABLE).option("write-format", "parquet")
    for key, value in metadata.items():
        writer.option(f"snapshot-property.{key}", value)
    # Iceberg writes data files before atomically committing the full replacement.
    writer.overwrite(F.lit(True))


def publish(spark, warehouse, directory):
    warehouse = warehouse.resolve()
    if spark.conf.get("spark.sql.catalog.lake.warehouse") != warehouse.as_uri():
        raise ValueError("publisher warehouse differs from Spark catalog warehouse")
    with TemporaryDirectory(prefix="kvstore-validated-") as temp:
        copied = Path(temp)
        manifest, digest = validate_export(directory, copied)
        metadata = {
            "kvstore.export-id": manifest["export_id"],
            "kvstore.capture-sequence": str(manifest["capture_sequence"]),
            "kvstore.captured-at": str(manifest["captured_at"]),
            "kvstore.manifest-sha256": digest,
        }
        with table_lock(warehouse):
            exists = spark.catalog.tableExists(TABLE)
            if exists:
                previous = _committed_snapshot(spark, metadata)
                if previous is not None:
                    return previous
                if manifest["capture_sequence"] <= _current_sequence(spark):
                    raise ValueError("stale or reused capture sequence")
            rows = _validated_rows(spark, copied, manifest)
            if not exists:
                spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.kvstore")
                spark.sql(f"CREATE TABLE {TABLE} ({TABLE_SCHEMA}) USING iceberg")
            try:
                _commit(rows, metadata)
            except Exception:
                # A lost acknowledgement can look like a failure after the commit.
                # If history is unavailable, the next invocation checks it before writing.
                committed = _committed_snapshot(spark, metadata)
                if committed is None:
                    raise
                return committed
            committed = _committed_snapshot(spark, metadata)
            if committed is None:
                raise RuntimeError("commit acknowledgement could not be confirmed; retry this export")
            return committed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("export", type=Path)
    args = parser.parse_args()
    with spark_session(args.warehouse) as spark:
        snapshot = publish(spark, args.warehouse, args.export)
    print(f"Iceberg snapshot ID {snapshot}")


if __name__ == "__main__":
    main()
