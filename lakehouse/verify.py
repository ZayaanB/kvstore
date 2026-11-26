"""Exercise the local lakehouse with deterministic in-memory capture fixtures."""

import argparse
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from unittest.mock import patch

from export import Capture, MANIFEST_NAME, capture_sequence, new_capture, write_export
import publish as publisher

FIRST_TIME = 1_760_000_000
SECOND_TIME = FIRST_TIME + 2


def capture_fixture(warehouse, state, timestamp):
    with capture_sequence(warehouse) as sequence:
        return new_capture(sequence, timestamp), state.copy()


def stage(directory, capture, frozen):
    start = perf_counter()
    manifest = write_export(
        directory, capture,
        ((key, value, expiry) for key, (value, expiry) in frozen.items()),
        chunk_bytes=100,
    )
    size = sum(chunk["bytes"] for chunk in manifest["chunks"])
    print(f"fixture export {capture.sequence}: {manifest['row_count']} rows, "
          f"{size} chunk bytes in {perf_counter() - start:.3f}s", flush=True)
    return manifest


def query(spark, expected, capture_time, snapshot=None):
    version = "" if snapshot is None else f" VERSION AS OF {snapshot}"
    actual = spark.sql(f"""
        SELECT key, value, unix_seconds(expires_at), unix_seconds(captured_at)
        FROM {publisher.TABLE}{version}
    """).collect()
    assert len(actual) == len(expected), actual
    assert {bytes(r[0]): (bytes(r[1]), r[2], r[3]) for r in actual} == {
        key: (value, expiry, capture_time) for key, (value, expiry) in expected.items()
    }, actual


def expect_failure(action, exception, message):
    try:
        action()
    except exception as error:
        assert message in str(error), error
    else:
        raise AssertionError(f"expected failure containing {message!r}")


def interrupted_rows():
    yield b"one", b"row", None
    raise OSError("fixture shard read failed")


def verify(work):
    warehouse = work / "warehouse"
    first, second = work / "first", work / "second"
    binary_key = b"\x00\xff\n"
    source = {
        binary_key: (b"\xfe\x00\n\r", None),
        b"": (b"", None),
        b"edit": (b"before", None),
        b"delete": (b"gone later", None),
        b"expire": (b"live at first", SECOND_TIME),
        b"expired_at_capture": (b"excluded", FIRST_TIME),
    }
    start = perf_counter()
    first_capture, first_view = capture_fixture(warehouse, source, FIRST_TIME)
    print(f"fixture capture including sequence reservation: {perf_counter() - start:.3f}s",
          flush=True)
    stale_capture, stale_view = capture_fixture(warehouse, source, FIRST_TIME + 1)
    source[b"edit"] = (b"after", None)
    source[b"add"] = (b"\x00new", SECOND_TIME + 10)
    del source[b"delete"]
    second_capture, second_view = capture_fixture(warehouse, source, SECOND_TIME)
    assert (first_capture.sequence, stale_capture.sequence, second_capture.sequence) == (1, 2, 3)
    first_expected = {key: first_view[key] for key in (binary_key, b"", b"edit", b"delete", b"expire")}
    second_expected = {key: second_view[key] for key in (binary_key, b"", b"edit", b"add")}
    first_manifest = stage(first, first_capture, first_view)
    assert len(first_manifest["chunks"]) > 1
    stage(second, second_capture, second_view)
    stage(work / "stale", stale_capture, stale_view)
    (first / "unlisted.jsonl").write_bytes(b"unlisted data must be ignored")

    broken_capture, _ = capture_fixture(warehouse, source, SECOND_TIME)
    incomplete = work / "incomplete"
    expect_failure(lambda: write_export(incomplete, broken_capture, interrupted_rows()),
                   OSError, "fixture shard read failed")
    assert not (incomplete / MANIFEST_NAME).exists()
    assert list(incomplete.glob("chunk-*.jsonl"))

    corrupt = work / "corrupt"
    corrupt_manifest = stage(corrupt, broken_capture, second_view)
    entry = corrupt_manifest["chunks"][0]
    chunk = corrupt / entry["name"]
    original = chunk.read_bytes()
    duplicated = work / "duplicate-keys"
    write_export(duplicated, broken_capture, [(b"key", b"one", None), (b"key", b"two", None)],
                 chunk_bytes=1)

    with publisher.spark_session(warehouse) as spark:
        def publish(export):
            return publisher.publish(spark, warehouse, export)

        first_id = publish(first)
        start = perf_counter()
        query(spark, first_expected, FIRST_TIME)
        print(f"fixture Spark SQL table scan: {perf_counter() - start:.3f}s", flush=True)
        assert spark.sql(f"""
            SELECT count(*) FROM {publisher.TABLE}
            WHERE hex(key) = '00FF0A' AND hex(value) = 'FE000A0D' AND expires_at IS NULL
        """).first()[0] == 1

        expect_failure(lambda: publish(incomplete), ValueError, "missing manifest")
        chunk.unlink()
        expect_failure(lambda: publish(corrupt), ValueError, "missing or not a regular file")
        damaged = original.replace(b"AP8K", b"BP8K")
        assert damaged != original and len(damaged) == len(original)
        chunk.write_bytes(damaged)
        expect_failure(lambda: publish(corrupt), ValueError, "SHA-256")
        invalid = original.replace(b"key_b64", b"unknown")
        chunk.write_bytes(invalid)
        entry["sha256"] = hashlib.sha256(invalid).hexdigest()
        (corrupt / MANIFEST_NAME).write_text(json.dumps(corrupt_manifest), encoding="utf-8")
        expect_failure(lambda: publish(corrupt), ValueError, "invalid row fields")
        corrupt_manifest["row_count"] += 1
        (corrupt / MANIFEST_NAME).write_text(json.dumps(corrupt_manifest), encoding="utf-8")
        expect_failure(lambda: publish(corrupt), ValueError, "total row count")
        expect_failure(lambda: publish(duplicated), ValueError, "duplicate or null")

        with patch.object(publisher, "_commit", side_effect=OSError("before commit")):
            expect_failure(lambda: publish(second), OSError, "before commit")
        query(spark, first_expected, FIRST_TIME)
        assert spark.table(f"{publisher.TABLE}.snapshots").count() == 1

        commit = publisher._commit

        def lose_acknowledgement(rows, metadata):
            commit(rows, metadata)
            raise OSError("lost acknowledgement")

        with (patch.object(publisher, "_commit", side_effect=lose_acknowledgement),
              patch.object(publisher, "_committed_snapshot",
                           side_effect=[None, OSError("history unavailable")])):
            expect_failure(lambda: publish(second), OSError, "history unavailable")

        # Retry with a separate Spark catalog session after losing the commit result.
        retry_session = spark.newSession()
        second_id = publisher.publish(retry_session, warehouse, second)
        assert second_id != first_id
        assert publish(second) == second_id
        assert publish(first) == first_id
        assert spark.table(f"{publisher.TABLE}.snapshots").count() == 2
        expect_failure(lambda: publish(work / "stale"), ValueError, "stale")
        reused = work / "reused-sequence"
        stage(reused, new_capture(second_capture.sequence, SECOND_TIME), second_view)
        expect_failure(lambda: publish(reused), ValueError, "stale or reused")
        changed = work / "changed-export"
        stage(changed, Capture(second_capture.sequence, SECOND_TIME, second_capture.export_id), {})
        expect_failure(lambda: publish(changed), ValueError, "ID already used")
        query(spark, second_expected, SECOND_TIME)
        query(spark, first_expected, FIRST_TIME, first_id)

        for index in range(2):
            empty = work / f"empty-{index}"
            capture, _ = capture_fixture(warehouse, {}, SECOND_TIME + 1)
            stage(empty, capture, {})
            if index == 0:
                with patch.object(publisher, "_commit", side_effect=lose_acknowledgement):
                    empty_id = publish(empty)
            else:
                empty_id = publish(empty)
            assert publish(empty) == empty_id
            assert spark.table(publisher.TABLE).count() == 0
        assert spark.table(f"{publisher.TABLE}.snapshots").count() == 4
        query(spark, second_expected, SECOND_TIME, second_id)
    print("PASS: binary and empty strings, capture-time TTLs, full replacement, time travel, "
          "chunk and key validation, interrupted export, commit failure and reconciliation, "
          "duplicate and stale retries, and empty replacements", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, help="keep fixtures in a new directory for SQL inspection")
    args = parser.parse_args()
    if args.work_dir:
        args.work_dir.mkdir()
        verify(args.work_dir.resolve())
    else:
        with TemporaryDirectory(prefix="kvstore-lakehouse-") as work:
            verify(Path(work))


if __name__ == "__main__":
    main()
