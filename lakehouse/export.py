"""Local snapshot interchange, independent of Spark and the C++ library."""

import argparse
import base64
from contextlib import closing, contextmanager, ExitStack
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import struct
import subprocess
from uuid import UUID, uuid4

import fcntl

MAX_SEQUENCE = (1 << 63) - 1
MAX_TIMESTAMP = 253402300799  # 9999-12-31 23:59:59 UTC
MAX_KEY_BYTES = 1 << 20
MAX_VALUE_BYTES = 1 << 28
MAX_LINE_BYTES = 4 * ((MAX_KEY_BYTES + 2) // 3 + (MAX_VALUE_BYTES + 2) // 3) + 128
MANIFEST_NAME = "manifest.json"
SCHEMA_VERSION = 1
ROW_FIELDS = {"key_b64", "value_b64", "expires_at"}


@dataclass(frozen=True)
class Capture:
    sequence: int
    captured_at: int
    export_id: str


def _integer(value, minimum, maximum=MAX_SEQUENCE):
    return type(value) is int and minimum <= value <= maximum


def _validate_capture(capture):
    if not _integer(capture.sequence, 1) or not _integer(capture.captured_at, 0, MAX_TIMESTAMP):
        raise ValueError("invalid capture sequence or timestamp")
    if not isinstance(capture.export_id, str) or str(UUID(capture.export_id)) != capture.export_id:
        raise ValueError("export ID must be a canonical UUID")


@contextmanager
def table_lock(warehouse):
    warehouse.mkdir(parents=True, exist_ok=True)
    with (warehouse / ".kvstore.lock").open("ab") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


@contextmanager
def capture_sequence(warehouse):
    """Reserve a durable number and hold the table lock through snapshot acquisition."""
    with table_lock(warehouse):
        with closing(sqlite3.connect(warehouse / ".kvstore-sequence.sqlite3")) as db:
            db.execute("CREATE TABLE IF NOT EXISTS captures (id INTEGER PRIMARY KEY CHECK (id = 1), "
                       "sequence INTEGER NOT NULL)")
            db.execute("INSERT OR IGNORE INTO captures VALUES (1, 0)")
            sequence = db.execute("SELECT sequence FROM captures WHERE id = 1").fetchone()[0] + 1
            if sequence > MAX_SEQUENCE:
                raise OverflowError("capture sequence exhausted")
            db.execute("UPDATE captures SET sequence = ? WHERE id = 1", (sequence,))
            db.commit()
            yield sequence


def _validate_expiry(expires_at, capture):
    if expires_at is not None and not _integer(expires_at, capture.captured_at + 1, MAX_TIMESTAMP):
        raise ValueError("row expiration must be null or later than capture time")


def _encode_row(key, value, expires_at):
    if not isinstance(key, bytes) or not isinstance(value, bytes):
        raise TypeError("snapshot keys and values must be bytes")
    if len(key) > MAX_KEY_BYTES or len(value) > MAX_VALUE_BYTES:
        raise ValueError("snapshot key or value exceeds its size limit")
    expiry = b"null" if expires_at is None else str(expires_at).encode("ascii")
    return (b'{"key_b64":"' + base64.b64encode(key) + b'","value_b64":"'
            + base64.b64encode(value) + b'","expires_at":'
            + expiry + b"}\n")


def _json_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate JSON field")
    return result


def _json(data):
    return json.loads(data.decode("utf-8"), object_pairs_hook=_json_object)


def _validate_binary(value, max_bytes):
    if not isinstance(value, str):
        raise ValueError("binary fields must be base64 strings")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ValueError("invalid base64") from error
    if len(decoded) > max_bytes or base64.b64encode(decoded).decode("ascii") != value:
        raise ValueError("noncanonical base64 or decoded field exceeds its size limit")


def _validate_line(line, capture):
    if len(line) > MAX_LINE_BYTES or not line.endswith(b"\n"):
        raise ValueError("oversized or unterminated row")
    row = _json(line)
    if not isinstance(row, dict) or row.keys() != ROW_FIELDS:
        raise ValueError("invalid row fields")
    _validate_binary(row["key_b64"], MAX_KEY_BYTES)
    _validate_binary(row["value_b64"], MAX_VALUE_BYTES)
    _validate_expiry(row["expires_at"], capture)


def _chunk_name(index):
    return f"chunk-{index:08d}.jsonl"


def _sync_dir(path):
    fd = os.open(path, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _check_chunk(path, chunk, capture=None, copy_to=None):
    """Check the exact bytes; optionally validate rows and write a private Spark input copy."""
    digest = hashlib.sha256()
    size = rows = 0
    with path.open("rb") as stream:
        if capture is None:
            for block in iter(lambda: stream.read(1 << 20), b""):
                digest.update(block)
                size += len(block)
                rows += block.count(b"\n")
        else:
            while line := stream.readline(MAX_LINE_BYTES + 1):
                _validate_line(line, capture)
                digest.update(line)
                size += len(line)
                rows += 1
                copy_to.write(line)
    if (size, rows, digest.hexdigest()) != (chunk["bytes"], chunk["row_count"], chunk["sha256"]):
        raise ValueError(f"chunk failed byte, row count, or SHA-256 validation: {path.name}")


def write_export(directory, capture, rows, *, chunk_bytes=8 << 20):
    """Stream (key bytes, value bytes, nullable expiry seconds) from one captured view.

    The cursor must raise on traversal errors and provide each live key once.
    Expirations are evaluated using the supplied capture time throughout the stream.
    """
    _validate_capture(capture)
    if not _integer(chunk_bytes, 1):
        raise ValueError("chunk_bytes must be positive")
    directory.mkdir()
    _sync_dir(directory.parent)
    chunks = []
    total = 0
    stream = None
    size = count = 0
    digest = hashlib.sha256()

    def finish_chunk():
        stream.flush()
        os.fsync(stream.fileno())
        stream.close()
        name = _chunk_name(len(chunks))
        chunk = {"name": name, "bytes": size, "row_count": count, "sha256": digest.hexdigest()}
        _check_chunk(directory / name, chunk)
        chunks.append(chunk)

    try:
        for key, value, expires_at in rows:
            if expires_at is not None:
                if not _integer(expires_at, 0, MAX_TIMESTAMP):
                    raise ValueError("invalid row expiration")
                if expires_at <= capture.captured_at:
                    continue
            line = _encode_row(key, value, expires_at)
            if stream is not None and size + len(line) > chunk_bytes:
                finish_chunk()
                stream = None
            if stream is None:
                stream = (directory / _chunk_name(len(chunks))).open("xb")
                size = count = 0
                digest = hashlib.sha256()
            stream.write(line)
            digest.update(line)
            size += len(line)
            count += 1
            total += 1
        if stream is not None:
            finish_chunk()
    finally:
        if stream is not None:
            stream.close()

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "export_id": capture.export_id,
        "capture_sequence": capture.sequence,
        "captured_at": capture.captured_at,
        "row_count": total,
        "chunks": chunks,
    }
    pending = directory / ".manifest.tmp"
    with pending.open("xb") as output:
        output.write(json.dumps(manifest, separators=(",", ":")).encode("ascii") + b"\n")
        output.flush()
        os.fsync(output.fileno())
    _sync_dir(directory)
    pending.replace(directory / MANIFEST_NAME)
    _sync_dir(directory)
    return manifest


def validate_export(directory, input_copy):
    """Return the manifest and its digest after copying only validated listed chunks."""
    try:
        raw = (directory / MANIFEST_NAME).read_bytes()
    except FileNotFoundError as error:
        raise ValueError("incomplete export: missing manifest.json") from error
    manifest = _json(raw)
    fields = {"schema_version", "export_id", "capture_sequence", "captured_at", "row_count", "chunks"}
    if not isinstance(manifest, dict) or manifest.keys() != fields:
        raise ValueError("invalid manifest fields")
    if (type(manifest["schema_version"]) is not int or manifest["schema_version"] != SCHEMA_VERSION
            or not _integer(manifest["row_count"], 0) or not isinstance(manifest["chunks"], list)):
        raise ValueError("invalid manifest version, row count, or chunk list")
    capture = Capture(manifest["capture_sequence"], manifest["captured_at"], manifest["export_id"])
    _validate_capture(capture)
    total = 0
    for index, chunk in enumerate(manifest["chunks"]):
        name = _chunk_name(index)
        if (not isinstance(chunk, dict) or chunk.keys() != {"name", "bytes", "row_count", "sha256"}
                or chunk["name"] != name or not _integer(chunk["bytes"], 1)
                or not _integer(chunk["row_count"], 1)
                or not isinstance(chunk["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", chunk["sha256"])):
            raise ValueError(f"invalid manifest entry for {name}")
        total += chunk["row_count"]
    if total != manifest["row_count"]:
        raise ValueError("manifest total row count disagrees with chunk counts")
    for chunk in manifest["chunks"]:
        name = chunk["name"]
        source = directory / name
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"export chunk is missing or not a regular file: {name}")
        with (input_copy / name).open("xb") as output:
            _check_chunk(source, chunk, capture, output)
    return manifest, hashlib.sha256(raw).hexdigest()


def new_capture(sequence, captured_at):
    return Capture(sequence, captured_at, str(uuid4()))


def _read_exact(stream, size):
    data = stream.read(size)
    if len(data) != size:
        raise OSError("KVStore snapshot stream ended early")
    return data


def _snapshot_rows(process):
    stream = process.stdout
    while True:
        tag = _read_exact(stream, 1)
        if tag == b"\0":
            if stream.read(1) or process.wait() != 0:
                raise OSError("KVStore did not finish the snapshot successfully")
            return
        if tag != b"\1":
            raise ValueError("invalid KVStore snapshot record tag")
        key_length, value_length, expiry = struct.unpack("<IIq", _read_exact(stream, 16))
        if key_length > MAX_KEY_BYTES or value_length > MAX_VALUE_BYTES:
            raise ValueError("snapshot record exceeds KVStore size limits")
        yield _read_exact(stream, key_length), _read_exact(stream, value_length), expiry or None


@contextmanager
def _snapshot_process(cli, database):
    with subprocess.Popen([cli.resolve(), "--snapshot", database], stdout=subprocess.PIPE) as process:
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()


def export_database(cli, database, warehouse, output, *, chunk_bytes=8 << 20):
    with ExitStack() as source:
        with capture_sequence(warehouse) as sequence:
            process = source.enter_context(_snapshot_process(cli, database))
            if _read_exact(process.stdout, 8) != b"KVSNAP01":
                raise ValueError("unsupported KVStore snapshot stream")
            captured_at, = struct.unpack("<q", _read_exact(process.stdout, 8))
            capture = new_capture(sequence, captured_at)
        return write_export(output, capture, _snapshot_rows(process), chunk_bytes=chunk_bytes)


def main():
    parser = argparse.ArgumentParser(description="Export a consistent snapshot from a KVStore database.")
    parser.add_argument("--cli", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--warehouse", required=True, type=Path)
    parser.add_argument("--chunk-bytes", type=int, default=8 << 20)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    manifest = export_database(args.cli, args.database, args.warehouse, args.output,
                               chunk_bytes=args.chunk_bytes)
    print(f"Export {manifest['export_id']} contains {manifest['row_count']} live rows")


if __name__ == "__main__":
    main()
