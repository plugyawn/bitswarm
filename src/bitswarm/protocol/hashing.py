"""Canonical hashing helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, BinaryIO

import orjson


def canonical_json_bytes(payload: Any) -> bytes:
    """Serialize payload using deterministic JSON ordering."""
    return orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_stream(stream: BinaryIO, *, size: int | None = None, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    remaining = size
    while True:
        if remaining is None:
            data = stream.read(chunk_size)
        elif remaining <= 0:
            break
        else:
            data = stream.read(min(chunk_size, remaining))
            remaining -= len(data)
        if not data:
            break
        digest.update(data)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    with path.open("rb") as file:
        return sha256_stream(file)


def sha256_file_range(path: Path, *, offset: int, size: int) -> str:
    with path.open("rb") as file:
        file.seek(offset)
        return sha256_stream(file, size=size)


def manifest_root(payload_without_root: dict[str, Any]) -> str:
    """Compute the canonical manifest root hash."""
    return sha256_bytes(canonical_json_bytes(payload_without_root))

