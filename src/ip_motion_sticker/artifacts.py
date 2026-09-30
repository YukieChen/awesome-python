"""Deterministic content hashing and artifact metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO

from .schema import SchemaValidationError, validate_json_value

DEFAULT_CHUNK_SIZE = 1024 * 1024


def hash_bytes(data: bytes) -> str:
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    return sha256(data).hexdigest()


def _canonical_json(value: Any) -> bytes:
    validate_json_value(value, "value")
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def hash_json(value: Any) -> str:
    """Hash a JSON value using a canonical key order and compact encoding."""
    return hash_bytes(_canonical_json(value))


def _hash_stream(stream: BinaryIO, chunk_size: int) -> tuple[str, int]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = sha256()
    size = 0
    while chunk := stream.read(chunk_size):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def hash_file(path: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    with Path(path).open("rb") as stream:
        digest, _ = _hash_stream(stream, chunk_size)
    return digest


@dataclass(frozen=True)
class Artifact:
    """Content-addressed output produced by a pipeline stage."""

    path: str
    sha256: str
    size: int

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path or "\x00" in self.path:
            raise SchemaValidationError("artifact path must be a non-empty string")
        if not isinstance(self.sha256, str) or not re_full_sha256(self.sha256):
            raise SchemaValidationError(
                "artifact sha256 must be 64 lowercase hexadecimal characters"
            )
        if type(self.size) is not int or self.size < 0:
            raise SchemaValidationError("artifact size must be a non-negative integer")

    @classmethod
    def from_file(cls, path: str | Path) -> Artifact:
        artifact_path = Path(path)
        with artifact_path.open("rb") as stream:
            digest, size = _hash_stream(stream, DEFAULT_CHUNK_SIZE)
        return cls(path=str(artifact_path), sha256=digest, size=size)

    @classmethod
    def from_dict(cls, value: object) -> Artifact:
        if not isinstance(value, dict) or set(value) != {"path", "sha256", "size"}:
            raise SchemaValidationError("artifact must contain exactly path, sha256, and size")
        return cls(path=value["path"], sha256=value["sha256"], size=value["size"])

    def to_dict(self) -> dict[str, str | int]:
        return {"path": self.path, "sha256": self.sha256, "size": self.size}

    def verify(self, root: str | Path | None = None) -> bool:
        path = Path(self.path)
        if root is not None and not path.is_absolute():
            path = Path(root) / path
        try:
            current = Artifact.from_file(path)
        except OSError:
            return False
        return current.sha256 == self.sha256 and current.size == self.size


def re_full_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)
