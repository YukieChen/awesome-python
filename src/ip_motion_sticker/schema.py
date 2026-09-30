"""Versioned input schema for pipeline jobs."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class SchemaValidationError(ValueError):
    """Raised when persisted or user-provided data violates a schema."""


_JOB_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_MANIFEST_KEYS = frozenset({"schema_version", "job_id", "source", "output", "parameters"})


def _require_mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SchemaValidationError(f"{name} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise SchemaValidationError(f"{name} keys must be strings")
    return value


def validate_json_value(value: object, path: str = "parameters") -> None:
    """Reject values that cannot be represented reproducibly as JSON."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise SchemaValidationError(f"{path} contains a non-finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise SchemaValidationError(f"{path} keys must be strings")
            validate_json_value(item, f"{path}.{key}")
        return
    raise SchemaValidationError(f"{path} contains unsupported type {type(value).__name__}")


@dataclass(frozen=True)
class PipelineManifest:
    """Stable description of one requested pipeline run."""

    job_id: str
    source: str
    output: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    schema_version: int = 1

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise SchemaValidationError("schema_version must be 1")
        if not isinstance(self.job_id, str) or not _JOB_ID.fullmatch(self.job_id):
            raise SchemaValidationError("job_id must be 1-128 URL-safe characters")
        for name, value in (("source", self.source), ("output", self.output)):
            if not isinstance(value, str) or not value.strip():
                raise SchemaValidationError(f"{name} must be a non-empty string")
            if "\x00" in value:
                raise SchemaValidationError(f"{name} must not contain NUL bytes")
        parameters = _require_mapping(self.parameters, "parameters")
        validate_json_value(parameters)
        object.__setattr__(self, "parameters", dict(parameters))

    @classmethod
    def from_dict(cls, value: object) -> PipelineManifest:
        data = _require_mapping(value, "manifest")
        unknown = set(data) - _MANIFEST_KEYS
        missing = {"schema_version", "job_id", "source", "output"} - set(data)
        if unknown:
            raise SchemaValidationError(
                f"manifest has unknown fields: {', '.join(sorted(unknown))}"
            )
        if missing:
            raise SchemaValidationError(f"manifest is missing fields: {', '.join(sorted(missing))}")
        return cls(
            schema_version=data["schema_version"],
            job_id=data["job_id"],
            source=data["source"],
            output=data["output"],
            parameters=data.get("parameters", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "source": self.source,
            "output": self.output,
            "parameters": dict(self.parameters),
        }

    def resolve_paths(self, root: Path) -> tuple[Path, Path]:
        """Resolve paths without requiring either path to exist yet."""
        return (root / self.source).resolve(), (root / self.output).resolve()
