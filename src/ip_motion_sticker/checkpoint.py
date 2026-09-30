"""Atomic, integrity-checked persistence for pipeline progress."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import Artifact, hash_json, re_full_sha256
from .schema import SchemaValidationError
from .state_machine import PipelineState, PipelineStateMachine, TransitionError

_CHECKPOINT_KEYS = frozenset(
    {"schema_version", "job_id", "manifest_hash", "state", "revision", "updated_at", "artifacts"}
)


@dataclass(frozen=True)
class Checkpoint:
    job_id: str
    manifest_hash: str
    state: PipelineState = PipelineState.CREATED
    revision: int = 0
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    artifacts: Mapping[str, Artifact] = field(default_factory=dict)
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise SchemaValidationError("checkpoint schema_version must be 1")
        if not isinstance(self.job_id, str) or not self.job_id:
            raise SchemaValidationError("checkpoint job_id must be a non-empty string")
        if not re_full_sha256(self.manifest_hash):
            raise SchemaValidationError("manifest_hash must be a SHA-256 digest")
        try:
            object.__setattr__(self, "state", PipelineState(self.state))
        except (TypeError, ValueError) as error:
            raise SchemaValidationError("checkpoint has an unknown state") from error
        if type(self.revision) is not int or self.revision < 0:
            raise SchemaValidationError("checkpoint revision must be a non-negative integer")
        try:
            parsed = datetime.fromisoformat(self.updated_at)
        except (TypeError, ValueError) as error:
            raise SchemaValidationError("updated_at must be an ISO-8601 timestamp") from error
        if parsed.tzinfo is None:
            raise SchemaValidationError("updated_at must include a timezone")
        if not isinstance(self.artifacts, Mapping) or not all(
            isinstance(name, str) and name and isinstance(artifact, Artifact)
            for name, artifact in self.artifacts.items()
        ):
            raise SchemaValidationError("artifacts must map non-empty names to Artifact values")
        object.__setattr__(self, "artifacts", dict(self.artifacts))

    @classmethod
    def from_dict(cls, value: object) -> Checkpoint:
        if not isinstance(value, dict):
            raise SchemaValidationError("checkpoint must be an object")
        if set(value) != _CHECKPOINT_KEYS:
            raise SchemaValidationError("checkpoint fields do not match schema version 1")
        raw_artifacts = value["artifacts"]
        if not isinstance(raw_artifacts, dict):
            raise SchemaValidationError("artifacts must be an object")
        return cls(
            schema_version=value["schema_version"],
            job_id=value["job_id"],
            manifest_hash=value["manifest_hash"],
            state=value["state"],
            revision=value["revision"],
            updated_at=value["updated_at"],
            artifacts={name: Artifact.from_dict(item) for name, item in raw_artifacts.items()},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "manifest_hash": self.manifest_hash,
            "state": self.state.value,
            "revision": self.revision,
            "updated_at": self.updated_at,
            "artifacts": {
                name: artifact.to_dict() for name, artifact in sorted(self.artifacts.items())
            },
        }

    def transition(self, target: PipelineState | str) -> Checkpoint:
        machine = PipelineStateMachine(self.state)
        new_state = machine.transition_to(target)
        return replace(
            self,
            state=new_state,
            revision=self.revision + 1,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    def with_artifact(self, name: str, artifact: Artifact) -> Checkpoint:
        if not isinstance(name, str) or not name:
            raise SchemaValidationError("artifact name must be a non-empty string")
        if not isinstance(artifact, Artifact):
            raise SchemaValidationError("artifact value must be an Artifact")
        artifacts = dict(self.artifacts)
        artifacts[name] = artifact
        return replace(
            self,
            artifacts=artifacts,
            revision=self.revision + 1,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )


class CheckpointStore:
    """Store a checkpoint in a checksummed JSON envelope using atomic replace."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def save(self, checkpoint: Checkpoint, expected_revision: int | None = None) -> None:
        if expected_revision is not None and self.path.exists():
            current = self.load()
            if current.revision != expected_revision:
                message = (
                    f"checkpoint revision changed: expected {expected_revision}, "
                    f"got {current.revision}"
                )
                raise TransitionError(message)
        payload = checkpoint.to_dict()
        envelope = {"checksum": hash_json(payload), "checkpoint": payload}
        encoded = (json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def load(self, expected_manifest_hash: str | None = None) -> Checkpoint:
        try:
            envelope = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise SchemaValidationError(f"cannot read checkpoint: {error}") from error
        if not isinstance(envelope, dict) or set(envelope) != {"checksum", "checkpoint"}:
            raise SchemaValidationError("checkpoint envelope is invalid")
        if envelope["checksum"] != hash_json(envelope["checkpoint"]):
            raise SchemaValidationError("checkpoint checksum mismatch")
        checkpoint = Checkpoint.from_dict(envelope["checkpoint"])
        if (
            expected_manifest_hash is not None
            and checkpoint.manifest_hash != expected_manifest_hash
        ):
            raise SchemaValidationError("checkpoint belongs to a different manifest")
        return checkpoint
