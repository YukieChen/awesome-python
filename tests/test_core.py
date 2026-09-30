from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from ip_motion_sticker import (
    Artifact,
    Checkpoint,
    CheckpointStore,
    PipelineManifest,
    PipelineState,
    PipelineStateMachine,
    SchemaValidationError,
    TransitionError,
    hash_bytes,
    hash_file,
    hash_json,
)


class ManifestTests(unittest.TestCase):
    def test_round_trip_and_deterministic_hash(self) -> None:
        first = PipelineManifest.from_dict(
            {
                "schema_version": 1,
                "job_id": "job-1",
                "source": "in.png",
                "output": "out.png",
                "parameters": {"z": 1, "a": [True, None]},
            }
        )
        second = {**first.to_dict(), "parameters": {"a": [True, None], "z": 1}}
        self.assertEqual(hash_json(first.to_dict()), hash_json(second))
        self.assertEqual(PipelineManifest.from_dict(first.to_dict()), first)

    def test_rejects_unknown_fields_and_non_json_values(self) -> None:
        valid = {"schema_version": 1, "job_id": "job", "source": "a", "output": "b"}
        with self.assertRaises(SchemaValidationError):
            PipelineManifest.from_dict({**valid, "surprise": True})
        with self.assertRaises(SchemaValidationError):
            PipelineManifest(**valid, parameters={"bad": float("nan")})


class ArtifactTests(unittest.TestCase):
    def test_hashes_bytes_and_files_and_detects_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "artifact.bin"
            path.write_bytes(b"motion")
            expected = hashlib.sha256(b"motion").hexdigest()
            self.assertEqual(hash_bytes(b"motion"), expected)
            self.assertEqual(hash_file(path, chunk_size=2), expected)
            artifact = Artifact.from_file(path)
            self.assertTrue(artifact.verify())
            path.write_bytes(b"changed")
            self.assertFalse(artifact.verify())

    def test_artifact_schema_is_strict(self) -> None:
        with self.assertRaises(SchemaValidationError):
            Artifact(path="x", sha256="not-a-hash", size=0)


class StateMachineTests(unittest.TestCase):
    def test_happy_path_and_terminal_state(self) -> None:
        machine = PipelineStateMachine()
        for state in ("validated", "processing", "encoding", "completed"):
            machine.transition_to(state)
        self.assertEqual(machine.state, PipelineState.COMPLETED)
        self.assertTrue(machine.is_terminal)

    def test_invalid_transition_does_not_mutate_state(self) -> None:
        machine = PipelineStateMachine()
        with self.assertRaises(TransitionError):
            machine.transition_to(PipelineState.ENCODING)
        self.assertEqual(machine.state, PipelineState.CREATED)

    def test_failure_can_retry_from_validation(self) -> None:
        machine = PipelineStateMachine(PipelineState.PROCESSING)
        machine.transition_to(PipelineState.FAILED)
        machine.transition_to(PipelineState.VALIDATED)
        self.assertEqual(machine.state, PipelineState.VALIDATED)


class CheckpointTests(unittest.TestCase):
    manifest_hash = "a" * 64

    def test_atomic_round_trip_transition_and_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "input.bin"
            artifact_path.write_bytes(b"data")
            checkpoint = Checkpoint("job", self.manifest_hash)
            checkpoint = checkpoint.with_artifact("input", Artifact.from_file(artifact_path))
            checkpoint = checkpoint.transition(PipelineState.VALIDATED)
            store = CheckpointStore(Path(directory) / "nested" / "checkpoint.json")
            store.save(checkpoint)
            self.assertEqual(store.load(self.manifest_hash), checkpoint)

    def test_detects_tampering_and_manifest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            store = CheckpointStore(path)
            store.save(Checkpoint("job", self.manifest_hash))
            document = json.loads(path.read_text())
            document["checkpoint"]["state"] = "completed"
            path.write_text(json.dumps(document))
            with self.assertRaisesRegex(SchemaValidationError, "checksum"):
                store.load()
            store.save(Checkpoint("job", self.manifest_hash))
            with self.assertRaisesRegex(SchemaValidationError, "different manifest"):
                store.load("b" * 64)

    def test_optimistic_revision_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory) / "checkpoint.json")
            store.save(Checkpoint("job", self.manifest_hash))
            with self.assertRaisesRegex(TransitionError, "revision changed"):
                store.save(Checkpoint("job", self.manifest_hash), expected_revision=4)


if __name__ == "__main__":
    unittest.main()
