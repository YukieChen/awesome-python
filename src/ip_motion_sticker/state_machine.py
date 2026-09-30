"""Guarded state transitions for resumable pipeline execution."""

from __future__ import annotations

from enum import Enum


class TransitionError(RuntimeError):
    """Raised when a state transition is not part of the pipeline graph."""


class PipelineState(str, Enum):
    CREATED = "created"
    VALIDATED = "validated"
    PROCESSING = "processing"
    ENCODING = "encoding"
    COMPLETED = "completed"
    FAILED = "failed"


_TRANSITIONS: dict[PipelineState, frozenset[PipelineState]] = {
    PipelineState.CREATED: frozenset({PipelineState.VALIDATED, PipelineState.FAILED}),
    PipelineState.VALIDATED: frozenset({PipelineState.PROCESSING, PipelineState.FAILED}),
    PipelineState.PROCESSING: frozenset({PipelineState.ENCODING, PipelineState.FAILED}),
    PipelineState.ENCODING: frozenset({PipelineState.COMPLETED, PipelineState.FAILED}),
    PipelineState.COMPLETED: frozenset(),
    PipelineState.FAILED: frozenset({PipelineState.VALIDATED}),
}


class PipelineStateMachine:
    def __init__(self, state: PipelineState | str = PipelineState.CREATED) -> None:
        try:
            self._state = PipelineState(state)
        except (TypeError, ValueError) as error:
            raise TransitionError(f"unknown pipeline state: {state!r}") from error

    @property
    def state(self) -> PipelineState:
        return self._state

    @property
    def is_terminal(self) -> bool:
        return not _TRANSITIONS[self._state]

    def can_transition_to(self, target: PipelineState | str) -> bool:
        try:
            candidate = PipelineState(target)
        except (TypeError, ValueError):
            return False
        return candidate in _TRANSITIONS[self._state]

    def transition_to(self, target: PipelineState | str) -> PipelineState:
        try:
            candidate = PipelineState(target)
        except (TypeError, ValueError) as error:
            raise TransitionError(f"unknown pipeline state: {target!r}") from error
        if candidate not in _TRANSITIONS[self._state]:
            raise TransitionError(
                f"cannot transition from {self._state.value} to {candidate.value}"
            )
        self._state = candidate
        return self._state
