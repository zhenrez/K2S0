"""Forked simulation plane that cannot silently become human evidence."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping

from .aggregate import TwinState
from .errors import InvariantViolation
from .ownership import EventOwnershipPolicy
from .types import ActorContext, EventEnvelope, EventPlane


@dataclass(slots=True)
class SimulationBranch:
    branch_id: str
    twin_id: str
    base_sequence: int
    scenario: str
    events: list[EventEnvelope] = field(default_factory=list)
    predicted_state: dict[str, Any] = field(default_factory=dict)

    def append(self, event: EventEnvelope, *, actor: ActorContext) -> None:
        EventOwnershipPolicy().authorize(event, actor)
        if event.plane is not EventPlane.SIMULATION:
            raise InvariantViolation("simulation branch accepts simulation-plane events only")
        if event.twin_id != self.twin_id:
            raise InvariantViolation("simulation event belongs to a different twin")
        self.events.append(event)
        changes = event.payload.get("predicted_state")
        if isinstance(changes, Mapping):
            self.predicted_state.update(changes)


class SimulationEngine:
    def fork(self, state: TwinState, *, scenario: str) -> SimulationBranch:
        if not scenario.strip():
            raise InvariantViolation("simulation scenario is required")
        return SimulationBranch(
            branch_id=str(uuid.uuid4()),
            twin_id=state.twin_id,
            base_sequence=state.sequence,
            scenario=scenario,
        )

    def proposal_payload(self, branch: SimulationBranch) -> dict[str, Any]:
        """Produce a review proposal, never an EvidenceIngested event."""

        return {
            "branch_id": branch.branch_id,
            "base_sequence": branch.base_sequence,
            "scenario": branch.scenario,
            "predicted_state": dict(branch.predicted_state),
            "simulation_event_ids": [event.event_id for event in branch.events],
            "requires_human_review": True,
            "may_not_be_used_as_evidence": True,
        }
