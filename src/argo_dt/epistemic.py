"""Epistemic lineage and gap-directed elicitation primitives.

This module remains storage-neutral. SQLite supplies the authoritative lineage
index through ``DependencyIndex``; a graph database may project the same edges
but is never required for correctness.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from .aggregate import TwinState
from .errors import InvariantViolation
from .ports import DependencyIndex
from .types import Sensitivity, canonical_json

MAX_ELICITATION_ANSWER_BYTES = 1024 * 1024


class EpistemicNodeKind(StrEnum):
    EVIDENCE = "evidence"
    ENTITY = "entity"
    CLAIM = "claim"
    CONTRADICTION = "contradiction"
    CORRECTION = "correction"
    PROJECTION = "projection"


@dataclass(frozen=True, slots=True, order=True)
class LineageNode:
    kind: EpistemicNodeKind
    node_id: str

    def __post_init__(self) -> None:
        if not self.node_id:
            raise InvariantViolation("lineage node_id is required")


@dataclass(frozen=True, slots=True)
class LineageTrace:
    twin_id: str
    root: LineageNode
    ancestors: tuple[LineageNode, ...]
    dependents: tuple[LineageNode, ...]
    evidence_by_independence_group: tuple[tuple[str, tuple[str, ...]], ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "argo.dt.lineage-trace/v1",
            "twin_id": self.twin_id,
            "root": {"kind": self.root.kind.value, "node_id": self.root.node_id},
            "ancestors": [
                {"kind": node.kind.value, "node_id": node.node_id}
                for node in self.ancestors
            ],
            "dependents": [
                {"kind": node.kind.value, "node_id": node.node_id}
                for node in self.dependents
            ],
            "evidence_groups": [
                {"independence_group": group, "evidence_ids": list(evidence_ids)}
                for group, evidence_ids in self.evidence_by_independence_group
            ],
        }


class LineageTracer:
    """Returns reversible ancestry and impact traces from a derived index."""

    def __init__(self, index: DependencyIndex) -> None:
        self._index = index

    def trace(self, state: TwinState, root: LineageNode) -> LineageTrace:
        ancestors = tuple(
            LineageNode(EpistemicNodeKind(kind), node_id)
            for kind, node_id in self._index.ancestors(
                state.twin_id,
                root.kind.value,
                root.node_id,
                up_to_sequence=state.sequence,
            )
        )
        dependents = tuple(
            LineageNode(EpistemicNodeKind(kind), node_id)
            for kind, node_id in self._index.dependents(
                state.twin_id,
                root.kind.value,
                root.node_id,
                up_to_sequence=state.sequence,
            )
        )
        evidence_ids = {
            node.node_id for node in ancestors if node.kind is EpistemicNodeKind.EVIDENCE
        }
        if root.kind is EpistemicNodeKind.EVIDENCE:
            evidence_ids.add(root.node_id)
        grouped: dict[str, list[str]] = {}
        for evidence_id in sorted(evidence_ids):
            evidence = state.evidence.get(evidence_id)
            if evidence is None:
                continue
            group = str(evidence.get("independence_group", ""))
            if group:
                grouped.setdefault(group, []).append(evidence_id)
        return LineageTrace(
            twin_id=state.twin_id,
            root=root,
            ancestors=ancestors,
            dependents=dependents,
            evidence_by_independence_group=tuple(
                (group, tuple(ids)) for group, ids in sorted(grouped.items())
            ),
        )


class GapKind(StrEnum):
    MISSING_CLAIM = "missing_claim"
    INDEPENDENCE_DEFICIT = "independence_deficit"
    CONTESTED_CLAIM = "contested_claim"
    STALE_LINEAGE = "stale_lineage"
    UNRESOLVED_CONTRADICTION = "unresolved_contradiction"


@dataclass(frozen=True, slots=True)
class EpistemicGap:
    gap_id: str
    kind: GapKind
    target_ids: tuple[str, ...]
    rationale: str


@dataclass(frozen=True, slots=True)
class ElicitationQuestion:
    question_id: str
    gap_id: str
    prompt: str
    requested_relation: str


@dataclass(frozen=True, slots=True)
class ElicitationPlan:
    plan_id: str
    twin_id: str
    source_sequence: int
    objective: str
    claim_ids: tuple[str, ...]
    gaps: tuple[EpistemicGap, ...]
    questions: tuple[ElicitationQuestion, ...]

    def question(self, question_id: str) -> ElicitationQuestion:
        for question in self.questions:
            if question.question_id == question_id:
                return question
        raise InvariantViolation("question does not belong to elicitation plan")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "argo.dt.elicitation-plan/v1",
            "plan_id": self.plan_id,
            "twin_id": self.twin_id,
            "source_sequence": self.source_sequence,
            "objective": self.objective,
            "claim_ids": list(self.claim_ids),
            "gaps": [
                {
                    "gap_id": gap.gap_id,
                    "kind": gap.kind.value,
                    "target_ids": list(gap.target_ids),
                    "rationale": gap.rationale,
                }
                for gap in self.gaps
            ],
            "questions": [
                {
                    "question_id": question.question_id,
                    "gap_id": question.gap_id,
                    "prompt": question.prompt,
                    "requested_relation": question.requested_relation,
                }
                for question in self.questions
            ],
        }


@dataclass(frozen=True, slots=True)
class ElicitationResponse:
    response_id: str
    question_id: str
    answer: str
    sensitivity: Sensitivity
    answered_at: datetime

    def __post_init__(self) -> None:
        if not self.response_id or not self.question_id or not self.answer.strip():
            raise InvariantViolation("elicitation response identity and answer are required")
        if len(self.answer.encode("utf-8")) > MAX_ELICITATION_ANSWER_BYTES:
            raise InvariantViolation("elicitation answer exceeds the 1 MiB limit")
        if self.answered_at.tzinfo is None:
            raise InvariantViolation("elicitation answered_at must be timezone-aware")


class GapDirectedElicitor:
    """Deterministically asks only about observable gaps in current state."""

    def __init__(self, *, minimum_independence_groups: int = 2) -> None:
        if minimum_independence_groups < 1:
            raise ValueError("minimum_independence_groups must be positive")
        self._minimum_groups = minimum_independence_groups

    @staticmethod
    def _stable_id(prefix: str, *parts: object) -> str:
        material = canonical_json([prefix, *parts])
        return f"{prefix}-{uuid.uuid5(uuid.NAMESPACE_URL, material)}"

    def plan(
        self,
        state: TwinState,
        *,
        objective: str,
        claim_ids: tuple[str, ...] | None = None,
    ) -> ElicitationPlan:
        if not objective.strip():
            raise InvariantViolation("elicitation objective is required")
        selected_ids = tuple(
            sorted(state.claims if claim_ids is None else set(claim_ids))
        )
        unknown = set(selected_ids).difference(state.claims)
        if unknown:
            raise InvariantViolation("elicitation target references an unknown claim")

        gaps: list[EpistemicGap] = []
        questions: list[ElicitationQuestion] = []

        def add_gap(
            kind: GapKind,
            targets: tuple[str, ...],
            rationale: str,
            prompt: str,
            *,
            identity_parts: tuple[str, ...] = (),
        ) -> None:
            gap_id = self._stable_id(
                "gap",
                state.twin_id,
                state.sequence,
                objective,
                kind.value,
                *targets,
                *identity_parts,
            )
            question_id = self._stable_id("question", gap_id)
            gaps.append(EpistemicGap(gap_id, kind, targets, rationale))
            questions.append(
                ElicitationQuestion(
                    question_id,
                    gap_id,
                    prompt,
                    "supports_or_refutes",
                )
            )

        if not selected_ids:
            add_gap(
                GapKind.MISSING_CLAIM,
                (),
                "No claim currently addresses the objective.",
                f"What firsthand observation or document would help establish: {objective}?",
            )

        for claim_id in selected_ids:
            claim = state.claims[claim_id]
            statement = str(claim.get("statement", claim_id))
            groups = {
                str(reference.get("independence_group", ""))
                for reference in claim.get("provenance", [])
                if isinstance(reference, dict) and reference.get("independence_group")
            }
            if len(groups) < self._minimum_groups:
                add_gap(
                    GapKind.INDEPENDENCE_DEFICIT,
                    (claim_id,),
                    f"Claim has {len(groups)} independent group(s); "
                    f"{self._minimum_groups} required.",
                    "Provide an independent observation or source that supports or "
                    f"refutes: {statement}",
                )
            if claim_id in state.contested_claim_ids:
                add_gap(
                    GapKind.CONTESTED_CLAIM,
                    (claim_id,),
                    "The claim remains contested.",
                    "What evidence would distinguish the competing interpretations "
                    f"of: {statement}?",
                )
            if claim_id in state.stale_claim_ids:
                add_gap(
                    GapKind.STALE_LINEAGE,
                    (claim_id,),
                    "The claim depends on deleted or superseded evidence.",
                    f"Can you provide current replacement evidence for: {statement}?",
                )

        for contradiction_id, contradiction in sorted(state.contradictions.items()):
            if contradiction_id in state.resolved_contradiction_ids:
                continue
            targets = tuple(sorted(str(item) for item in contradiction.get("claim_ids", [])))
            if claim_ids is not None and not set(targets).intersection(selected_ids):
                continue
            add_gap(
                GapKind.UNRESOLVED_CONTRADICTION,
                targets,
                "Competing claims have not been adjudicated.",
                "What dated, firsthand, or independently sourced evidence resolves "
                "this contradiction?",
                identity_parts=(contradiction_id,),
            )

        plan_id = self._stable_id(
            "plan",
            state.twin_id,
            state.sequence,
            objective,
            self._minimum_groups,
            *selected_ids,
        )
        return ElicitationPlan(
            plan_id=plan_id,
            twin_id=state.twin_id,
            source_sequence=state.sequence,
            objective=objective,
            claim_ids=selected_ids,
            gaps=tuple(gaps),
            questions=tuple(questions),
        )

    def verify_plan(self, state: TwinState, plan: ElicitationPlan) -> None:
        """Fail closed unless ``plan`` is exactly derivable from recorded state."""

        if state.twin_id != plan.twin_id or state.sequence != plan.source_sequence:
            raise InvariantViolation("elicitation plan source state is unavailable")
        expected = self.plan(
            state,
            objective=plan.objective,
            claim_ids=plan.claim_ids,
        )
        if expected != plan:
            raise InvariantViolation("elicitation plan authenticity check failed")
