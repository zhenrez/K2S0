from __future__ import annotations

import unittest
from datetime import UTC, datetime

from seed_contracts import (
    AdmissionDisposition,
    AdmissionResult,
    AdmissionStatus,
    CanonicalEffect,
    ContractViolation,
    ReadinessResult,
    RepresentationSnapshotRef,
    SemanticBasisRef,
    SubjectRef,
)


class Day0ArchitectureContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.subject = SubjectRef("subject-1")
        self.snapshot = RepresentationSnapshotRef(
            subject_ref=self.subject,
            snapshot_id="representation-17",
            canonical_sequence=17,
            integrity_ref="sha256:representation-17",
        )

    def test_at54_admitted_empty_effects_requires_explicit_noop(self) -> None:
        with self.assertRaises(ContractViolation):
            AdmissionResult(
                request_id="request-1",
                admission_id="admission-1",
                status=AdmissionStatus.ADMITTED,
                disposition=AdmissionDisposition.EFFECTS_APPLIED,
                canonical_effects=(),
                resulting_representation_state_ref=self.snapshot,
            )

        result = AdmissionResult(
            request_id="request-1",
            admission_id="admission-1",
            status=AdmissionStatus.ADMITTED,
            disposition=AdmissionDisposition.NO_OP_ALREADY_CANONICAL,
            canonical_effects=(),
            resulting_representation_state_ref=self.snapshot,
        )
        self.assertEqual(
            result.disposition,
            AdmissionDisposition.NO_OP_ALREADY_CANONICAL,
        )

    def test_at57_multiple_readiness_profiles_share_one_semantic_basis(self) -> None:
        basis = SemanticBasisRef.seal(
            subject_ref=self.subject,
            representation_snapshot_ref=self.snapshot,
            schema_registry_snapshot_ref="schemas:v1",
            construct_registry_snapshot_ref="constructs:v1",
            admission_policy_ref="admission:v1",
            epistemic_policy_ref="epistemic:v1",
            semantic_constraint_policy_ref="constraints:v1",
            sealed_at=datetime(2026, 9, 19, tzinfo=UTC),
        )
        result_a = ReadinessResult(
            readiness_result_id="rr-a",
            semantic_basis_ref=basis,
            readiness_profile_ref="profile:a",
            readiness_profile_hash="sha256:a",
            result="satisfied",
        )
        result_b = ReadinessResult(
            readiness_result_id="rr-b",
            semantic_basis_ref=basis,
            readiness_profile_ref="profile:b",
            readiness_profile_hash="sha256:b",
            result="unsatisfied",
        )
        self.assertEqual(
            result_a.semantic_basis_ref.semantic_basis_id,
            basis.semantic_basis_id,
        )
        self.assertEqual(
            result_b.semantic_basis_ref.semantic_basis_id,
            basis.semantic_basis_id,
        )
        self.assertNotEqual(
            result_a.readiness_profile_ref,
            result_b.readiness_profile_ref,
        )

    def test_same_representation_can_have_new_basis_under_new_policy(self) -> None:
        common = dict(
            subject_ref=self.subject,
            representation_snapshot_ref=self.snapshot,
            schema_registry_snapshot_ref="schemas:v1",
            construct_registry_snapshot_ref="constructs:v1",
            admission_policy_ref="admission:v1",
            semantic_constraint_policy_ref="constraints:v1",
            sealed_at=datetime(2026, 9, 19, tzinfo=UTC),
        )
        basis_v1 = SemanticBasisRef.seal(
            epistemic_policy_ref="epistemic:v1",
            **common,
        )
        basis_v2 = SemanticBasisRef.seal(
            epistemic_policy_ref="epistemic:v2",
            **common,
        )
        self.assertEqual(basis_v1.subject_ref, basis_v2.subject_ref)
        self.assertEqual(
            basis_v1.representation_snapshot_ref,
            basis_v2.representation_snapshot_ref,
        )
        self.assertNotEqual(
            basis_v1.semantic_basis_id,
            basis_v2.semantic_basis_id,
        )

    def test_at58_admission_does_not_require_semantic_basis_sealing(self) -> None:
        effect = CanonicalEffect(
            effect_id="effect-1",
            effect_kind="evidence_registered",
            affected_semantic_ref="evidence-1",
            source_request_id="request-1",
            canonical_position=self.snapshot,
        )
        admission = AdmissionResult(
            request_id="request-1",
            admission_id="admission-1",
            status=AdmissionStatus.ADMITTED,
            disposition=AdmissionDisposition.EFFECTS_APPLIED,
            canonical_effects=(effect,),
            resulting_representation_state_ref=self.snapshot,
        )
        self.assertEqual(
            admission.resulting_representation_state_ref,
            self.snapshot,
        )

        basis = SemanticBasisRef.seal(
            subject_ref=self.subject,
            representation_snapshot_ref=admission.resulting_representation_state_ref,
            schema_registry_snapshot_ref="schemas:v1",
            construct_registry_snapshot_ref="constructs:v1",
            admission_policy_ref="admission:v1",
            epistemic_policy_ref="epistemic:v1",
            semantic_constraint_policy_ref="constraints:v1",
        )
        self.assertEqual(
            basis.representation_snapshot_ref,
            effect.canonical_position,
        )


if __name__ == "__main__":
    unittest.main()
