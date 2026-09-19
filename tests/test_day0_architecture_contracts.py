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


    def test_at59_non_admitted_result_cannot_carry_resulting_state(self) -> None:
        for status in (AdmissionStatus.REJECTED, AdmissionStatus.DEFERRED):
            with self.subTest(status=status), self.assertRaises(ContractViolation):
                AdmissionResult(
                    request_id="request-1",
                    admission_id=f"admission-{status.value}",
                    status=status,
                    disposition=None,
                    canonical_effects=(),
                    resulting_representation_state_ref=self.snapshot,
                )

    def test_at60_admission_effects_are_coherent_with_result(self) -> None:
        other_subject = SubjectRef("subject-2")
        other_snapshot = RepresentationSnapshotRef(
            subject_ref=other_subject,
            snapshot_id="representation-other",
            canonical_sequence=17,
            integrity_ref="sha256:representation-other",
        )
        earlier_snapshot = RepresentationSnapshotRef(
            subject_ref=self.subject,
            snapshot_id="representation-16",
            canonical_sequence=16,
            integrity_ref="sha256:representation-16",
        )

        bad_request = CanonicalEffect(
            effect_id="effect-request-mismatch",
            effect_kind="evidence_registered",
            affected_semantic_ref="evidence-1",
            source_request_id="request-B",
            canonical_position=self.snapshot,
        )
        with self.assertRaises(ContractViolation):
            AdmissionResult(
                request_id="request-A",
                admission_id="admission-request-mismatch",
                status=AdmissionStatus.ADMITTED,
                disposition=AdmissionDisposition.EFFECTS_APPLIED,
                canonical_effects=(bad_request,),
                resulting_representation_state_ref=self.snapshot,
            )

        cross_subject = CanonicalEffect(
            effect_id="effect-cross-subject",
            effect_kind="evidence_registered",
            affected_semantic_ref="evidence-1",
            source_request_id="request-A",
            canonical_position=other_snapshot,
        )
        with self.assertRaises(ContractViolation):
            AdmissionResult(
                request_id="request-A",
                admission_id="admission-cross-subject",
                status=AdmissionStatus.ADMITTED,
                disposition=AdmissionDisposition.EFFECTS_APPLIED,
                canonical_effects=(cross_subject,),
                resulting_representation_state_ref=self.snapshot,
            )

        intermediate_position = CanonicalEffect(
            effect_id="effect-intermediate-position",
            effect_kind="evidence_registered",
            affected_semantic_ref="evidence-1",
            source_request_id="request-A",
            canonical_position=earlier_snapshot,
        )
        with self.assertRaises(ContractViolation):
            AdmissionResult(
                request_id="request-A",
                admission_id="admission-intermediate-position",
                status=AdmissionStatus.ADMITTED,
                disposition=AdmissionDisposition.EFFECTS_APPLIED,
                canonical_effects=(intermediate_position,),
                resulting_representation_state_ref=self.snapshot,
            )

        effect_a = CanonicalEffect(
            effect_id="effect-a",
            effect_kind="evidence_registered",
            affected_semantic_ref="evidence-1",
            source_request_id="request-A",
            canonical_position=self.snapshot,
        )
        effect_b = CanonicalEffect(
            effect_id="effect-b",
            effect_kind="support_relation_created",
            affected_semantic_ref="support-1",
            source_request_id="request-A",
            canonical_position=self.snapshot,
        )
        result = AdmissionResult(
            request_id="request-A",
            admission_id="admission-coherent",
            status=AdmissionStatus.ADMITTED,
            disposition=AdmissionDisposition.EFFECTS_APPLIED,
            canonical_effects=(effect_a, effect_b),
            resulting_representation_state_ref=self.snapshot,
        )
        self.assertEqual(len(result.canonical_effects), 2)

    def test_at61_semantic_basis_dependency_set_is_order_independent(self) -> None:
        common = dict(
            subject_ref=self.subject,
            representation_snapshot_ref=self.snapshot,
            schema_registry_snapshot_ref="schemas:v1",
            construct_registry_snapshot_ref="constructs:v1",
            admission_policy_ref="admission:v1",
            epistemic_policy_ref="epistemic:v1",
            semantic_constraint_policy_ref="constraints:v1",
            sealed_at=datetime(2026, 9, 19, tzinfo=UTC),
        )
        basis_ab = SemanticBasisRef.seal(
            other_interpretation_critical_refs=("dependency:A", "dependency:B"),
            **common,
        )
        basis_ba = SemanticBasisRef.seal(
            other_interpretation_critical_refs=("dependency:B", "dependency:A"),
            **common,
        )
        self.assertEqual(basis_ab.semantic_basis_id, basis_ba.semantic_basis_id)
        self.assertEqual(
            basis_ab.other_interpretation_critical_refs,
            ("dependency:A", "dependency:B"),
        )
        self.assertEqual(
            basis_ba.other_interpretation_critical_refs,
            ("dependency:A", "dependency:B"),
        )

        with self.assertRaises(ContractViolation):
            SemanticBasisRef.seal(
                other_interpretation_critical_refs=("dependency:A", "dependency:A"),
                **common,
            )


if __name__ == "__main__":
    unittest.main()
