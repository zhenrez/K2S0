from __future__ import annotations

import json
import unittest

from seed_contracts import RepresentationSnapshotRef, SubjectRef
from seed_contracts.envelope import (
    ExtensionBlock,
    SeedEnvelope,
    SeedSection,
    compile_seed_envelope,
    read_seed_envelope,
)


class SeedEnvelopeTests(unittest.TestCase):
    def fixture(self) -> SeedEnvelope:
        subject = SubjectRef("human-1")
        snapshot = RepresentationSnapshotRef(
            subject_ref=subject,
            snapshot_id="representation-17",
            canonical_sequence=17,
            integrity_ref="sha256:representation-17",
        )
        return SeedEnvelope(
            subject_ref=subject,
            representation_snapshot_ref=snapshot,
            source_sequence=17,
            compiler_ref="k2s0-seed-compiler:test-v1",
            sections=(
                SeedSection(
                    name="representation",
                    payload={
                        "items": [
                            {"ref": "item-1", "epistemic_state": "contested"},
                            {"ref": "item-2", "epistemic_state": "withheld"},
                            {"ref": "item-3", "epistemic_state": "not_assessed"},
                        ],
                        "normative": {"policy_ref": "policy:v1"},
                        "descriptive": {"summary": "fixture"},
                    },
                ),
            ),
            acquisition_info=(
                {
                    "source": "chat-history",
                    "source_record_id": "message-1",
                    "connector_version": "fixture-v1",
                },
            ),
            extension_blocks=(
                ExtensionBlock(
                    type_uri="urn:example:unknown-extension",
                    version="7",
                    payload={"opaque": {"keep": ["exactly", 1, True]}},
                ),
            ),
            loss_manifest=(),
        )

    def test_seed_envelope_compile_read_round_trip_is_deterministic(self) -> None:
        envelope = self.fixture()
        first = compile_seed_envelope(envelope)
        second = compile_seed_envelope(envelope)
        self.assertEqual(first, second)

        reopened = read_seed_envelope(first)
        self.assertEqual(reopened, envelope)
        self.assertEqual(compile_seed_envelope(reopened), first)

        wire = json.loads(first)
        self.assertEqual(wire["schema_version"], "k2s0.seed-envelope/v1")
        self.assertEqual(wire["source_sequence"], 17)
        self.assertTrue(wire["integrity_ref"].startswith("seed-envelope:sha256:"))

    def test_unknown_extension_and_acquisition_info_round_trip_without_interpretation(self) -> None:
        payload = compile_seed_envelope(self.fixture())
        reopened = read_seed_envelope(payload)

        self.assertEqual(reopened.extension_blocks[0].type_uri, "urn:example:unknown-extension")
        self.assertEqual(
            reopened.extension_blocks[0].payload,
            {"opaque": {"keep": ["exactly", 1, True]}},
        )
        self.assertEqual(
            reopened.acquisition_info[0]["source_record_id"],
            "message-1",
        )

    def test_reader_rejects_integrity_tampering_and_unknown_schema_version(self) -> None:
        wire = json.loads(compile_seed_envelope(self.fixture()))

        tampered = dict(wire)
        tampered["source_sequence"] = 18
        with self.assertRaisesRegex(ValueError, "integrity"):
            read_seed_envelope(json.dumps(tampered, sort_keys=True).encode())

        unsupported = dict(wire)
        unsupported["schema_version"] = "k2s0.seed-envelope/v999"
        with self.assertRaisesRegex(ValueError, "schema_version"):
            read_seed_envelope(json.dumps(unsupported, sort_keys=True).encode())


if __name__ == "__main__":
    unittest.main()
