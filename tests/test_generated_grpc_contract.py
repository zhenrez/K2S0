from __future__ import annotations

import hashlib
import unittest
from datetime import UTC, datetime

from argo_dt.adapters.grpc import ProtobufCodec

try:
    from argo.dt.v1 import twin_pb2, twin_pb2_grpc
    from google.protobuf.struct_pb2 import Struct
    from google.protobuf.timestamp_pb2 import Timestamp
except ModuleNotFoundError:
    twin_pb2 = None
    twin_pb2_grpc = None
    Struct = None
    Timestamp = None


@unittest.skipUnless(twin_pb2 is not None, "generated protobuf modules not on PYTHONPATH")
class GeneratedGrpcContractTests(unittest.TestCase):
    def test_generated_telemetry_and_state_messages_map_to_adapter_types(self) -> None:
        assert twin_pb2 is not None
        assert twin_pb2_grpc is not None
        assert Struct is not None
        assert Timestamp is not None
        valid_from = Timestamp()
        valid_from.FromDatetime(datetime(2026, 1, 1, tzinfo=UTC))
        rights = Struct()
        rights.update({"processing": ["modeling"]})
        payload = b'{"temperature":21}'
        record = twin_pb2.TelemetryRecord(
            source_record_id="sensor-1",
            valid_from=valid_from,
            media_type="application/json",
            payload=payload,
            content_hash="sha256:" + hashlib.sha256(payload).hexdigest(),
            independence_group="sensor-a",
            sensitivity="internal",
            rights=rights,
        )
        batch = twin_pb2.TelemetryBatch(
            twin_id="twin-1",
            subject_id="human-1",
            source="sensor",
            connector_version="1",
            idempotency_key="generated-contract",
            expected_sequence=0,
            records=[record],
        )
        codec = ProtobufCodec.from_generated(twin_pb2)
        decoded, sizes, total = codec.telemetry_batch(batch)
        self.assertEqual("sensor-1", decoded.records[0].source_record_id)
        self.assertEqual((record.ByteSize(),), sizes)
        self.assertEqual(batch.ByteSize(), total)

        subscribe = twin_pb2.StateStreamRequest(
            subscribe=twin_pb2.StateSubscription(
                twin_id="twin-1",
                max_in_flight=16,
            )
        )
        control = codec.state_control(subscribe)
        self.assertEqual(16, control.max_in_flight)


if __name__ == "__main__":
    unittest.main()
