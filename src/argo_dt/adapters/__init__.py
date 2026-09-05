"""Optional network and observability adapters around the dependency-light kernel."""

from .auth import AuthenticatedPrincipal, Authenticator, TransportCredentials
from .grpc import DigitalTwinGrpcAdapter, ProtobufCodec, register_grpc_service
from .nats import JetStreamConsumer, JetStreamPublisher, NatsConsumerStats
from .observability import OpenTelemetrySyncExporter
from .telemetry import TelemetryBatchProcessor, TelemetryInputBatch, TelemetryInputRecord
from .websocket import StateWebSocketApp, StateWebSocketCodec

__all__ = [
    "AuthenticatedPrincipal",
    "Authenticator",
    "DigitalTwinGrpcAdapter",
    "JetStreamConsumer",
    "JetStreamPublisher",
    "NatsConsumerStats",
    "OpenTelemetrySyncExporter",
    "ProtobufCodec",
    "StateWebSocketApp",
    "StateWebSocketCodec",
    "TelemetryBatchProcessor",
    "TelemetryInputBatch",
    "TelemetryInputRecord",
    "TransportCredentials",
    "register_grpc_service",
]
