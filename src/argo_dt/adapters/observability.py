"""Low-cardinality OpenTelemetry export for synchronization counters."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from typing import Protocol

from ..sync import SyncMetrics


class Counter(Protocol):
    def add(
        self,
        amount: int,
        attributes: Mapping[str, object] | None = None,
    ) -> None: ...


class Meter(Protocol):
    def create_counter(
        self,
        name: str,
        *,
        unit: str = "1",
        description: str = "",
    ) -> Counter: ...


_COUNTER_NAMES = {
    "replayed_events": "argo.dt.sync.events.replayed",
    "live_events": "argo.dt.sync.events.live",
    "duplicate_events": "argo.dt.sync.events.duplicate",
    "emitted_frames": "argo.dt.sync.frames.emitted",
    "acknowledgements": "argo.dt.sync.acknowledgements",
    "heartbeats": "argo.dt.sync.heartbeats",
    "closed_streams": "argo.dt.sync.streams.closed",
    "integrity_failures": "argo.dt.sync.failures.integrity",
    "backpressure_closes": "argo.dt.sync.failures.backpressure",
}
_ALLOWED_ATTRIBUTES = {
    "service.name",
    "service.version",
    "deployment.environment.name",
    "network.transport",
}


class OpenTelemetrySyncExporter:
    """Bridge cumulative in-process counters into OTel monotonic counters.

    Only a fixed low-cardinality resource-like attribute set is permitted.
    Per-twin, subject, event, cursor, payload, and hash attributes are rejected.
    """

    def __init__(
        self,
        *,
        metrics: SyncMetrics,
        meter: Meter,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        supplied = dict(attributes or {})
        if not set(supplied).issubset(_ALLOWED_ATTRIBUTES):
            raise ValueError("OpenTelemetry attributes exceed the fixed allow-list")
        if any(
            not isinstance(value, str) or not value or len(value) > 128
            for value in supplied.values()
        ):
            raise ValueError("OpenTelemetry attributes must be short non-empty strings")
        self._metrics = metrics
        self._attributes = supplied
        self._counters = {
            key: meter.create_counter(
                name,
                unit="1",
                description=f"Cumulative ARGO DT synchronization {key.replace('_', ' ')}",
            )
            for key, name in _COUNTER_NAMES.items()
        }
        self._previous = {key: 0 for key in _COUNTER_NAMES}
        self._lock = threading.Lock()

    @classmethod
    def from_global_provider(
        cls,
        *,
        metrics: SyncMetrics,
        attributes: Mapping[str, object] | None = None,
        instrumentation_version: str = "0.5.0",
    ) -> OpenTelemetrySyncExporter:
        """Create against the installed OTel API without a hard dependency."""

        try:
            from opentelemetry import metrics as otel_metrics
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError(
                "install the 'otel' extra to use OpenTelemetry export"
            ) from exc
        meter = otel_metrics.get_meter(
            "argo_dt.synchronization",
            instrumentation_version,
        )
        return cls(metrics=metrics, meter=meter, attributes=attributes)

    def export(self) -> dict[str, int]:
        """Record new deltas and return the redaction-safe exported delta set."""

        with self._lock:
            current = self._metrics.snapshot()
            deltas: dict[str, int] = {}
            for key, counter in self._counters.items():
                value = current[key]
                previous = self._previous[key]
                delta = value - previous if value >= previous else value
                if delta:
                    counter.add(delta, attributes=self._attributes)
                deltas[key] = delta
            self._previous = current
            return deltas

    async def run(self, *, interval_seconds: float, stop: asyncio.Event) -> None:
        """Periodically export until the deployment lifecycle signals stop."""

        if interval_seconds <= 0:
            raise ValueError("OpenTelemetry export interval must be positive")
        while not stop.is_set():
            self.export()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
