from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from seed_contracts import AdmissionResult

from .contracts import (
    AdmissionLookupRequest,
    AdmitEvidenceRequest,
    K2PublicAPI,
    K2PublicError,
    LineageQuery,
    LineageTraceView,
    PublicError,
    RepresentationDeficit,
    RepresentationQuery,
    RepresentationStateView,
    admission_result_from_wire,
    admission_result_to_wire,
    json_round_trip,
)

T = TypeVar("T")


class TransportSimulatedK2Client:
    """JSON round-trip adapter for successful values and public failures."""

    def __init__(self, backend: K2PublicAPI) -> None:
        self._backend = backend

    async def _transport_call(self, operation: Callable[[], Awaitable[T]]) -> T:
        try:
            return await operation()
        except K2PublicError as exc:
            transported = PublicError.from_wire(
                json_round_trip(exc.to_public_error().to_wire())
            )
            raise K2PublicError.from_public_error(transported) from None

    async def admit_evidence(self, request: AdmitEvidenceRequest) -> AdmissionResult:
        inbound = AdmitEvidenceRequest.from_wire(json_round_trip(request.to_wire()))
        result = await self._transport_call(
            lambda: self._backend.admit_evidence(inbound)
        )
        return admission_result_from_wire(
            json_round_trip(admission_result_to_wire(result))
        )

    async def get_admission_result(
        self, request: AdmissionLookupRequest
    ) -> AdmissionResult | None:
        inbound = AdmissionLookupRequest.from_wire(json_round_trip(request.to_wire()))
        result = await self._transport_call(
            lambda: self._backend.get_admission_result(inbound)
        )
        if result is None:
            return None
        return admission_result_from_wire(
            json_round_trip(admission_result_to_wire(result))
        )

    async def read_representation_state(
        self, request: RepresentationQuery
    ) -> RepresentationStateView:
        inbound = RepresentationQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._transport_call(
            lambda: self._backend.read_representation_state(inbound)
        )
        return RepresentationStateView.from_wire(json_round_trip(result.to_wire()))

    async def read_representation_deficits(
        self, request: RepresentationQuery
    ) -> tuple[RepresentationDeficit, ...]:
        inbound = RepresentationQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._transport_call(
            lambda: self._backend.read_representation_deficits(inbound)
        )
        return tuple(
            RepresentationDeficit.from_wire(json_round_trip(item.to_wire()))
            for item in result
        )

    async def trace_lineage(self, request: LineageQuery) -> LineageTraceView:
        inbound = LineageQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._transport_call(
            lambda: self._backend.trace_lineage(inbound)
        )
        return LineageTraceView.from_wire(json_round_trip(result.to_wire()))
