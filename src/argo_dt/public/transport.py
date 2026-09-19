from __future__ import annotations

from .contracts import (
    AdmissionLookupRequest,
    AdmitEvidenceRequest,
    K2PublicAPI,
    LineageQuery,
    LineageTraceView,
    RepresentationDeficit,
    RepresentationQuery,
    RepresentationStateView,
    admission_result_from_wire,
    admission_result_to_wire,
    json_round_trip,
)
from seed_contracts import AdmissionResult


class TransportSimulatedK2Client:
    """JSON round-trip adapter proving public semantics do not require in-process objects."""

    def __init__(self, backend: K2PublicAPI) -> None:
        self._backend = backend

    async def admit_evidence(self, request: AdmitEvidenceRequest) -> AdmissionResult:
        inbound = AdmitEvidenceRequest.from_wire(json_round_trip(request.to_wire()))
        result = await self._backend.admit_evidence(inbound)
        return admission_result_from_wire(
            json_round_trip(admission_result_to_wire(result))
        )

    async def get_admission_result(
        self, request: AdmissionLookupRequest
    ) -> AdmissionResult | None:
        inbound = AdmissionLookupRequest.from_wire(json_round_trip(request.to_wire()))
        result = await self._backend.get_admission_result(inbound)
        if result is None:
            return None
        return admission_result_from_wire(
            json_round_trip(admission_result_to_wire(result))
        )

    async def read_representation_state(
        self, request: RepresentationQuery
    ) -> RepresentationStateView:
        inbound = RepresentationQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._backend.read_representation_state(inbound)
        return RepresentationStateView.from_wire(json_round_trip(result.to_wire()))

    async def read_representation_deficits(
        self, request: RepresentationQuery
    ) -> tuple[RepresentationDeficit, ...]:
        inbound = RepresentationQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._backend.read_representation_deficits(inbound)
        return tuple(
            RepresentationDeficit.from_wire(json_round_trip(item.to_wire()))
            for item in result
        )

    async def trace_lineage(self, request: LineageQuery) -> LineageTraceView:
        inbound = LineageQuery.from_wire(json_round_trip(request.to_wire()))
        result = await self._backend.trace_lineage(inbound)
        return LineageTraceView.from_wire(json_round_trip(result.to_wire()))
