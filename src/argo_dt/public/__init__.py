"""Stable public K2 bridge.

R2D2 and external transports may depend on this package. They must not depend on
private K2 aggregate, event-store, SQLite, compiler, or event-envelope objects.
"""

from seed_contracts import AdmissionResult

from .contracts import (
    AdmissionLookupRequest,
    AdmitEvidenceRequest,
    AuthorityContext,
    K2PublicAPI,
    K2PublicError,
    LineageNodeRef,
    LineageQuery,
    LineageTraceView,
    PublicError,
    PublicErrorCode,
    PublicLineageNodeKind,
    RepresentationDeficit,
    RepresentationQuery,
    RepresentationStateView,
)
from .facade import InProcessK2Facade
from .transport import TransportSimulatedK2Client

__all__ = [
    "AdmissionLookupRequest",
    "AdmissionResult",
    "AdmitEvidenceRequest",
    "AuthorityContext",
    "InProcessK2Facade",
    "K2PublicAPI",
    "K2PublicError",
    "LineageNodeRef",
    "LineageQuery",
    "LineageTraceView",
    "PublicError",
    "PublicErrorCode",
    "PublicLineageNodeKind",
    "RepresentationDeficit",
    "RepresentationQuery",
    "RepresentationStateView",
    "TransportSimulatedK2Client",
]
