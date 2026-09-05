"""Authentication boundary shared by concrete transports.

Transport adapters extract credentials but never validate or invent identity
claims themselves. A deployment-provided authenticator verifies OIDC/mTLS and
returns the already-bound ActorContext plus explicit OAuth scopes.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from ..errors import AuthorizationDenied
from ..types import ActorContext


@dataclass(frozen=True, slots=True)
class TransportCredentials:
    transport: str
    metadata: Mapping[str, str] = field(repr=False)
    peer: str | None = None

    def __post_init__(self) -> None:
        if self.transport not in {"grpc", "websocket"}:
            raise ValueError("unsupported authentication transport")
        if len(self.metadata) > 64:
            raise AuthorizationDenied("transport credentials are malformed")
        normalized: dict[str, str] = {}
        for name, value in self.metadata.items():
            key = str(name).strip().lower()
            rendered = str(value)
            if (
                not key
                or len(key) > 128
                or len(rendered) > 8192
                or key in normalized
            ):
                raise AuthorizationDenied("transport credentials are malformed")
            normalized[key] = rendered
        object.__setattr__(self, "metadata", MappingProxyType(normalized))

    @property
    def authorization(self) -> str | None:
        return self.metadata.get("authorization")


@dataclass(frozen=True, slots=True)
class AuthenticatedPrincipal:
    actor: ActorContext
    scopes: frozenset[str]

    def __post_init__(self) -> None:
        if any(not scope or len(scope) > 128 for scope in self.scopes):
            raise AuthorizationDenied("authenticated scope set is invalid")

    def require_scope(self, scope: str) -> None:
        if scope not in self.scopes:
            raise AuthorizationDenied("required transport scope is absent")


class Authenticator(Protocol):
    """Deployment adapter for OIDC/mTLS verification and claim binding."""

    async def authenticate(
        self,
        credentials: TransportCredentials,
    ) -> AuthenticatedPrincipal: ...
