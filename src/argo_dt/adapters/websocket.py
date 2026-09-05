"""Dependency-free ASGI WebSocket adapter for state-change streams."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import unquote

from ..errors import (
    AuthorizationDenied,
    BackpressureExceeded,
    IntegrityError,
    MessageTooLarge,
    ProtocolViolation,
)
from ..service import DigitalTwinService
from ..types import parse_time
from .auth import Authenticator, TransportCredentials

AsgiReceive = Callable[[], Awaitable[dict[str, Any]]]
AsgiSend = Callable[[dict[str, Any]], Awaitable[None]]
_SUBPROTOCOL = "argo.dt.state-stream.v1"
_TWIN_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


@dataclass(frozen=True, slots=True)
class SubscribeControl:
    resume_token: str | None
    event_types: tuple[str, ...]
    include_projection_events: bool
    max_in_flight: int | None


@dataclass(frozen=True, slots=True)
class AcknowledgeControl:
    resume_token: str


@dataclass(frozen=True, slots=True)
class HeartbeatControl:
    sent_at: datetime


@dataclass(frozen=True, slots=True)
class ClientCloseControl:
    code: int
    reason: str


ControlFrame = (
    SubscribeControl | AcknowledgeControl | HeartbeatControl | ClientCloseControl
)


class StateWebSocketCodec:
    """Strict JSON codec with duplicate-key, shape, and byte-limit checks."""

    def __init__(self, *, max_frame_bytes: int = 16 * 1024) -> None:
        if max_frame_bytes < 256:
            raise ValueError("max_frame_bytes must be at least 256")
        self.max_frame_bytes = max_frame_bytes

    def decode_asgi_message(self, message: Mapping[str, Any]) -> ControlFrame:
        if message.get("type") != "websocket.receive":
            raise ProtocolViolation("expected a WebSocket data frame")
        text = message.get("text")
        raw = message.get("bytes")
        if (text is None) == (raw is None):
            raise ProtocolViolation("WebSocket frame must contain text or bytes")
        payload = text if text is not None else raw
        assert isinstance(payload, (str, bytes))
        return self.decode(payload)

    def decode(self, payload: str | bytes) -> ControlFrame:
        encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
        if len(encoded) > self.max_frame_bytes:
            raise MessageTooLarge("WebSocket control frame exceeds its byte limit")
        try:
            text = encoded.decode("utf-8")
            value = json.loads(text, object_pairs_hook=self._unique_object)
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise ProtocolViolation("WebSocket control frame is invalid JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolViolation("WebSocket control frame must be an object")
        frame_type = value.get("type")
        if frame_type == "subscribe":
            return self._subscribe(value)
        if frame_type == "acknowledge":
            self._require_keys(value, {"type", "resume_token"})
            return AcknowledgeControl(self._token(value.get("resume_token")))
        if frame_type == "heartbeat":
            self._require_keys(value, {"type", "sent_at"})
            sent_at = value.get("sent_at")
            if not isinstance(sent_at, str):
                raise ProtocolViolation("heartbeat sent_at must be a timestamp")
            try:
                parsed = parse_time(sent_at)
            except Exception as exc:
                raise ProtocolViolation("heartbeat sent_at must be a timestamp") from exc
            if parsed > datetime.now(UTC) + timedelta(minutes=5):
                raise ProtocolViolation("heartbeat sent_at is too far in the future")
            return HeartbeatControl(parsed)
        if frame_type == "close":
            self._require_keys(value, {"type", "code", "reason"})
            code = value.get("code")
            reason = value.get("reason")
            if isinstance(code, bool) or not isinstance(code, int):
                raise ProtocolViolation("close code must be an integer")
            if not 1000 <= code <= 4999:
                raise ProtocolViolation("close code is outside the WebSocket range")
            if not isinstance(reason, str) or len(reason) > 256:
                raise ProtocolViolation("close reason is invalid")
            return ClientCloseControl(code, reason)
        raise ProtocolViolation("unknown WebSocket control frame type")

    @staticmethod
    def encode(frame: Mapping[str, object]) -> str:
        return json.dumps(
            frame,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @staticmethod
    def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value

    @staticmethod
    def _require_keys(value: Mapping[str, Any], allowed: set[str]) -> None:
        if set(value) != allowed:
            raise ProtocolViolation("WebSocket control frame shape is invalid")

    def _subscribe(self, value: Mapping[str, Any]) -> SubscribeControl:
        allowed = {
            "type",
            "resume_token",
            "event_types",
            "include_projection_events",
            "max_in_flight",
        }
        if not set(value).issubset(allowed):
            raise ProtocolViolation("subscribe frame contains unknown fields")
        resume_token = (
            self._token(value["resume_token"])
            if "resume_token" in value
            else None
        )
        raw_types = value.get("event_types", [])
        if not isinstance(raw_types, list) or len(raw_types) > 64:
            raise ProtocolViolation("event_types must be an array of at most 64 values")
        if any(
            not isinstance(item, str) or not 1 <= len(item) <= 128
            for item in raw_types
        ):
            raise ProtocolViolation("event_types contains an invalid value")
        if len(raw_types) != len(set(raw_types)):
            raise ProtocolViolation("event_types values must be unique")
        include_projection = value.get("include_projection_events", False)
        if not isinstance(include_projection, bool):
            raise ProtocolViolation("include_projection_events must be boolean")
        max_in_flight = value.get("max_in_flight")
        if "max_in_flight" in value and max_in_flight is None:
            raise ProtocolViolation("max_in_flight must be an integer")
        if max_in_flight is not None and (
            isinstance(max_in_flight, bool) or not isinstance(max_in_flight, int)
        ):
            raise ProtocolViolation("max_in_flight must be an integer")
        if max_in_flight is not None and max_in_flight < 1:
            raise ProtocolViolation("max_in_flight must be positive")
        return SubscribeControl(
            resume_token=resume_token,
            event_types=tuple(raw_types),
            include_projection_events=include_projection,
            max_in_flight=max_in_flight,
        )

    @staticmethod
    def _token(value: object) -> str:
        if not isinstance(value, str) or not 32 <= len(value) <= 2048:
            raise ProtocolViolation("resume token is invalid")
        return value


class StateWebSocketApp:
    """ASGI 3 application implementing the documented state-stream protocol."""

    def __init__(
        self,
        *,
        service: DigitalTwinService,
        authenticator: Authenticator,
        codec: StateWebSocketCodec | None = None,
        subscribe_timeout_seconds: float = 10.0,
        client_idle_timeout_seconds: float = 90.0,
    ) -> None:
        if subscribe_timeout_seconds <= 0 or client_idle_timeout_seconds <= 0:
            raise ValueError("WebSocket timeouts must be positive")
        self._service = service
        self._authenticator = authenticator
        self._codec = codec or StateWebSocketCodec()
        self._subscribe_timeout = subscribe_timeout_seconds
        self._client_idle_timeout = client_idle_timeout_seconds

    async def __call__(
        self,
        scope: Mapping[str, Any],
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        if scope.get("type") != "websocket":
            raise ValueError("StateWebSocketApp accepts only ASGI WebSocket scopes")
        try:
            connect = await asyncio.wait_for(receive(), timeout=self._subscribe_timeout)
        except asyncio.TimeoutError:
            await send(
                {"type": "websocket.close", "code": 4410, "reason": "connect timeout"}
            )
            return
        if connect.get("type") != "websocket.connect":
            await send(
                {
                    "type": "websocket.close",
                    "code": 4400,
                    "reason": "invalid connection lifecycle",
                }
            )
            return
        twin_id = self._twin_id(scope.get("path"))
        if twin_id is None:
            await send({"type": "websocket.close", "code": 4404, "reason": "not found"})
            return
        if _SUBPROTOCOL not in scope.get("subprotocols", ()):
            await send(
                {
                    "type": "websocket.close",
                    "code": 4406,
                    "reason": "subprotocol required",
                }
            )
            return
        try:
            principal = await self._authenticator.authenticate(
                TransportCredentials(
                    transport="websocket",
                    metadata=self._headers(scope.get("headers", ())),
                    peer=self._peer(scope.get("client")),
                )
            )
            principal.require_scope("dt.stream")
        except AuthorizationDenied:
            await send(
                {"type": "websocket.close", "code": 4403, "reason": "access denied"}
            )
            return
        await send(
            {
                "type": "websocket.accept",
                "subprotocol": _SUBPROTOCOL,
                "headers": [],
            }
        )
        session = None
        try:
            first = await asyncio.wait_for(receive(), timeout=self._subscribe_timeout)
            if first.get("type") == "websocket.disconnect":
                return
            control = self._codec.decode_asgi_message(first)
            if not isinstance(control, SubscribeControl):
                raise ProtocolViolation("first WebSocket frame must subscribe")
            session = await self._service.open_state_stream(
                twin_id,
                actor=principal.actor,
                resume_token=control.resume_token,
                event_types=control.event_types,
                include_projection_events=control.include_projection_events,
                max_in_flight=control.max_in_flight,
            )
            await self._serve(session, receive, send)
        except asyncio.TimeoutError:
            await self._fail(session, send, code=4410, reason="stream timeout")
        except AuthorizationDenied:
            await self._fail(session, send, code=4403, reason="access denied")
        except MessageTooLarge:
            await self._fail(session, send, code=4413, reason="frame too large")
        except ProtocolViolation:
            await self._fail(session, send, code=4400, reason="invalid stream frame")
        except BackpressureExceeded:
            await self._fail(session, send, code=4408, reason="acknowledgement required")
        except IntegrityError:
            await self._fail(session, send, code=4411, reason="stream integrity failure")
        except Exception:
            await self._fail(session, send, code=1011, reason="internal stream error")
            raise
        finally:
            if session is not None:
                await session.close()

    async def _serve(
        self,
        session: Any,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        receiver = asyncio.create_task(self._receive_controls(session, receive))
        next_event = asyncio.create_task(session.__anext__())
        heartbeat_seconds = self._service.sync_limits.heartbeat_seconds
        try:
            while True:
                done, _ = await asyncio.wait(
                    {receiver, next_event},
                    timeout=heartbeat_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receiver in done:
                    control = receiver.result()
                    if isinstance(control, ClientCloseControl):
                        frame = await session.close(
                            code=control.code,
                            reason=control.reason,
                        )
                        await self._send_json(send, frame.to_dict())
                        await send(
                            {
                                "type": "websocket.close",
                                "code": control.code,
                                "reason": control.reason,
                            }
                        )
                    return
                if next_event in done:
                    frame = next_event.result()
                    await self._send_json(send, frame.to_dict())
                    next_event = asyncio.create_task(session.__anext__())
                else:
                    await self._send_json(send, session.heartbeat().to_dict())
        finally:
            for task in (receiver, next_event):
                if not task.done():
                    task.cancel()
            await asyncio.gather(receiver, next_event, return_exceptions=True)

    async def _receive_controls(
        self,
        session: Any,
        receive: AsgiReceive,
    ) -> ClientCloseControl | None:
        while True:
            message = await asyncio.wait_for(
                receive(),
                timeout=self._client_idle_timeout,
            )
            if message.get("type") == "websocket.disconnect":
                return None
            control = self._codec.decode_asgi_message(message)
            if isinstance(control, SubscribeControl):
                raise ProtocolViolation("a stream can subscribe only once")
            if isinstance(control, AcknowledgeControl):
                session.acknowledge(control.resume_token)
            elif isinstance(control, ClientCloseControl):
                return control

    async def _fail(
        self,
        session: Any | None,
        send: AsgiSend,
        *,
        code: int,
        reason: str,
    ) -> None:
        if session is not None:
            frame = await session.close(code=code, reason=reason)
            await self._send_json(send, frame.to_dict())
        await send({"type": "websocket.close", "code": code, "reason": reason})

    async def _send_json(
        self,
        send: AsgiSend,
        frame: Mapping[str, object],
    ) -> None:
        await send({"type": "websocket.send", "text": self._codec.encode(frame)})

    @staticmethod
    def _twin_id(path: object) -> str | None:
        if not isinstance(path, str):
            return None
        prefix = "/v1/twins/"
        suffix = "/events"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None
        encoded = path[len(prefix) : -len(suffix)]
        if not encoded or "/" in encoded:
            return None
        decoded = unquote(encoded)
        return decoded if _TWIN_ID.fullmatch(decoded) else None

    @staticmethod
    def _headers(raw_headers: object) -> dict[str, str]:
        if not isinstance(raw_headers, (list, tuple)):
            raise AuthorizationDenied("transport headers are malformed")
        headers: dict[str, str] = {}
        try:
            for raw_name, raw_value in raw_headers:
                name = bytes(raw_name).decode("ascii").lower()
                value = bytes(raw_value).decode("latin-1")
                if name in headers:
                    raise AuthorizationDenied("duplicate transport header")
                headers[name] = value
        except (TypeError, ValueError, UnicodeError) as exc:
            raise AuthorizationDenied("transport headers are malformed") from exc
        return headers

    @staticmethod
    def _peer(client: object) -> str | None:
        if isinstance(client, (tuple, list)) and client:
            return str(client[0])
        return None
