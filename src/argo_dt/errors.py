"""Typed failures used at service and transport boundaries."""


class DigitalTwinError(Exception):
    """Base class for expected Digital Twin failures."""


class InvariantViolation(DigitalTwinError):
    """A constitutional or domain invariant was violated."""


class ConcurrencyConflict(DigitalTwinError):
    """The caller wrote against a stale stream sequence."""


class IntegrityError(DigitalTwinError):
    """The event hash chain is invalid."""


class PolicyDenied(DigitalTwinError):
    """A policy decision denied disclosure or action."""


class AuthorizationDenied(DigitalTwinError):
    """An authenticated actor does not own the requested event transition."""


class BackpressureExceeded(DigitalTwinError):
    """A real-time subscriber could not keep up safely."""


class ProtocolViolation(DigitalTwinError):
    """A synchronization peer sent an invalid frame or acknowledgement."""


class ResumeCursorRejected(ProtocolViolation):
    """A resume cursor is malformed, forged, expired, or bound elsewhere."""


class MessageTooLarge(ProtocolViolation):
    """A transport record or batch exceeded its configured byte budget."""


class NotFound(DigitalTwinError):
    """A requested Digital Twin object does not exist."""
