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


class BackpressureExceeded(DigitalTwinError):
    """A real-time subscriber could not keep up safely."""


class NotFound(DigitalTwinError):
    """A requested Digital Twin object does not exist."""

