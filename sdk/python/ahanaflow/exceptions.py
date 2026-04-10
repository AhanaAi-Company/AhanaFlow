"""
ahanaflow.exceptions — AhanaFlow SDK Exceptions
"""


class AhanaFlowError(Exception):
    """Base exception for all AhanaFlow errors."""


class ConnectionError(AhanaFlowError):
    """Raised when a TCP connection to the server cannot be established or is lost."""


class CommandError(AhanaFlowError):
    """Raised when the server returns an error response to a command."""


class TimeoutError(AhanaFlowError):
    """Raised when a command times out waiting for a response."""


class ProtocolError(AhanaFlowError):
    """Raised when the server sends an unexpected or malformed response."""
