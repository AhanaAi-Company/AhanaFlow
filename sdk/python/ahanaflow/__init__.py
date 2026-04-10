"""
ahanaflow — Python SDK
Compressed State & Event Engine client by AhanaAI.

Usage:
    # Remote (TCP server):
    from ahanaflow import AhanaFlowClient
    client = AhanaFlowClient("localhost", 9633)
    client.set("key", "value")

    # Async remote:
    from ahanaflow import AsyncAhanaFlowClient
    client = AsyncAhanaFlowClient("localhost", 9633)
    await client.connect()
    await client.set("key", "value")

CLI:
    ahanaflow ping
    ahanaflow stats
    python -m ahanaflow --help
"""

from ahanaflow.client import AhanaFlowClient
from ahanaflow.async_client import AsyncAhanaFlowClient

__all__ = ["AhanaFlowClient", "AsyncAhanaFlowClient"]
__version__ = "1.0.0"
