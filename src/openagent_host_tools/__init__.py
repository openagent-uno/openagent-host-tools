"""Local host capabilities for OpenAgent clients."""

from ._version import __version__
from .bridge import CLIENT_CAPABILITIES_PROTOCOL, CapabilityBridge
from .consent import CONSENT_VERSION, ConsentState, ConsentStore
from .host import CapabilityHost
from .local_broker import LocalCapabilityClient
from .manifests import build_manifest_lock, manifest_sha256
from .sources import sidecar_source
from .paths import HostPaths
from .types import (
    HostError,
    HostMcpManifest,
    ServerManifest,
    ToolClassification,
    ToolManifest,
    ToolResult,
)

__all__ = [
    "CLIENT_CAPABILITIES_PROTOCOL",
    "CONSENT_VERSION",
    "CapabilityBridge",
    "CapabilityHost",
    "ConsentState",
    "ConsentStore",
    "HostError",
    "HostMcpManifest",
    "HostPaths",
    "LocalCapabilityClient",
    "build_manifest_lock",
    "sidecar_source",
    "manifest_sha256",
    "ServerManifest",
    "ToolClassification",
    "ToolManifest",
    "ToolResult",
    "__version__",
]
