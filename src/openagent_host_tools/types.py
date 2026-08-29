"""Public manifest and result types for local capabilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class ToolClassification(StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT = "idempotent"
    MUTATING = "mutating"


@dataclass(frozen=True)
class ToolManifest:
    name: str
    description: str
    input_schema: dict[str, Any]
    classification: ToolClassification = ToolClassification.READ_ONLY
    classification_by_argument: dict[str, dict[str, ToolClassification]] = field(
        default_factory=dict
    )

    def classification_for(self, arguments: dict[str, Any]) -> ToolClassification:
        """Resolve the conservative classification for one concrete invocation.

        MCP annotations classify an entire tool, but a few canonical MCP tools
        multiplex read-only and mutating operations behind an ``action``
        argument. The base classification remains the fail-closed fallback for
        missing, malformed, or newly-added argument values.
        """

        classification = self.classification
        for argument, values in self.classification_by_argument.items():
            value = arguments.get(argument)
            if isinstance(value, str):
                classification = values.get(value, classification)
        return classification

    def to_wire(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
            "classification": self.classification.value,
        }
        if self.classification_by_argument:
            value["classification_by_argument"] = {
                argument: {
                    option: classification.value
                    for option, classification in sorted(options.items())
                }
                for argument, options in sorted(self.classification_by_argument.items())
            }
        return value


@dataclass(frozen=True)
class HostMcpManifest:
    name: str
    version: str
    instructions: str
    tools: tuple[ToolManifest, ...]
    available: bool = True
    unavailable_reason: str | None = None
    platforms: tuple[str, ...] = ()
    os_requirements: tuple[str, ...] = ()
    data_directory: str | None = None
    manifest_version: int = 1

    @property
    def id(self) -> str:
        return self.name

    def to_wire(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "manifest_version": self.manifest_version,
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "instructions": self.instructions,
            "tools": [tool.to_wire() for tool in self.tools],
        }
        if not self.available:
            value["available"] = False
            if self.unavailable_reason:
                value["unavailable_reason"] = self.unavailable_reason
        if self.platforms:
            value["platforms"] = list(self.platforms)
        if self.os_requirements:
            value["os_requirements"] = list(self.os_requirements)
        if self.data_directory:
            value["data_directory"] = self.data_directory
        return value

    def tool(self, name: str) -> ToolManifest | None:
        return next((tool for tool in self.tools if tool.name == name), None)


# Compatibility for early adopters of the package. New integrations should use
# HostMcpManifest; the wire representation is identical.
ServerManifest = HostMcpManifest


@dataclass
class ToolResult:
    content: list[dict[str, Any]] = field(default_factory=list)
    structured_content: Any | None = None
    is_error: bool = False
    meta: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def text(
        cls,
        text: str,
        *,
        structured: Any | None = None,
        is_error: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> "ToolResult":
        return cls(
            content=[{"type": "text", "text": text}],
            structured_content=structured,
            is_error=is_error,
            meta=meta or {},
        )

    @classmethod
    def from_wire(cls, value: dict[str, Any]) -> "ToolResult":
        canonical = {"content", "structuredContent", "isError", "_meta"}
        return cls(
            content=list(value.get("content") or []),
            structured_content=value.get("structuredContent"),
            is_error=bool(value.get("isError", False)),
            meta=dict(value.get("_meta") or {}),
            extra={key: item for key, item in value.items() if key not in canonical},
        )

    def to_wire(self) -> dict[str, Any]:
        # Extension fields are lossless across dispatch, persistence and replay;
        # canonical MCP fields always win if a malformed provider duplicates one.
        value: dict[str, Any] = {
            **self.extra,
            "content": self.content,
            "isError": self.is_error,
        }
        if self.structured_content is not None:
            value["structuredContent"] = self.structured_content
        if self.meta:
            value["_meta"] = self.meta
        return value


class HostError(RuntimeError):
    def __init__(self, code: str, message: str, data: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data or {}

    def to_wire(self) -> dict[str, Any]:
        value: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data:
            value["data"] = self.data
        return value


def tool_error_result(error: HostError) -> ToolResult:
    """Canonical MCP CallToolResult for a tool-level HostError."""

    return ToolResult.text(
        error.message,
        is_error=True,
        meta={"openagent/error": error.to_wire()},
    )


class CapabilityServer(Protocol):
    manifest: HostMcpManifest

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult: ...

    async def close(self) -> None: ...
