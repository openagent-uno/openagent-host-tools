from __future__ import annotations

import json
from typing import Any

from ..types import HostError, ToolResult


def json_result(value: Any, *, meta: dict[str, Any] | None = None) -> ToolResult:
    return ToolResult.text(
        json.dumps(value, indent=2, ensure_ascii=False, default=str),
        structured=value,
        meta=meta,
    )


def require_string(args: dict[str, Any], name: str, *, allow_empty: bool = False) -> str:
    value = args.get(name)
    if not isinstance(value, str) or (not allow_empty and not value):
        raise HostError("invalid_arguments", f"{name} must be a string")
    return value


def integer_arg(
    args: dict[str, Any], name: str, default: int, *, minimum: int, maximum: int
) -> int:
    value = args.get(name, default)
    if isinstance(value, bool):
        raise HostError("invalid_arguments", f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise HostError("invalid_arguments", f"{name} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise HostError(
            "invalid_arguments", f"{name} must be between {minimum} and {maximum}"
        )
    return parsed
