"""Shared NDJSON control request dispatcher for stdio and the local broker."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from .consent import CONSENT_VERSION
from .host import CapabilityHost
from .types import HostError

HOST_PROTOCOL = "openagent-host-tools/1"


@dataclass(frozen=True)
class ControlReply:
    frame: dict[str, Any]
    close: bool = False


async def dispatch_control(host: CapabilityHost, request: dict[str, Any]) -> ControlReply:
    request_id = request.get("id")
    request_type = request.get("type")
    try:
        if request_id is None or request_id == "":
            raise HostError("invalid_request", "id is required")
        if request_type == "initialize":
            protocol = request.get("protocol", HOST_PROTOCOL)
            if protocol != HOST_PROTOCOL:
                raise HostError("unsupported_protocol", f"unsupported protocol {protocol!r}")
            status = await host.status()
            result = {
                "protocol": HOST_PROTOCOL,
                "consent": status["consent"],
                "config_path": status["config_path"],
            }
        elif request_type == "catalog":
            result = {"servers": await host.catalog()}
        elif request_type == "status":
            result = await host.status()
        elif request_type == "set_consent":
            if not isinstance(request.get("enabled"), bool):
                raise HostError("invalid_request", "enabled must be boolean")
            state = await host.set_consent(
                request["enabled"],
                version=int(request.get("consent_version", CONSENT_VERSION)),
            )
            result = {"consent": state.to_wire(), "status": await host.status()}
        elif request_type == "call":
            call_id = str(request.get("call_id") or request_id)
            result_obj = await host.call(
                str(request.get("server") or ""),
                str(request.get("tool") or ""),
                dict(request.get("args") or {}),
                principal=request.get("principal") or "local-control-client",
                call_id=call_id,
                idempotency_key=(
                    str(request["idempotency_key"])
                    if request.get("idempotency_key") is not None
                    else None
                ),
                deadline_ms=request.get("deadline_ms"),
                arguments_sha256=(
                    str(request["arguments_sha256"])
                    if request.get("arguments_sha256") is not None
                    else None
                ),
            )
            result = result_obj.to_wire()
        elif request_type == "cancel":
            call_id = str(request.get("call_id") or "")
            result = {"cancelled": await host.cancel(call_id)}
        elif request_type == "release_principal":
            principal = request.get("principal") or ""
            if not principal:
                raise HostError("invalid_request", "principal is required")
            await host.release_principal(principal)
            result = {"released": True}
        elif request_type == "ack_event":
            principal = request.get("principal") or ""
            shell_id = str(request.get("shell_id") or "")
            if not principal or not shell_id:
                raise HostError(
                    "invalid_request", "principal and shell_id are required"
                )
            result = {
                "acknowledged": await host.ack_event(principal, shell_id)
            }
        elif request_type == "shutdown":
            # Shutdown closes this local control connection, not the shared
            # single-instance broker used by other Desktop/CLI processes.
            result = {"shutting_down": True}
            return _success(request_id, result, close=True)
        else:
            raise HostError("unknown_request", f"unknown request type {request_type!r}")
    except asyncio.CancelledError:
        return _error(request_id, HostError("cancelled", "request cancelled"))
    except HostError as exc:
        return _error(request_id, exc)
    except Exception as exc:  # noqa: BLE001
        return _error(request_id, HostError("host_error", str(exc)))
    return _success(request_id, result)


def _success(request_id: Any, result: Any, *, close: bool = False) -> ControlReply:
    return ControlReply(
        {"id": request_id, "type": "response", "ok": True, "result": result},
        close=close,
    )


def _error(request_id: Any, error: HostError) -> ControlReply:
    return ControlReply(
        {
            "id": request_id,
            "type": "response",
            "ok": False,
            "error": error.to_wire(),
        }
    )
