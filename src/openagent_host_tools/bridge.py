"""Transport-neutral implementation of the Gateway capability WebSocket protocol."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import inspect
import json
import platform
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from .host import CapabilityHost
from .idempotency import IdempotencyLedger
from .types import HostError

CLIENT_CAPABILITIES_PROTOCOL = "client-capabilities/1"
ARTIFACT_THRESHOLD_BYTES = 256 * 1024
ARTIFACT_CHUNK_BYTES = 512 * 1024
MAX_ARTIFACT_BYTES_PER_CALL = 64 * 1024 * 1024
MAX_ARTIFACT_TRANSFERS_PER_CALL = 64
MAX_PENDING_EVENTS = 1024


class CapabilityBridge:
    """Translate Gateway capability frames to a local ``CapabilityHost``.

    The caller owns the WebSocket. Pass its ``send_json`` method and feed every
    received JSON object to ``handle``. This keeps Desktop and CLI on the same
    dispatch implementation without adding a WebSocket dependency here.
    """

    def __init__(
        self,
        host: CapabilityHost,
        *,
        client_instance_id: str,
        send_json: Callable[[dict[str, Any]], Awaitable[None] | None],
        generation: int | None = None,
        device_label: str | None = None,
        trusted_account_id: str | None = None,
        trusted_network_id: str | None = None,
        trusted_device_id: str | None = None,
        on_transport_lost: Callable[[], Awaitable[None] | None] | None = None,
    ):
        if not client_instance_id:
            raise ValueError("client_instance_id is required")
        self.host = host
        self.client_instance_id = client_instance_id
        self.send_json = send_json
        self.generation = generation or max(1, time.time_ns())
        self.device_label = device_label or platform.node() or platform.system()
        self.trusted_account_id = str(trusted_account_id) if trusted_account_id else None
        self.trusted_network_id = str(trusted_network_id) if trusted_network_id else None
        self.trusted_device_id = str(trusted_device_id) if trusted_device_id else None
        self.on_transport_lost = on_transport_lost
        self.principal: dict[str, Any] = {
            "kind": "interactive-client",
            "client_instance_id": self.client_instance_id,
            "device_label": self.device_label,
            "account_id": self.trusted_account_id,
            "network_id": self.trusted_network_id,
            "device_id": self.trusted_device_id,
            "generation": self.generation,
        }
        self._calls: dict[str, asyncio.Task[None]] = {}
        self._principals: dict[tuple[str | None, str], dict[str, Any]] = {}
        self._events_subscribed = False
        self._catalog_subscribed = False
        self._disconnect_subscribed = False
        self._transport_lost = False
        self._pending_events: OrderedDict[str, dict[str, Any]] = OrderedDict()

    async def hello(self) -> dict[str, Any]:
        await self.host.start()
        frame = {
            "type": "capability_hello",
            "protocol": CLIENT_CAPABILITIES_PROTOCOL,
            "client_instance_id": self.client_instance_id,
            "generation": self.generation,
            "device_label": self.device_label,
            "servers": await self.host.catalog(),
        }
        if self.trusted_network_id is not None:
            frame["network_id"] = self.trusted_network_id
        await self._send(frame)
        return frame

    def activate_events(self) -> None:
        """Begin event delivery after the Gateway accepted ``hello``.

        Keeping this separate prevents a replayed shell completion from racing
        ahead of the capability handshake on a freshly reconnected socket.
        """
        if hasattr(self.host, "subscribe_events") and not self._events_subscribed:
            self.host.subscribe_events(self._on_host_event)
            self._events_subscribed = True
        if hasattr(self.host, "subscribe_catalog") and not self._catalog_subscribed:
            self.host.subscribe_catalog(self._on_catalog_changed)
            self._catalog_subscribed = True
        if hasattr(self.host, "subscribe_disconnect") and not self._disconnect_subscribed:
            self.host.subscribe_disconnect(self._on_host_disconnect)
            self._disconnect_subscribed = True

    async def catalog_update(
        self, servers: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        frame = {
            "type": "capability_catalog_update",
            "generation": self.generation,
            "servers": await self.host.catalog() if servers is None else servers,
        }
        await self._send(frame)
        return frame

    async def heartbeat(self) -> None:
        # Terminal events are idempotent at the Gateway. Retain and replay them
        # until its explicit ACK so packet loss on an otherwise-live socket
        # cannot strand a completed background shell.
        for frame in list(self._pending_events.values()):
            await self._send(dict(frame))
        await self._send({"type": "capability_heartbeat", "generation": self.generation})

    async def handle(self, frame: dict[str, Any]) -> bool:
        frame_type = frame.get("type")
        if frame_type in {"capability_hello_ack", "capability_heartbeat_ack"}:
            return True
        if frame_type == "client_tool_call":
            call_id = str(frame.get("call_id") or "")
            if not call_id:
                return False
            if int(frame.get("generation", -1)) != self.generation:
                await self._send_error(
                    call_id,
                    HostError("stale_generation", "tool call targets a stale client generation"),
                )
                return True
            if call_id in self._calls:
                await self._send_error(
                    call_id, HostError("duplicate_call", "call_id is already running")
                )
                return True
            task = asyncio.create_task(self._run_call(frame), name=f"gateway-local-tool-{call_id}")
            self._calls[call_id] = task
            task.add_done_callback(lambda _task, cid=call_id: self._calls.pop(cid, None))
            return True
        if frame_type == "client_tool_cancel":
            call_id = str(frame.get("call_id") or "")
            if int(frame.get("generation", -1)) != self.generation:
                if call_id:
                    await self._send_error(
                        call_id,
                        HostError(
                            "stale_generation",
                            "tool cancellation targets a stale client generation",
                        ),
                    )
                return True
            task = self._calls.get(call_id)
            await self.host.cancel(call_id)
            if task is not None:
                task.cancel()
            return True
        if frame_type == "client_tool_event_ack":
            if int(frame.get("generation", -1)) != self.generation:
                return False
            shell_id = str(frame.get("shell_id") or "")
            if not shell_id or frame.get("accepted") is not True:
                return False
            pending = self._pending_events.pop(shell_id, None)
            if pending is not None and hasattr(self.host, "ack_event"):
                await self.host.ack_event(self.principal, shell_id)
            return True
        return False

    async def close(self, *, release_principals: bool = True) -> None:
        if self._events_subscribed and hasattr(self.host, "unsubscribe_events"):
            self.host.unsubscribe_events(self._on_host_event)
            self._events_subscribed = False
        if self._catalog_subscribed and hasattr(self.host, "unsubscribe_catalog"):
            self.host.unsubscribe_catalog(self._on_catalog_changed)
            self._catalog_subscribed = False
        if self._disconnect_subscribed and hasattr(self.host, "unsubscribe_disconnect"):
            self.host.unsubscribe_disconnect(self._on_host_disconnect)
            self._disconnect_subscribed = False
        # Tell the broker first so a cancelled read does not leave a durable
        # ``executing`` claim that rejects the exact reconnect/retry as
        # idempotency_in_flight. Background shells that already returned are
        # no longer in ``_calls`` and remain owned by the stable principal.
        for call_id in list(self._calls):
            try:
                await self.host.cancel(call_id)
            except Exception:
                pass
        for task in list(self._calls.values()):
            task.cancel()
        if self._calls:
            await asyncio.gather(*self._calls.values(), return_exceptions=True)
        if release_principals:
            principals = self._principals.values() if self._principals else (self.principal,)
            for principal in principals:
                await self.host.release_principal(principal)

    async def _on_host_event(self, event: dict[str, Any]) -> None:
        if event.get("type") == "catalog_changed":
            await self._on_catalog_changed(event)
            return
        principal = event.get("principal")
        if principal:
            try:
                parsed = json.loads(principal) if isinstance(principal, str) else principal
            except (TypeError, json.JSONDecodeError):
                return
            if parsed.get("client_instance_id") != self.client_instance_id:
                return
            if parsed.get("account_id") != self.trusted_account_id:
                return
            if parsed.get("network_id") != self.trusted_network_id:
                return
            if self.trusted_device_id and parsed.get("device_id") != self.trusted_device_id:
                return
            if int(parsed.get("generation") or -1) != self.generation:
                return
        shell_id = str(event.get("shell_id") or "")
        if not shell_id:
            return
        frame = {
            "type": "client_tool_event",
            "generation": self.generation,
            "event": event,
        }
        self._pending_events[shell_id] = frame
        self._pending_events.move_to_end(shell_id)
        while len(self._pending_events) > MAX_PENDING_EVENTS:
            self._pending_events.popitem(last=False)
        await self._send(frame)

    async def _on_catalog_changed(self, event: dict[str, Any]) -> None:
        servers = event.get("servers")
        if isinstance(servers, list) and all(
            isinstance(server, dict) for server in servers
        ):
            await self.catalog_update(list(servers))

    async def _on_host_disconnect(self) -> None:
        self._transport_lost = True
        if self.on_transport_lost is not None:
            result = self.on_transport_lost()
            if inspect.isawaitable(result):
                await result

    async def _run_call(self, frame: dict[str, Any]) -> None:
        call_id = str(frame["call_id"])
        try:
            server = str(frame.get("server") or "")
            received_account_id = frame.get("account_id")
            if self.trusted_account_id is None:
                raise HostError(
                    "account_context_unavailable",
                    "this capability socket has no certified account binding",
                )
            if received_account_id is None or str(received_account_id) != self.trusted_account_id:
                raise HostError(
                    "account_mismatch",
                    "capability call account does not match this certified connection",
                )
            received_network_id = frame.get("network_id")
            if received_network_id is not None and (
                self.trusted_network_id is None
                or str(received_network_id) != self.trusted_network_id
            ):
                raise HostError(
                    "network_mismatch",
                    "capability call network does not match this certified connection",
                )
            if server == "agent-in-chrome" and self.trusted_network_id is None:
                raise HostError(
                    "network_context_unavailable",
                    "agent-in-chrome requires a certified network binding",
                )
            args = dict(frame.get("args") or {})
            expected_hash = frame.get("arguments_sha256")
            actual_hash = IdempotencyLedger.arguments_sha256(args)
            if expected_hash is not None and str(expected_hash) != actual_hash:
                raise HostError(
                    "arguments_hash_mismatch",
                    "tool arguments do not match arguments_sha256",
                    {"expected": str(expected_hash), "actual": actual_hash},
                )
            principal = self.principal
            self._principals[(self.trusted_network_id, self.trusted_account_id)] = principal
            result = await self.host.call(
                server,
                str(frame.get("tool") or ""),
                args,
                principal=principal,
                call_id=call_id,
                idempotency_key=(
                    str(frame["idempotency_key"])
                    if frame.get("idempotency_key") is not None
                    else None
                ),
                deadline_ms=frame.get("deadline_ms"),
                arguments_sha256=str(expected_hash) if expected_hash is not None else None,
            )
        except asyncio.CancelledError:
            if self._transport_lost:
                return
            await self._send_error(call_id, HostError("cancelled", "local tool call cancelled"))
            raise
        except HostError as exc:
            if self._transport_lost:
                return
            await self._send_error(call_id, _gateway_error(exc))
        except Exception as exc:  # noqa: BLE001
            if self._transport_lost:
                return
            await self._send_error(call_id, HostError("host_error", str(exc)))
        else:
            try:
                result_wire, artifacts = _prepare_result_artifacts(result.to_wire(), call_id)
                for artifact in artifacts:
                    await self._send_artifact(call_id, artifact)
            except HostError as exc:
                await self._send_error(call_id, exc)
            else:
                await self._send(
                    {
                        "type": "client_tool_result",
                        "call_id": call_id,
                        "generation": self.generation,
                        "result": result_wire,
                    }
                )

    async def _send_artifact(self, call_id: str, artifact: dict[str, Any]) -> None:
        data: bytes = artifact["data"]
        for seq, offset in enumerate(range(0, len(data), ARTIFACT_CHUNK_BYTES)):
            chunk = data[offset : offset + ARTIFACT_CHUNK_BYTES]
            frame: dict[str, Any] = {
                "type": "client_artifact_chunk",
                "call_id": call_id,
                "generation": self.generation,
                "transfer_id": artifact["transfer_id"],
                "seq": seq,
                "data": base64.b64encode(chunk).decode("ascii"),
                "eof": offset + len(chunk) >= len(data),
            }
            if seq == 0:
                frame.update(
                    {
                        "size": len(data),
                        "mime_type": artifact["mime_type"],
                        "sha256": artifact["sha256"],
                    }
                )
            await self._send(frame)

    async def _send_error(self, call_id: str, error: HostError) -> None:
        await self._send(
            {
                "type": "client_tool_result",
                "call_id": call_id,
                "generation": self.generation,
                "error": error.to_wire(),
            }
        )

    async def _send(self, frame: dict[str, Any]) -> None:
        result = self.send_json(frame)
        if inspect.isawaitable(result):
            await result


def _prepare_result_artifacts(
    result: dict[str, Any], call_id: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Extract large base64 MCP blocks into the bounded artifact protocol.

    This intentionally mirrors Desktop's thresholds and accepted block shapes
    so CLI and Desktop produce indistinguishable Gateway frames.
    """
    artifacts: list[dict[str, Any]] = []
    total_bytes = 0
    meta = result.get("_meta") if isinstance(result.get("_meta"), dict) else {}
    index = 0

    def visit(raw: Any) -> Any:
        nonlocal total_bytes, index
        if isinstance(raw, list):
            return [visit(item) for item in raw]
        if not isinstance(raw, dict):
            return raw
        block_type = str(raw.get("type") or "")
        encoded: str | None = None
        mime_type = str(raw.get("mimeType") or raw.get("mime_type") or "application/octet-stream")
        if block_type in {"image", "audio", "video", "file", "blob"} and isinstance(
            raw.get("data"), str
        ):
            encoded = raw["data"]
        elif (
            block_type == "resource"
            and isinstance(raw.get("resource"), dict)
            and isinstance(raw["resource"].get("blob"), str)
        ):
            resource = raw["resource"]
            encoded = resource["blob"]
            mime_type = str(resource.get("mimeType") or resource.get("mime_type") or mime_type)
        elif (
            block_type == "text"
            and isinstance(raw.get("text"), str)
            and meta.get("encoding") == "base64"
        ):
            encoded = raw["text"]
            if isinstance(meta.get("mimeType"), str):
                mime_type = meta["mimeType"]
        data = _decode_base64(encoded) if encoded is not None else None
        if data is not None and len(data) >= ARTIFACT_THRESHOLD_BYTES:
            if len(artifacts) >= MAX_ARTIFACT_TRANSFERS_PER_CALL:
                raise HostError(
                    "too_many_artifacts",
                    "client tool result exceeds "
                    f"{MAX_ARTIFACT_TRANSFERS_PER_CALL} artifact transfers",
                )
            total_bytes += len(data)
            if total_bytes > MAX_ARTIFACT_BYTES_PER_CALL:
                raise HostError(
                    "artifact_too_large",
                    f"client tool artifacts exceed {MAX_ARTIFACT_BYTES_PER_CALL} bytes",
                )
            digest = hashlib.sha256(data).hexdigest()
            transfer_id = f"{call_id[:160]}-{index}-{digest[:12]}"
            index += 1
            artifacts.append(
                {
                    "transfer_id": transfer_id,
                    "mime_type": mime_type,
                    "sha256": digest,
                    "data": data,
                }
            )
            template: dict[str, Any] | None = None
            insert_path: list[str] | None = None
            if block_type in {"image", "audio", "video", "file", "blob"}:
                template = {key: value for key, value in raw.items() if key != "data"}
                insert_path = ["data"]
            elif block_type == "resource" and isinstance(raw.get("resource"), dict):
                resource = dict(raw["resource"])
                resource.pop("blob", None)
                template = {**raw, "resource": resource}
                insert_path = ["resource", "blob"]
            elif block_type == "text":
                template = {key: value for key, value in raw.items() if key != "text"}
                insert_path = ["text"]
            reference: dict[str, Any] = {
                "type": "artifact_ref",
                "transfer_id": transfer_id,
            }
            if template is not None and insert_path is not None:
                reference["artifact_template"] = template
                reference["artifact_insert_path"] = insert_path
            return reference
        return {key: visit(value) for key, value in raw.items()}

    prepared = visit(result)
    if not artifacts:
        return result, artifacts
    return prepared, artifacts


def _decode_base64(value: str) -> bytes | None:
    compact = "".join(value.split())
    if not compact or len(compact) % 4:
        return None
    try:
        return base64.b64decode(compact, validate=True)
    except (ValueError, binascii.Error):
        return None


def _gateway_error(error: HostError) -> HostError:
    if error.code != "idempotency_indeterminate":
        return error
    return HostError(
        "CLIENT_RESULT_INDETERMINATE",
        error.message,
        {"local_code": error.code, **error.data},
    )
