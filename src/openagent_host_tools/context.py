from __future__ import annotations

import contextvars

current_principal: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "openagent_host_tools_principal", default=None
)
