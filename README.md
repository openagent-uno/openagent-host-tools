# OpenAgent Host Tools

`openagent-host-tools` is the local capability host shared by OpenAgent's
Desktop and interactive CLI clients. It exposes filesystem, editor, shell and
configured local MCP servers from the computer on which the client is running.

The package can be imported as a Python API or launched as an NDJSON stdio
sidecar:

```console
openagent-host-tools
```

The protocol version is `openagent-host-tools/1`. Requests are one JSON object
per line and use the following `type` values: `initialize`, `catalog`, `call`,
`cancel`, `status`, `set_consent`, and `shutdown`.

Local access is fail-closed. A user enables it once per device with the Desktop
client or:

```console
openagent-cli local-tools enable
```

That consent is persistent and unrestricted: after enabling, there are no
per-call prompts or artificial filesystem roots. Only built-ins and MCP plugins
explicitly present in `client-mcps.toml` are loaded.

Desktop and CLI share `~/.openagent/user/client-mcps.toml` and
`~/.openagent/user/client-tools-consent.json`. Set
`OPENAGENT_HOST_TOOLS_HOME` to override that user directory. Internal lease,
idempotency and audit databases live in its `host-tools/` child directory.

The release bundles are built from the computer-control and Agent-in-Chrome
sources under `sidecars/` in this repository. Each native bundle contains a
`bundle-manifest.json` with the version, size and SHA-256 of every runtime file;
frozen hosts verify it before starting a sidecar. Tag `v0.1.0` publishes the
universal Python wheel and native macOS, Linux and Windows x64/arm64 archives
with detached checksums.
