> [!IMPORTANT]
> This is a historical source repository. New development and coordinated
> releases live in [`openagent`](https://github.com/openagent-uno/openagent).
> Existing tags, releases and updater URLs remain available during the verified
> transition and will be preserved when this repository is archived. See
> <https://openagent.uno/downloads> for current artifacts.

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

The Python built-ins also have official MCP stdio entrypoints:
`openagent-filesystem-mcp`, `openagent-editor-mcp`, and `openagent-shell-mcp`.
The shell advertises the experimental capability
`openagent/shell-completion` version `1`. Background completion is delivered as
a standard `notifications/message` logging notification from logger
`openagent.shell`; its `data` object is the same `shell_completed` event emitted
by the embedded capability host. MCP consumers should register a logging
callback and route that logger's data into the agent event/autoloop path rather
than treating it as diagnostic text.

## Compatibility UTF-8 patch 0.1.3

This narrowly scoped transition release keeps the `openagent-host-tools/1`
protocol and 0.1.2 dependency lock. NDJSON output is encoded explicitly as UTF-8
bytes, so a frozen Windows process cannot serialize Unicode using CP1252.
The same correction is submitted to the canonical monorepo. No Core dependency
is added or upgraded. This repository is reopened only for this compatibility
release and will return to its archived state after publication.

`compat-utf8.yml` builds the Python executable on all six native runners and
checks real non-ASCII initialize/status/shutdown frames. Native sidecars, Node
and browser runtime files are reused byte-for-byte from the checksum-pinned
0.1.2 release; `compatibility-<platform>.json` records their hashes. The frozen
MCP core smoke and archive verifier also run. These are qualification candidates,
not public releases: macOS signing/notarization and the final release-index
verification must finish before an immutable 0.1.3 release is published.
