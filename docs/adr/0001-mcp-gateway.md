# 0001 — MCP execution gateway

Status: proposed
Date: 2026-11-19

## Context
ADR 0006 keeps the registry off the tool-call path and allows an optional gateway that
authorizes execution. Agents in `bae6-ai` need one governed way to call approved servers
without each holding network reach to arbitrary endpoints. Upstream OAuth needs a token
exchange design that does not exist yet (ADR 0003 forbids passthrough).

## Decision
- A separate optional service, `services/mcp-gateway`. The registry, data and embed
  services do not depend on it, and it does not read the registry's database (ADR 0007):
  it reads approved entries from the registry API with its own workload token.
- One operation, `POST /v1/servers/{id}/tools/call` with `{tool, arguments, version?}`.
  The gateway speaks MCP streamable HTTP to the upstream per call (initialize, initialized,
  `tools/call`, close) and keeps no session between calls. Resources, prompts, streamed
  results and server-initiated requests (sampling, elicitation) are not supported and are
  refused.
- Scope `mcp:invoke`, separate from `mcp:discover`, `mcp:publish` and `mcp:approve`. The
  organization comes from the token issuer's configuration (ADR 0002). A server of another
  organization is `404`.
- A call needs an approved version (the latest approved, or the one pinned by `version`)
  and the tool must be one that version declares. Anything else is refused before the
  network is touched.
- **Upstream authentication is `none` only in v1.** A version declaring `oauth2` is
  `409 upstream_auth_unsupported`. The gateway never sends the caller's token or any
  credential upstream. Consequence: approving a `none` server lets every holder of
  `mcp:invoke` in that organization call its declared tools, so approvers must treat the
  approval as that grant. OAuth upstreams need their own ADR (RFC 8693 exchange, audience
  and resource binding).
- Egress is the registry's: `checkEndpoint` is re-applied at call time, names resolve
  through `guardedLookup` to public addresses only, redirects are refused, and the
  connection uses the checked address.
- Bounded everywhere: request body 64 KiB, upstream response 1 MiB, per-call deadline,
  per-organization and global in-flight caps (`429` with `Retry-After`), no retries. The
  upstream's text never appears in error responses; failures carry a fixed code.
- Tool results are untrusted data. They are returned unmodified under `result`; the
  gateway does not interpret them and does not forward tool descriptions.
- Approved entries are cached per organization with a TTL (default 30 s, at most 300 s).
  The TTL is the revocation bound from ADR 0006. A registry outage fails calls whose entry
  is not cached, and does not affect cached ones until the TTL ends.
- Each call writes one structured access-log event: organization, subject, workload,
  server, version, tool, outcome, duration. Arguments and results are never logged.

## Consequences
- Agents get one allowlisted path to MCP tools, and the registry stays a control plane.
- A per-call handshake costs extra round trips; session reuse is a later optimization.
- `none`-only limits the gateway to servers that are safe to reach unauthenticated from the
  gateway's network, which operators enforce with the egress policy.
- Unverified until built: behaviour against real MCP servers, including those that answer
  with an event stream; v1 accepts a JSON reply or the first matching event.
