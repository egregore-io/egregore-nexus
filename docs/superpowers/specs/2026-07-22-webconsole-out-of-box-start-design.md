# Webconsole Out-of-the-Box Start Design

## Goal

`nexus webconsole start` starts a usable Webconsole as a detached background process on Linux, macOS, and Windows. It returns control to the invoking shell after readiness without restarting or interrupting the Nexus daemon, Gateway, agents, or active sessions.

## Lifecycle Contract

- Reuse a verified healthy Webconsole when one is already running.
- Start or reuse the Gateway through its existing lifecycle; never stop or restart a healthy Gateway as part of Webconsole startup.
- Treat the requested port as preferred. If another process owns it, leave that process untouched and bind the Webconsole to an operating-system-assigned free port.
- Publish the actual bound host, port, URL, Gateway URL, PID, and executable in the existing discovery record.
- Remove or replace stale discovery during startup.
- Keep `start` background-only. `launch` retains its separate browser-opening behavior.
- Report readiness only after the discovered Webconsole health endpoint succeeds.

## Server Resilience

The packaged Webconsole server owns the final bind decision. It first attempts the requested port and, only for an address-in-use failure, retries once on port `0`. This avoids the race introduced by selecting and releasing a free port in the Rust parent before spawning Node.

Gateway proxy failures are scoped to the affected request. Upstream response bodies are transferred with an awaited pipeline so an abort or body timeout rejects into the request handler instead of becoming an unhandled stream error. Before response headers are sent, the handler returns the existing typed `502 gateway_unavailable` response. After headers are sent, it terminates only that response safely. The Webconsole process and `/health` endpoint remain available.

## Safety Boundaries

- Never kill, replace, or adopt an unrelated process on the preferred port.
- Only reuse a process that passes the existing Webconsole health and executable checks.
- Preserve detached process flags, null stdin, and log-file stdout/stderr on every supported operating system.
- Preserve actual-port discovery as the authority used by `status`, `url`, `stop`, and `launch`.
- Do not introduce an OS service-manager dependency.

## Verification

Tests will prove:

1. An occupied preferred port causes a successful fallback bind and records the actual free port.
2. A stale discovery record is replaced during a successful start.
3. An aborted or timed-out upstream body cannot terminate the Webconsole process; a subsequent `/health` request succeeds.
4. A healthy existing Webconsole is reused rather than duplicated.
5. The Rust lifecycle permits safe fallback without touching an unrelated port owner.
6. Existing Windows command-shim and detached-launch contracts remain green.

