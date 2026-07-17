# Examples

Runnable references for talking to Nexus. Each one is small on purpose — it shows the *shape*, not a
framework. Two directions: **notification sources** — how anything (a desktop notification stream,
your own app, a webhook) puts events on the bus for agents to react to — and **sinks** — how bus
traffic reaches a human (desktop toasts) without Nexus learning what's on the other end.

> **Status:** these are runnable references for the current source API. The local example is covered
> by `bash examples/sources/_smoke.sh`, which starts an isolated embedded-store daemon and proves one
> exactly-once pull-consumer receipt. The Python and Node remote examples are exercised against the
> signed gateway edge; platform notification taps still require their listed OS dependencies.

## The source model in one breath

A **source** is a generic producer. It pushes opaque events (`summary` / `body` / `meta`) to a
**topic**; agents **subscribe** to the topics they care about and get woken on each event. Nexus
never knows or cares what the source *is* — the **tap** (where events come from) and the **filter**
(which ones matter) live entirely in your script. The only Nexus surface a source author learns is
one command: `nexus push`.

```
your tap  ──►  your filter  ──►  nexus push <source> --topic <t>   ──►  topic  ──►  subscribed agents
(D-Bus, Action Center,        (the only Nexus
 your app, a webhook…)         touchpoint)
```

## The API these examples assume

CLI is the contract; the web console and the HTTP edge are just translations of it.

```bash
# register once (operator). --topic = default fan-out topic (defaults to <name>).
nexus source register <name> [--topic <topic>]      # prints a token (only remote producers need it)
nexus source ls | show <name> | enable <name> | disable <name> | rotate <name> | rm <name>

# push an event AS a source (script author). local on the operator's box = trusted, no token.
nexus push <source> [--topic <t>] [--summary <s>] [ -m <body> | --json | <stdin pipe> ]

# the consumer side (already in Nexus today)
nexus subscribe <topic> | unsubscribe <topic> | topics
```

Remote producers (a server, a SaaS webhook, your app on another box) can't run the CLI, so they POST
the **same** event to one edge, signed with the source's token:

```
POST /api/v1/sources/<name>/push
  X-Nexus-Timestamp: <unix seconds>
  X-Nexus-Signature: sha256=<hmac_sha256(token, "<ts>." + rawBody)>
  Content-Type: application/json
  {"summary":"…","body":"…","meta":{…}}            # ?topic=<t> overrides the source default
```

### Contract guarantees the examples rely on
- **`body` is required**; `summary` and `meta` are optional.
- **`meta` is passthrough** — stored and delivered verbatim, opaque to Nexus, so your app can rely on
  its own schema reaching its agents.
- **`--topic` per push** — one registered source can fan different events to different topics
  (`deploys`, `errors`, `alerts`) from a single identity.
- **Local is trusted** — a `nexus push` from the operator's own machine needs no token; the token
  exists only so a *remote* producer can authenticate to the edge.
- **Failures are loud** — all examples raise/exit non-zero when Nexus rejects a push; they never
  present an upstream application event as delivered after a failed transport call.

## What's here

| File | Source kind | Tap |
|---|---|---|
| [`sources/desktop_notifications_linux.py`](sources/desktop_notifications_linux.py) | desktop/app notifications, filtered | freedesktop D-Bus |
| [`sources/desktop_notifications_windows.py`](sources/desktop_notifications_windows.py) | desktop/app notifications, filtered | WinRT `UserNotificationListener` |
| [`sources/custom_app_local.py`](sources/custom_app_local.py) | your own app, same box | direct (shells `nexus push`) |
| [`sources/custom_app_remote.py`](sources/custom_app_remote.py) | your own app / server, remote | direct (signed HTTP) |
| [`sources/custom_app_remote.mjs`](sources/custom_app_remote.mjs) | your own app, Node | direct (signed HTTP) |

### Sinks (the mirror image)

A **sink** drains the bus and delivers to a human. Same edge philosophy, reversed: the only Nexus
touchpoint is one long-lived `nexus listen --json` (the same held-receive drain loop agents run);
the notification library and the filter live entirely in your script.

| File | Sink | Delivery |
|---|---|---|
| [`sinks/desktop_toast.py`](sinks/desktop_toast.py) | desktop toasts, Linux, zero-dep | shells `notify-send` (swap for `desktop-notifier`/Apprise) |
| [`sinks/desktop_toast.mjs`](sinks/desktop_toast.mjs) | desktop toasts, all three OSes | `node-notifier` |

### Other recipes (no script needed)

**GitHub / CI webhook** — register a source, paste its token as the webhook secret, point the webhook
at `/sources/<name>/push`:
```bash
nexus source register github --topic prs     # token → GitHub webhook "Secret"
# Payload URL: https://<your-nexus>/api/v1/sources/github/push
```

**Cron / scheduled** — the push *is* the job:
```cron
0 9 * * 1-5  nexus push cron -m "standup in 5"
```
On Windows, the same line as a Task Scheduler action running `nexus.exe push cron -m "…"`.

See [`docs/adding-a-harness.md`](../docs/adding-a-harness.md) for the agent side and
[`docs/architecture.md`](../docs/architecture.md) for the bus.
