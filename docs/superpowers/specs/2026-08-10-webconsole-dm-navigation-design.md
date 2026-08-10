# Webconsole DM-First Agent Navigation

## Goal

The Nexus Webconsole presents human-to-agent interaction as durable direct
messaging. Selecting an agent from the **Direct messages** rail opens that
agent's DM conversation instead of the live harness-session observer.

## User-visible behavior

- A Direct messages row links to `/dm/<agent_id>` using the stable agent
  identity supplied by the Gateway member projection.
- The DM page shows only durable messages between the signed-in human and that
  exact agent.
- The DM page loads history and new messages through the existing HTTP cursor
  long-poll and sends through the existing Nexus message endpoint. It opens no
  WebSocket or EventSource.
- Agent thoughts, tool activity, terminal output, and unrelated session events
  never appear in the DM page.

## Compatibility

- `/agent/<name>:<session_id>` remains unchanged and continues to provide the
  explicit live session observer over the existing AG-UI WebSocket path.
- The Webconsole no longer links normal Direct messages navigation to that live
  route. Existing deep links remain valid.
- `/dm/<agent_name>` remains compatible. Stable-id links become the canonical
  links emitted by the Webconsole.

## Implementation boundary

1. Carry `agentId` from `MemberRow` into `DmNavItem`.
2. Change `DmRow` matching and linking from `/agent/$handle` to `/dm/$agent`,
   passing the exact `agentId`.
3. Resolve DM header/presence/facts by stable id so the page continues to show
   the human-readable agent name.
4. Reuse `LiveChannelPane` and `useGatewayMessageHistory`; add no new API,
   WebSocket, EventSource, or message authority.

## Failure behavior

A member without a stable agent id is not exposed as an addressable Direct
messages row. The UI must not fabricate an id from a display name or session
id.

## Verification

- Navigation tests prove a DM row targets `/dm/<agent_id>` and never
  `/agent/<name>:<session_id>`.
- Route tests prove an id-addressed DM renders the human-readable identity and
  uses the existing bus target.
- Existing agent-session WebSocket tests remain unchanged and green.
- Existing DM history tests continue to prove that ordinary DM panes open no
  WebSocket/EventSource.
