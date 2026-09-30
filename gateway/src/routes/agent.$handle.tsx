// Agent session route — `/agent/$handle` where `handle` is `<name>:<session_id>`.
//
// This is the OPERATOR's view onto one daemon-owned harness session (NOT a "DM" —
// a DM is agent↔agent). Whatever the operator types here injects a turn into that
// session; the harness's reply AND the operator's own input stream back as the
// session's AG-UI `observe` feed. The `nexus attach <name>` TUI is the terminal
// twin of this view — both watch the same session, so they mirror each other.
//
// The `session_id` in the handle disambiguates the exact session (names are
// roster-unique today, but the id is explicit so the view can never address the
// wrong session). Addressing the observe/prompt by name stays correct because the
// name resolves to this one session.
//
// Lane B: observes `/api/agui/observe?session=<name>` (agent.update stream).
// Composer: session input (`POST /api/conversation/prompt`) — NOT a bus send.
import { createFileRoute } from "@tanstack/react-router";
import { useIdentityName } from "@app/identity";

import {
  AgentContext,
  ConversationPane,
  useAgentFacts,
  useAgentSession,
  useAgentSessionView,
} from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";

export const Route = createFileRoute("/agent/$handle")({ component: AgentSessionRoute });

/** Split `<name>:<session_id>` → `{ name, sessionId }`. A bare `name` (no colon) is tolerated. */
export function parseHandle(handle: string): { name: string; sessionId: string } {
  const i = handle.indexOf(":");
  if (i === -1) return { name: handle, sessionId: "" };
  return { name: handle.slice(0, i), sessionId: handle.slice(i + 1) };
}

function AgentSessionRoute() {
  const { handle } = Route.useParams();
  const { name, sessionId } = parseHandle(handle);
  const header = useAgentSessionView(name);
  const { facts } = useAgentFacts(name);
  const operator = useIdentityName();
  const you = operator
    ? { who: operator, glyph: operator.charAt(0).toUpperCase() }
    : undefined;

  const { messages, send } = useAgentSession({
    name,
    sessionId: sessionId || undefined,
    agent: {
      who: name,
      glyph: name.charAt(0).toUpperCase(),
      presence: header.presence,
    },
    you,
  });

  return (
    <>
      <ConversationPane
        view={{
          key: name,
          title: header.title,
          presence: header.presence,
          composerPlaceholder: header.composerPlaceholder,
          composerLabel: header.composerLabel,
        }}
        items={messages}
        onSend={send}
        textRenderer="streamdown"
      />
      <ContextPanelContent>
        <AgentContext facts={facts} session={sessionId || undefined} />
      </ContextPanelContent>
    </>
  );
}
