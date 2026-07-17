// DM route — `/dm/$agent` (T9b).
//
// Renders the shared conversation pane as a Message Post conversation: the
// thread shows committed `message.created` events (Lane A), and the composer
// sends via the daemon bus (`sendMode: "bus"`). This mirrors the `/c` channel
// route — operator DMs go via the nexus envelope on the bus, NOT direct ACP.
//
// Keep the DM chrome (title from `useDmView`, AgentContext facts panel). The
// observe endpoint is `?dm=<agent>`, which now falls through to `handleObserve`
// (Lane A), exactly like `?thread=`.
import { createFileRoute } from "@tanstack/react-router";

import {
  AgentContext,
  LiveChannelPane,
  useAgentFacts,
  useDmView,
} from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";

export const Route = createFileRoute("/dm/$agent")({ component: DmRoute });

function DmRoute() {
  const { agent } = Route.useParams();
  const view = useDmView(agent);
  const { facts } = useAgentFacts(agent);

  return (
    <>
      <LiveChannelPane
        view={view}
        thread={agent}
        agent={{
          who: view.title,
          glyph: view.title.charAt(0).toUpperCase(),
          presence: view.presence,
        }}
      />
      <ContextPanelContent>
        <AgentContext facts={facts} />
      </ContextPanelContent>
    </>
  );
}
