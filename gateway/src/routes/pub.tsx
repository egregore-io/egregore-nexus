// Pub route — `/pub`.
//
// The Pub feed (incoming notifications + route-to) with the routing-rules
// ctx-view, both fed from the live read-view (`usePubFeed`/`usePubRuleFacts`).
// No seed.
import { createFileRoute } from "@tanstack/react-router";

import { PubContext, PubView, usePubFeed, usePubRuleFacts } from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";

export const Route = createFileRoute("/pub")({ component: PubRoute });

function PubRoute() {
  const { rows } = usePubFeed();
  const { facts } = usePubRuleFacts();
  return (
    <>
      <PubView rows={rows} />
      <ContextPanelContent>
        <PubContext rules={facts} />
      </ContextPanelContent>
    </>
  );
}
