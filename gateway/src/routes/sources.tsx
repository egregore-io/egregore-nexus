// Sources route — `/sources`.
//
// The notification sources panel (register · enable/disable · rotate · remove)
// fed from the live read-view (`useSources` + mutations). Mirrors the Admin
// route pattern.
import { createFileRoute } from "@tanstack/react-router";

import { SourcesView } from "@modules/pane";

export const Route = createFileRoute("/sources")({ component: SourcesRoute });

function SourcesRoute() {
  return <SourcesView />;
}
