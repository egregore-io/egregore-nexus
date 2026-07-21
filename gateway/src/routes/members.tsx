// Members route — `/members`.
//
// In the prototype, members surface as the right-panel ctx-view (not a top-level
// pane). This route keeps a thin, on-brand pane that names the roster and shows
// the live Members ctx-view (the full member directory) on the right.
import { createFileRoute } from "@tanstack/react-router";

import { MembersContext, PaneHead, useRoster } from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";

export const Route = createFileRoute("/members")({ component: MembersRoute });

function MembersRoute() {
  const { members } = useRoster();
  return (
    <>
      <section className="flex min-h-0 flex-1 flex-col">
        <PaneHead title="Members" glyph="□" topic="live transport presence" />
        <div className="flex flex-1 flex-col items-center justify-center gap-2 px-10 text-center text-text-muted">
          <h3 className="text-[15px] font-semibold text-text-normal">Roster</h3>
          <p className="max-w-[40ch] text-[13px]">
            Agents and humans with live transport presence. Consumer-specific
            labels belong to that consumer's metadata-backed interface.
          </p>
        </div>
      </section>
      <ContextPanelContent>
        <MembersContext members={members} />
      </ContextPanelContent>
    </>
  );
}
