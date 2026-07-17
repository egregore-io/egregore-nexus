// Search route — `/search`.
//
// The prototype's search is the top-bar input; this route is a thin, on-brand
// landing for deep-linked/submitted queries until the search feature view (full
// read-view query, scoped, jump-to-message) lands.
import { createFileRoute } from "@tanstack/react-router";

import { PaneHead } from "@modules/pane";

export const Route = createFileRoute("/search")({ component: SearchRoute });

function SearchRoute() {
  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead title="Search" glyph="⌕" topic="messages · members · topics" />
      <div className="flex flex-1 flex-col items-center justify-center gap-2 px-10 text-center text-text-muted">
        <h3 className="text-[15px] font-semibold text-text-normal">
          Search the web console
        </h3>
        <p className="max-w-[40ch] text-[13px]">
          Full-text across messages, members, and topics — scoped by project,
          channel, sender, and kind. Use the search box up top.
        </p>
      </div>
    </section>
  );
}
