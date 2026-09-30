// PubView — the Pub feed: incoming notifications from
// external producers, each with a route-to affordance. Pane head + a list of
// feed rows centered to a wide reading width. Presentational: the live rows are
// fetched at the route (`usePubFeed`) and passed in; with none, it shows an
// honest empty state. Tokens only.
import { cn } from "@shared/ui";

import { PaneHead } from "./PaneHead";
import type { FeedRow } from "./liveData";

export function PubView({ rows = [] }: { rows?: FeedRow[] }) {
  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead
        title="Pub feed"
        glyph="≋"
        topic="monitor incoming notifications · route who gets what"
      />
      <div className="mx-auto w-full max-w-[64rem] flex-1 overflow-y-auto px-6 py-3.5 lens-scroll">
        {rows.length === 0 ? (
          <div className="flex flex-1 flex-col items-center justify-center gap-2 py-16 text-center text-text-muted">
            <h3 className="text-[15px] font-semibold text-text-normal">No notifications yet</h3>
            <p className="max-w-[44ch] text-[13px]">
              External producers (GitHub, CI, Sentry, cron…) post here once they're
              wired to a topic. Routing rules decide who gets what.
            </p>
          </div>
        ) : (
          rows.map((row, i) => {
            const unrouted = row.routedTo.length === 0;
            return (
              <div
                key={i}
                className={cn(
                  "mb-2 grid grid-cols-[92px_1fr_auto] items-center gap-3.5 rounded-btn",
                  "border border-border-subtle bg-[color:var(--lens-fill-hover)] px-3.5 py-3",
                  "hover:bg-[color:var(--lens-fill-active)]",
                )}
              >
                <span className="rounded-2 border border-border-subtle px-1.5 py-0.5 text-center font-mono text-[11px] font-semibold text-text-muted">
                  {row.src}
                </span>
                <div className="min-w-0">
                  <div className="text-[13px] text-text-read">{row.title}</div>
                  <div className="mt-0.5 text-[11px] text-text-faint">{row.meta}</div>
                </div>
                <button
                  type="button"
                  className={cn(
                    "inline-flex items-center gap-[7px] rounded-pill border border-border-subtle px-2.5 py-1 text-[12px] outline-none",
                    "hover:border-[color:var(--lens-focus-halo)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20",
                    unrouted ? "text-text-faint" : "text-text-muted",
                  )}
                >
                  {unrouted ? "route…" : `→ ${row.routedTo.join(", ")}`}
                </button>
              </div>
            );
          })
        )}
      </div>
    </section>
  );
}
