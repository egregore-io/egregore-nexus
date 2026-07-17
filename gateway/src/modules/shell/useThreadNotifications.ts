// Native browser notifications for subscribed-thread activity.
//
// Rides the SAME polled threads read-view the sidebar already uses
// (`useThreads` → GET /api/v1/threads, refetchIntervalInBackground: true) — no
// new socket, no new poller. Each `ThreadRow` carries `latestSeq`/`lastAt`;
// when a thread's stamp advances while the tab is hidden or unfocused, we
// fetch that thread's newest history line and fire a `Notification` whose
// BODY IS THE MESSAGE ITSELF (agent's reply — proof the agent already acted,
// not just "something happened"). Clicking the toast focuses the window and
// navigates to `/c/$channel`.
//
// Permission is never prompted from here (browsers require a user gesture and
// the demo rig pre-grants via CDP `Browser.grantPermissions`); if permission
// isn't "granted" the hook is inert. First poll after mount only baselines —
// the backlog never toasts.
//
// Note: the history peek issues a human read receipt (same as opening the
// thread) — acceptable: the human was notified with the content, so "seen"
// is honest.
import { useNavigate } from "@tanstack/react-router";
import { useEffect, useRef } from "react";

import { useThreads } from "@modules/pane";
import { gatewayFetch } from "@app/gatewayClient";
import type { HistoryRow, ThreadRow } from "@shared/readView";

/** Longest toast body we'll show (Notification bodies clip anyway). */
const PREVIEW_MAX = 160;

/** A thread's monotonically-advancing activity stamp (seq preferred). */
const stampOf = (t: ThreadRow): number => t.latestSeq ?? t.lastAt ?? 0;

/**
 * Diff one threads snapshot against the previously-seen stamps and return the
 * names whose stamp advanced. Mutates `seen` to the new stamps. Exported for
 * tests.
 */
export function advancedThreads(
  seen: Map<string, number>,
  rows: ThreadRow[],
): string[] {
  const out: string[] = [];
  for (const t of rows) {
    const prev = seen.get(t.name);
    const next = stampOf(t);
    seen.set(t.name, next);
    if (prev !== undefined && next > prev) out.push(t.name);
  }
  return out;
}

/** True when the operator is NOT looking at this tab right now. */
function tabUnattended(): boolean {
  return document.hidden || !document.hasFocus();
}

/** Fetch the newest history line of a thread (or null on any failure). */
async function peekLatest(thread: string): Promise<HistoryRow | null> {
  try {
    const res = await gatewayFetch(
      `/api/v1/threads/${encodeURIComponent(thread)}/history?limit=1`,
      { headers: { accept: "application/json" } },
    );
    if (!res.ok) return null;
    const rows = (await res.json()) as HistoryRow[];
    return rows[rows.length - 1] ?? null;
  } catch {
    return null;
  }
}

/**
 * Mounted once in the AppShell. Watches the polled threads read-view and
 * raises a native notification per thread that gains a message while the tab
 * is unattended.
 */
export function useThreadNotifications(): void {
  const threads = useThreads();
  const navigate = useNavigate();
  // null until the first snapshot lands — that snapshot only baselines.
  const seenRef = useRef<Map<string, number> | null>(null);

  const data = threads.data;
  useEffect(() => {
    if (!data) return;
    if (typeof Notification === "undefined") return; // SSR / unsupported

    if (seenRef.current === null) {
      seenRef.current = new Map(data.map((t) => [t.name, stampOf(t)]));
      return;
    }

    const changed = advancedThreads(seenRef.current, data);
    if (changed.length === 0) return;
    if (Notification.permission !== "granted") return;
    if (!tabUnattended()) return; // they're already watching the app

    for (const name of changed) {
      void peekLatest(name).then((last) => {
        const title = last?.from ? `${last.from} @ #${name}` : `#${name}`;
        const body = (last?.summary || last?.body || "New message").slice(
          0,
          PREVIEW_MAX,
        );
        // tag: one live toast per thread — a burst replaces, never stacks.
        const n = new Notification(title, { body, tag: `nexus-${name}` });
        n.onclick = () => {
          window.focus();
          void navigate({ to: "/c/$channel", params: { channel: name } });
          n.close();
        };
      });
    }
  }, [data, navigate]);
}
