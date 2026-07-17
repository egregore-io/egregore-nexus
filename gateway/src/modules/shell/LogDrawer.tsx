// LogDrawer — a collapsible bottom console that tails the durable webconsole log
// stream (GET /api/conversation/logs, persisted in the separate Turso). This is
// the "if it moves it has a log" window: nav, clicks, send/run/observe/persist,
// and errors scroll by here so you can see exactly what the UI did.
import { useEffect, useRef, useState } from "react";
import { gatewayFetch } from "@app/gatewayClient";

import { cn } from "@shared/ui";

interface LogRow {
  seq: number;
  ts: number;
  level: string;
  scope: string;
  conversationId?: string;
  message: string;
  data?: string;
}

const LEVEL_COLOR: Record<string, string> = {
  error: "text-[#ff6b6b]",
  warn: "text-[#ffd166]",
  info: "text-text-normal",
  debug: "text-text-muted",
};

function hhmmss(ts: number): string {
  const d = new Date(ts);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

export function LogDrawer(): React.ReactElement {
  const [open, setOpen] = useState(false);
  const [rows, setRows] = useState<LogRow[]>([]);
  const seqRef = useRef(0);
  const bodyRef = useRef<HTMLDivElement>(null);

  // Tail the log stream while open.
  useEffect(() => {
    if (!open) return;
    let alive = true;
    const tick = async () => {
      try {
        const res = await gatewayFetch(`/api/conversation/logs?afterSeq=${seqRef.current}&limit=200`);
        if (!res.ok) return;
        const body = (await res.json()) as { logs?: LogRow[] };
        const fresh = body.logs ?? [];
        if (fresh.length && alive) {
          seqRef.current = fresh[fresh.length - 1]!.seq;
          setRows((prev) => [...prev, ...fresh].slice(-500));
        }
      } catch {
        /* transient — next tick retries */
      }
    };
    void tick();
    const id = setInterval(tick, 1200);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [open]);

  // Auto-scroll to the newest line.
  useEffect(() => {
    if (open && bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [rows, open]);

  return (
    <div className="border-t border-border-subtle bg-surface-inset">
      <button
        type="button"
        aria-label="Toggle log console"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 px-3 py-1 text-[11px] text-text-muted hover:text-text-normal"
      >
        <span className="font-mono">{open ? "▾" : "▸"}</span>
        <span className="font-semibold tracking-wide">LOGS</span>
        {!open && rows.length > 0 && (
          <span className="text-text-faint">· {rows.length} lines</span>
        )}
      </button>
      {open && (
        <div
          ref={bodyRef}
          role="log"
          aria-label="Webconsole logs"
          className="h-48 overflow-auto px-3 pb-2 font-mono text-[11px] leading-[1.5]"
        >
          {rows.length === 0 ? (
            <div className="text-text-faint">No logs yet — interact with the console.</div>
          ) : (
            rows.map((r) => (
              <div key={r.seq} className="flex gap-2 whitespace-pre-wrap">
                <span className="shrink-0 text-text-faint">{hhmmss(r.ts)}</span>
                <span className="shrink-0 text-text-faint">[{r.scope}]</span>
                <span className={cn(LEVEL_COLOR[r.level] ?? "text-text-normal")}>
                  {r.message}
                  {r.data ? <span className="text-text-faint"> {r.data}</span> : null}
                </span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}
