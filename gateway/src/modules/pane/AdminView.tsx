// AdminView — agents · launch. Pane head + a bar
// with a launch action over an agents table (name w/ presence, harness,
// status, evict/kill). Presentational: the live agent rows are
// fetched at the route (`useAdminAgents`) and passed in; with none, the table
// shows an honest empty state. Tokens only.
import { useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";

import { cn } from "@shared/ui";

import { PaneHead } from "./PaneHead";
import type { AdminRow } from "./liveData";

const dotColor: Record<string, string> = {
  online: "bg-online",
  busy: "bg-busy",
  offline: "bg-offline",
};

const AGENT_KINDS = ["codex", "claude"] as const;
type AgentKind = (typeof AGENT_KINDS)[number];

interface AdminViewProps {
  agents?: AdminRow[];
  /** Grant/revoke the durable admin tier for an agent. Backend enforces human-only access. */
  onGrantTier?: (name: string, tier: "agent" | "admin") => void;
  /** Called when the Launch modal is submitted. */
  onLaunch?: (kind: string, name?: string) => void;
  /** Evict — remove the agent from every thread (session + process kept). */
  onEvict?: (name: string) => void;
  /** Kill — terminate the agent's process (record kept; resumable). Confirmed. */
  onKill?: (name: string) => void;
  /** Delete — purge the agent entirely (daemon + the console's stored conversation). Confirmed. */
  onDelete?: (name: string) => void;
}

// ── LaunchModal ───────────────────────────────────────────────────────────────

interface LaunchModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onLaunch?: (kind: string, name?: string) => void;
}

function LaunchModal({ open, onOpenChange, onLaunch }: LaunchModalProps) {
  const [kind, setKind] = useState<AgentKind>("codex");
  const [name, setName] = useState("");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    onLaunch?.(kind, name.trim() || undefined);
    setName("");
    setKind("codex");
    onOpenChange(false);
  }

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />
        <Dialog.Content
          className={cn(
            "fixed left-1/2 top-1/2 z-[400] w-[400px] max-w-[calc(100vw-2rem)]",
            "-translate-x-1/2 -translate-y-1/2",
            "rounded-btn border border-border-subtle bg-bg-secondary",
            "shadow-[var(--lens-shadow-elevated)]",
            "p-6 outline-none",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          )}
          aria-describedby="launch-modal-desc"
        >
          {/* Header */}
          <div className="mb-5 flex items-center justify-between">
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              Launch headless agent
            </Dialog.Title>
            <Dialog.Close
              aria-label="Close"
              className="grid h-[26px] w-[26px] place-items-center rounded-btn text-text-muted outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
            >
              <span aria-hidden="true" className="text-[18px] leading-none">×</span>
            </Dialog.Close>
          </div>

          {/* Note */}
          <p id="launch-modal-desc" className="mb-4 text-[12px] text-text-muted">
            Launches a headless agent (interactive TUI is via the{" "}
            <code className="font-mono text-[11px]">nexus launch</code> CLI).
          </p>

          {/* Form */}
          <form onSubmit={handleSubmit} aria-label="Launch agent" className="flex flex-col gap-4">
            {/* Kind */}
            <div className="flex flex-col gap-1.5">
              <label htmlFor="agent-kind" className="text-[12px] font-semibold text-text-muted">
                Kind
              </label>
              <select
                id="agent-kind"
                value={kind}
                onChange={(e) => setKind(e.target.value as AgentKind)}
                className={cn(
                  "rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px]",
                  "text-[13px] text-text-normal outline-none transition-colors",
                  "hover:border-[color:var(--lens-focus-halo)] focus-visible:ring-2 focus-visible:ring-white/20",
                )}
              >
                {AGENT_KINDS.map((k) => (
                  <option key={k} value={k}>
                    {k}
                  </option>
                ))}
              </select>
            </div>

            {/* Name (optional) */}
            <div className="flex flex-col gap-1.5">
              <label htmlFor="agent-name" className="text-[12px] font-semibold text-text-muted">
                Name{" "}
                <span className="font-normal text-text-muted">(optional)</span>
              </label>
              <input
                id="agent-name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="auto-generated if blank"
                className={cn(
                  "rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px]",
                  "text-[13px] text-text-normal placeholder:text-text-faint outline-none transition-colors",
                  "hover:border-[color:var(--lens-focus-halo)] focus-visible:ring-2 focus-visible:ring-white/20",
                )}
              />
            </div>

            {/* Actions */}
            <div className="flex justify-end gap-2 pt-1">
              <Dialog.Close asChild>
                <button
                  type="button"
                  className={cn(
                    "rounded-pill border border-border-subtle bg-transparent px-4 py-[7px]",
                    "text-[13px] font-semibold text-text-muted outline-none transition-colors",
                    "hover:border-[color:var(--lens-focus-halo)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20",
                  )}
                >
                  Cancel
                </button>
              </Dialog.Close>
              <button
                type="submit"
                className={cn(
                  "rounded-pill border border-border-subtle bg-[color:var(--lens-fill-active)] px-4 py-[7px]",
                  "text-[13px] font-semibold text-text-normal outline-none transition-colors",
                  "hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover focus-visible:ring-2 focus-visible:ring-white/20",
                )}
              >
                Launch
              </button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

// ── AdminView ─────────────────────────────────────────────────────────────────

export function AdminView({
  agents = [],
  onGrantTier,
  onLaunch,
  onEvict,
  onKill,
  onDelete,
}: AdminViewProps) {
  const [modalOpen, setModalOpen] = useState(false);
  // A destructive op awaiting confirmation: kill or delete (evict is non-destructive, no confirm).
  const [pending, setPending] = useState<{ name: string; op: "kill" | "delete" } | null>(null);

  const headers = [
    "Name",
    "Harness",
    "Tier",
    "Status",
    "",
  ];

  function handleConfirm() {
    if (!pending) return;
    if (pending.op === "kill") onKill?.(pending.name);
    else onDelete?.(pending.name);
    setPending(null);
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead title="Admin" glyph="⌥" topic="agents · launch" />
      <div className="mx-auto w-full max-w-[70rem] flex-1 overflow-y-auto px-6 py-[18px] lens-scroll">
        <div className="mb-3.5 flex items-center justify-between">
          <h2 className="text-[15px] font-semibold text-text-normal">Agents</h2>
          <button
            type="button"
            onClick={() => setModalOpen(true)}
            className={cn(
              "rounded-pill border border-border-subtle bg-[color:var(--lens-fill-active)] px-4 py-[7px]",
              "text-[13px] font-semibold text-text-normal outline-none transition-colors",
              "hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover focus-visible:ring-2 focus-visible:ring-white/20",
            )}
          >
            + Launch agent
          </button>
        </div>

        {agents.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-2 rounded-btn border border-border-subtle px-6 py-14 text-center text-text-muted">
            <h3 className="text-[15px] font-semibold text-text-normal">No agents yet</h3>
            <p className="max-w-[44ch] text-[13px]">
              Launch an agent (or run{" "}
              <code className="font-mono text-[12px]">nexus launch</code>) and it appears here
              with live presence and current work.
            </p>
          </div>
        ) : (
          <table className="w-full overflow-hidden rounded-btn border border-border-subtle border-separate border-spacing-0">
            <thead>
              <tr>
                {headers.map((h, i) => (
                  <th
                    key={i}
                    className="border-b border-border-subtle bg-bg-tertiary px-3.5 py-[9px] text-left text-[11px] font-bold text-text-normal"
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {agents.map((a, i) => {
                const last = i === agents.length - 1;
                const cell = "px-3.5 py-2.5 text-[13px] text-text-read";
                const confirming = pending?.name === a.name ? pending.op : null;
                const adminTier = a.tier === "admin";
                return (
                  <tr key={a.name} className="hover:[&>td]:bg-[color:var(--lens-fill-hover)]">
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>
                      <span className="flex items-center gap-2.5 font-semibold text-text-normal">
                        <span
                          className={cn(
                            "h-[7px] w-[7px] rounded-full",
                            dotColor[String(a.presence)] ?? "bg-offline",
                          )}
                        />
                        {a.name}
                      </span>
                    </td>
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>{a.harness}</td>
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>
                      {adminTier ? (
                        <span className="rounded-2 border border-[color:var(--lens-focus-halo)]/60 px-[7px] py-px text-[11px] font-semibold text-text-normal">
                          admin tier
                        </span>
                      ) : a.tier}
                    </td>
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>{a.status}</td>
                    {/* Evict / Kill / Delete actions */}
                    <td
                      className={cn(
                        "px-3.5 py-2.5 text-[13px]",
                        !last && "border-b border-border-subtle",
                      )}
                    >
                      {confirming ? (
                        <span className="flex items-center gap-2">
                          <span className="text-[11px] text-text-muted">
                            {confirming === "delete" ? "Delete forever?" : "Kill?"}
                          </span>
                          <button
                            type="button"
                            aria-label={`Confirm ${confirming} ${a.name}`}
                            onClick={handleConfirm}
                            className={cn(
                              "rounded-2 px-2 py-px text-[11px] font-semibold text-[color:var(--lens-alert)] outline-none transition-colors",
                              "border border-[color:var(--lens-alert)]/40 hover:bg-[color:var(--lens-alert)]/10",
                              "focus-visible:ring-2 focus-visible:ring-white/20",
                            )}
                          >
                            Confirm
                          </button>
                          <button
                            type="button"
                            onClick={() => setPending(null)}
                            className="text-[11px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                          >
                            Cancel
                          </button>
                        </span>
                      ) : (
                        <span className="flex items-center gap-3">
                          <button
                            type="button"
                            aria-label={`${adminTier ? "Revoke admin from" : "Make"} ${a.name}${adminTier ? "" : " admin"}`}
                            onClick={() => onGrantTier?.(a.name, adminTier ? "agent" : "admin")}
                            className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                          >
                            {adminTier ? "Revoke admin" : "Make admin"}
                          </button>
                          <button
                            type="button"
                            aria-label={`Evict ${a.name}`}
                            onClick={() => onEvict?.(a.name)}
                            className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                          >
                            Evict
                          </button>
                          <button
                            type="button"
                            aria-label={`Kill ${a.name}`}
                            onClick={() => setPending({ name: a.name, op: "kill" })}
                            className={cn(
                              "text-[12px] font-semibold text-[color:var(--lens-alert)] underline-offset-2 outline-none transition-colors",
                              "hover:underline focus-visible:ring-2 focus-visible:ring-white/20",
                            )}
                          >
                            Kill
                          </button>
                          <button
                            type="button"
                            aria-label={`Delete ${a.name}`}
                            onClick={() => setPending({ name: a.name, op: "delete" })}
                            className={cn(
                              "text-[12px] font-semibold text-[color:var(--lens-alert)] underline-offset-2 outline-none transition-colors",
                              "hover:underline focus-visible:ring-2 focus-visible:ring-white/20",
                            )}
                          >
                            Delete
                          </button>
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Launch modal */}
      <LaunchModal open={modalOpen} onOpenChange={setModalOpen} onLaunch={onLaunch} />
    </section>
  );
}
