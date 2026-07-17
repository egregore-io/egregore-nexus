// NewThreadDialog — create a thread (a real daemon thread via the existing createThread RPC),
// optionally with initial members, and — when a project is active — file it under that project.
// On success it navigates into the new channel. Radix Dialog; tokens only, matching SettingsModal.
import * as Dialog from "@radix-ui/react-dialog";
import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";

import { useActiveProject } from "@app/activeProject";
import { useCreateThread } from "@app/projects";
import { cn } from "@shared/ui";

export interface NewThreadDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Names the operator can add as initial members (agents/members in the console). */
  candidates?: string[];
  /** Name of the active project (shown so the operator knows where it lands). */
  activeProjectName?: string;
}

export function NewThreadDialog({
  open,
  onOpenChange,
  candidates = [],
  activeProjectName,
}: NewThreadDialogProps) {
  const [name, setName] = useState("");
  const [members, setMembers] = useState<string[]>([]);
  const activeProjectId = useActiveProject();
  const navigate = useNavigate();
  const create = useCreateThread();

  function reset() {
    setName("");
    setMembers([]);
  }

  function toggle(m: string) {
    setMembers((prev) => (prev.includes(m) ? prev.filter((x) => x !== m) : [...prev, m]));
  }

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || create.isPending) return;
    create.mutate(
      { name: trimmed, members, projectId: activeProjectId },
      {
        onSuccess: ({ name: created }) => {
          reset();
          onOpenChange(false);
          void navigate({ to: "/c/$channel", params: { channel: created } });
        },
      },
    );
  }

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        if (!o) reset();
        onOpenChange(o);
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />
        <Dialog.Content
          className={[
            "fixed left-1/2 top-1/2 z-[400] w-[440px] max-w-[calc(100vw-2rem)]",
            "-translate-x-1/2 -translate-y-1/2",
            "rounded-btn border border-border-subtle bg-bg-secondary",
            "shadow-[var(--lens-shadow-elevated)] p-6 outline-none",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          ].join(" ")}
        >
          <div className="mb-4 flex items-center justify-between">
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              New channel
            </Dialog.Title>
            <Dialog.Close
              aria-label="Close"
              className="grid h-[26px] w-[26px] place-items-center rounded-btn text-text-muted outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
            >
              <span aria-hidden="true" className="text-[18px] leading-none">
                ×
              </span>
            </Dialog.Close>
          </div>

          <form onSubmit={submit}>
            <label className="mb-1.5 block text-[12px] font-medium text-text-muted">
              Channel name
            </label>
            <div className="mb-4 flex items-center gap-2 rounded-btn border border-border-subtle bg-surface-inset px-3 focus-within:border-[color:var(--lens-focus-halo)]">
              <span className="text-text-faint">#</span>
              <input
                autoFocus
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. backend"
                className="flex-1 border-0 bg-transparent py-2 text-[13px] text-text-read outline-none placeholder:text-text-muted"
              />
            </div>

            {candidates.length > 0 && (
              <>
                <span className="mb-1.5 block text-[12px] font-medium text-text-muted">
                  Add members <span className="text-text-faint">(optional)</span>
                </span>
                <div className="mb-4 flex max-h-[140px] flex-wrap gap-1.5 overflow-y-auto lens-scroll">
                  {candidates.map((m) => {
                    const on = members.includes(m);
                    return (
                      <button
                        type="button"
                        key={m}
                        onClick={() => toggle(m)}
                        aria-pressed={on}
                        className={cn(
                          "rounded-pill border px-2.5 py-1 text-[12px] outline-none transition-colors focus-visible:ring-2 focus-visible:ring-white/20",
                          on
                            ? "border-border-subtle bg-[color:var(--lens-fill-active)] text-text-normal"
                            : "border-border-subtle text-text-muted hover:text-text-normal",
                        )}
                      >
                        {m}
                      </button>
                    );
                  })}
                </div>
              </>
            )}

            {activeProjectName && (
              <p className="mb-4 text-[12px] text-text-faint">
                Will be filed under project{" "}
                <span className="font-medium text-text-muted">{activeProjectName}</span>.
              </p>
            )}
            {create.isError && (
              <p className="mb-3 text-[12px] text-alert">Could not create the channel. Try again.</p>
            )}

            <div className="flex justify-end gap-2">
              <Dialog.Close
                type="button"
                className="rounded-btn border border-border-subtle px-3 py-1.5 text-[13px] font-medium text-text-muted outline-none transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal focus-visible:ring-2 focus-visible:ring-white/20"
              >
                Cancel
              </Dialog.Close>
              <button
                type="submit"
                disabled={!name.trim() || create.isPending}
                className="rounded-btn border border-border-subtle bg-[color:var(--lens-fill-active)] px-3 py-1.5 text-[13px] font-semibold text-text-normal outline-none transition-colors hover:bg-bg-hover focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {create.isPending ? "Creating…" : "Create channel"}
              </button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
