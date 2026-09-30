// Channel / team route — `/c/$channel`.
//
// Renders the shared conversation pane (head + thread + composer) over the live
// AG-UI stream, with the Members ctx-view in the right panel. The pane header
// and roster are built from the live read-view (`useChannelView`/`useRoster`);
// the thread body hydrates recent daemon history, then tails the channel's
// AG-UI `observe` stream. Teams come through here with a `team-` param prefix.
import { useState } from "react";
import type { FormEvent } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { Archive, Pencil, Trash2 } from "lucide-react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";

import {
  LiveChannelPane,
  MembersContext,
  useAddThreadMember,
  useArchiveThread,
  useChannelView,
  useDeleteThread,
  useMembers,
  useRenameThread,
  useRemoveThreadMember,
  useRoster,
} from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";
import { cn } from "@shared/ui";

export const Route = createFileRoute("/c/$channel")({ component: ChannelRoute });

function ChannelRoute() {
  const { channel } = Route.useParams();
  const view = useChannelView(channel);
  const { members } = useRoster(channel);
  const navigate = useNavigate();

  // Addable = the directory minus who's already in the thread.
  const directory = useMembers();
  const present = new Set(members.map((m) => m.name));
  const candidates = (directory.data ?? [])
    .map((m) => m.name)
    .filter((n) => !present.has(n));

  const addMember = useAddThreadMember();
  const removeMember = useRemoveThreadMember();
  const renameThread = useRenameThread();
  const archiveThread = useArchiveThread();
  const deleteThread = useDeleteThread();

  async function renameChannel(newName: string) {
    const renamed = await renameThread.mutateAsync({ thread: channel, name: newName });
    await navigate({ to: "/c/$channel", params: { channel: renamed.name } });
  }

  async function closeThread(op: "archive" | "delete") {
    if (op === "archive") await archiveThread.mutateAsync({ thread: channel });
    else await deleteThread.mutateAsync({ thread: channel });
    await navigate({ to: "/" });
  }

  return (
    <>
      <LiveChannelPane
        view={view}
        thread={channel}
        actions={
          <ThreadPaneActions
            thread={channel}
            busy={renameThread.isPending || archiveThread.isPending || deleteThread.isPending}
            onRename={(name) => renameChannel(name)}
            onArchive={() => void closeThread("archive")}
            onDelete={() => void closeThread("delete")}
          />
        }
      />
      <ContextPanelContent>
        <MembersContext
          members={members}
          candidates={candidates}
          onAdd={(member) => addMember.mutate({ thread: channel, member })}
          onRemove={(member) => removeMember.mutate({ thread: channel, member })}
        />
      </ContextPanelContent>
    </>
  );
}

interface ThreadPaneActionsProps {
  thread: string;
  busy?: boolean;
  onRename: (name: string) => Promise<void>;
  onArchive: () => void;
  onDelete: () => void;
}

function ThreadPaneActions({
  thread,
  busy = false,
  onRename,
  onArchive,
  onDelete,
}: ThreadPaneActionsProps) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [renameOpen, setRenameOpen] = useState(false);
  const [name, setName] = useState(thread);
  const [renameError, setRenameError] = useState<string | undefined>();

  function resetRename() {
    setName(thread);
    setRenameError(undefined);
  }

  async function submitRename(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const next = name.trim();
    if (!next || next === thread || busy) return;
    setRenameError(undefined);
    try {
      await onRename(next);
      setRenameOpen(false);
    } catch (err) {
      const detail = err instanceof Error ? err.message : "Thread rename failed";
      setRenameError(
        detail.toLowerCase().includes("unauthorized") || detail.includes("403")
          ? "Only admin-tier callers can rename threads."
          : detail,
      );
    }
  }

  return (
    <div className="flex items-center gap-1.5">
      <Dialog.Root
        open={renameOpen}
        onOpenChange={(open) => {
          setRenameOpen(open);
          if (open) resetRename();
        }}
      >
        <Dialog.Trigger asChild>
          <button
            type="button"
            aria-label={`Rename ${thread}`}
            title="Rename thread"
            disabled={busy}
            className={threadActionClass()}
          >
            <Pencil aria-hidden="true" size={15} strokeWidth={1.7} />
          </button>
        </Dialog.Trigger>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />
          <Dialog.Content
            className={cn(
              "fixed left-1/2 top-1/2 z-[400] w-[380px] max-w-[calc(100vw-2rem)]",
              "-translate-x-1/2 -translate-y-1/2 rounded-btn border border-border-subtle",
              "bg-bg-secondary p-5 shadow-[var(--lens-shadow-elevated)] outline-none",
              "focus-visible:ring-2 focus-visible:ring-white/20",
            )}
          >
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              Rename #{thread}
            </Dialog.Title>
            <Dialog.Description className="mt-2 text-[13px] leading-5 text-text-muted">
              Rename the channel everywhere it appears. Membership and message history stay attached.
            </Dialog.Description>
            <form className="mt-5" onSubmit={submitRename}>
              <label className="mb-1.5 block text-[12px] font-medium text-text-muted">
                Channel name
              </label>
              <div className="flex items-center gap-2 rounded-btn border border-border-subtle bg-surface-inset px-3 focus-within:border-[color:var(--lens-focus-halo)]">
                <span className="text-text-faint">#</span>
                <input
                  autoFocus
                  value={name}
                  onChange={(event) => {
                    setName(event.target.value);
                    setRenameError(undefined);
                  }}
                  className="h-9 min-w-0 flex-1 bg-transparent text-[14px] text-text-normal outline-none placeholder:text-text-faint"
                />
              </div>
              {renameError && (
                <p role="alert" className="mt-3 text-[12px] leading-5 text-[color:var(--lens-alert)]">
                  {renameError}
                </p>
              )}
              <div className="mt-5 flex justify-end gap-2">
                <Dialog.Close asChild>
                  <button
                    type="button"
                    className={cn(
                      "rounded-pill border border-border-subtle bg-transparent px-4 py-[7px]",
                      "text-[13px] font-semibold text-text-muted outline-none transition-colors",
                      "hover:border-[color:var(--lens-focus-halo)] hover:text-text-normal",
                      "focus-visible:ring-2 focus-visible:ring-white/20",
                    )}
                  >
                    Cancel
                  </button>
                </Dialog.Close>
                <button
                  type="submit"
                  disabled={busy || !name.trim() || name.trim() === thread}
                  className={cn(
                    "rounded-pill border border-border-subtle bg-surface-raised px-4 py-[7px]",
                    "text-[13px] font-semibold text-text-normal outline-none transition-colors",
                    "hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover",
                    "focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-50",
                  )}
                >
                  Rename thread
                </button>
              </div>
            </form>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      <button
        type="button"
        aria-label={`Archive ${thread}`}
        title="Archive thread"
        disabled={busy}
        onClick={onArchive}
        className={threadActionClass()}
      >
        <Archive aria-hidden="true" size={15} strokeWidth={1.7} />
      </button>

      <Dialog.Root open={confirmOpen} onOpenChange={setConfirmOpen}>
        <Dialog.Trigger asChild>
          <button
            type="button"
            aria-label={`Delete ${thread}`}
            title="Delete thread"
            disabled={busy}
            className={threadActionClass("danger")}
          >
            <Trash2 aria-hidden="true" size={15} strokeWidth={1.7} />
          </button>
        </Dialog.Trigger>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />
          <Dialog.Content
            className={cn(
              "fixed left-1/2 top-1/2 z-[400] w-[380px] max-w-[calc(100vw-2rem)]",
              "-translate-x-1/2 -translate-y-1/2 rounded-btn border border-border-subtle",
              "bg-bg-secondary p-5 shadow-[var(--lens-shadow-elevated)] outline-none",
              "focus-visible:ring-2 focus-visible:ring-white/20",
            )}
          >
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              Delete #{thread}
            </Dialog.Title>
            <Dialog.Description className="mt-2 text-[13px] leading-5 text-text-muted">
              This removes the thread from active routing and navigation. Durable
              message rows remain in the store.
            </Dialog.Description>
            <div className="mt-5 flex justify-end gap-2">
              <Dialog.Close asChild>
                <button
                  type="button"
                  className={cn(
                    "rounded-pill border border-border-subtle bg-transparent px-4 py-[7px]",
                    "text-[13px] font-semibold text-text-muted outline-none transition-colors",
                    "hover:border-[color:var(--lens-focus-halo)] hover:text-text-normal",
                    "focus-visible:ring-2 focus-visible:ring-white/20",
                  )}
                >
                  Cancel
                </button>
              </Dialog.Close>
              <button
                type="button"
                disabled={busy}
                onClick={() => {
                  setConfirmOpen(false);
                  onDelete();
                }}
                className={cn(
                  "rounded-pill border border-[color:var(--lens-alert)]/40 px-4 py-[7px]",
                  "bg-[color:var(--lens-alert)]/10 text-[13px] font-semibold text-[color:var(--lens-alert)]",
                  "outline-none transition-colors hover:bg-[color:var(--lens-alert)]/15",
                  "focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-50",
                )}
              >
                Delete thread
              </button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  );
}

function threadActionClass(intent: "normal" | "danger" = "normal") {
  return cn(
    "grid h-[28px] w-[28px] place-items-center rounded-btn border outline-none transition-colors",
    "focus-visible:ring-2 focus-visible:ring-white/20 disabled:cursor-not-allowed disabled:opacity-45",
    intent === "danger"
      ? "border-[color:var(--lens-alert)]/30 text-[color:var(--lens-alert)] hover:bg-[color:var(--lens-alert)]/10"
      : "border-border-subtle text-text-muted hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover hover:text-text-normal",
  );
}
