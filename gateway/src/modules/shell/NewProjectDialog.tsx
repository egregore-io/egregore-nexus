// NewProjectDialog — create a project group (a console-only bucket of threads). On create it
// selects the new project so the operator drops straight into it. Radix Dialog; tokens only,
// matching SettingsModal.
import * as Dialog from "@radix-ui/react-dialog";
import { useState } from "react";

import { useSetActiveProject } from "@app/activeProject";
import { useCreateProjectGroup } from "@app/projects";

export interface NewProjectDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function NewProjectDialog({ open, onOpenChange }: NewProjectDialogProps) {
  const [name, setName] = useState("");
  const setActiveProject = useSetActiveProject();
  const create = useCreateProjectGroup();

  function submit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || create.isPending) return;
    create.mutate(trimmed, {
      onSuccess: (project) => {
        setActiveProject(project.id);
        setName("");
        onOpenChange(false);
      },
    });
  }

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(o) => {
        if (!o) setName("");
        onOpenChange(o);
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />
        <Dialog.Content
          className={[
            "fixed left-1/2 top-1/2 z-[400] w-[420px] max-w-[calc(100vw-2rem)]",
            "-translate-x-1/2 -translate-y-1/2",
            "rounded-btn border border-border-subtle bg-bg-secondary",
            "shadow-[var(--lens-shadow-elevated)] p-6 outline-none",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          ].join(" ")}
        >
          <div className="mb-4 flex items-center justify-between">
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              New project
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

          <Dialog.Description className="mb-4 text-[12px] leading-relaxed text-text-muted">
            A project groups your threads. It lives only in this console — it does not change the
            agents or the bus.
          </Dialog.Description>

          <form onSubmit={submit}>
            <label className="mb-1.5 block text-[12px] font-medium text-text-muted">
              Project name
            </label>
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Ops cleanup"
              className="mb-4 w-full rounded-btn border border-border-subtle bg-surface-inset px-3 py-2 text-[13px] text-text-read outline-none placeholder:text-text-muted focus:border-[color:var(--lens-focus-halo)]"
            />
            {create.isError && (
              <p className="mb-3 text-[12px] text-alert">Could not create the project. Try again.</p>
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
                {create.isPending ? "Creating…" : "Create project"}
              </button>
            </div>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
