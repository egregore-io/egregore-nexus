// ProjectSwitcher — presentational selector for console project groups.
//
// Appearance: a compact pill — status dot + project name + a
// chevron — rendered next to the brand. The Radix dropdown lists the supplied
// groups and an "All threads" option. The parent owns the active selection;
// this component reports selection and add-project actions through callbacks.
// Tokens only.
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";

import { PresenceDot, cn } from "@shared/ui";

import { type ProjectNavItem } from "../nav";

export interface ProjectSwitcherProps {
  /** The project GROUPS the operator has created (console-only). Empty → only "All threads". */
  projects?: ProjectNavItem[];
  /** The active group id, or undefined for "All threads". */
  activeProjectId?: string;
  /** Select a group by id, or "" for "All threads". */
  onSelect?: (projectId: string) => void;
  /** Open the "new project" dialog. */
  onAddProject?: () => void;
}

export function ProjectSwitcher({
  projects = [],
  activeProjectId,
  onSelect,
  onAddProject,
}: ProjectSwitcherProps) {
  const current = activeProjectId
    ? projects.find((p) => p.projectId === activeProjectId)
    : undefined;

  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger asChild>
        <button
          type="button"
          aria-label="Switch project"
          aria-haspopup="listbox"
          className={cn(
            "flex items-center gap-2 rounded-row px-2.5 py-[5px]",
            "border border-border-subtle bg-[color:var(--lens-fill-active)]",
            "text-[13px] font-semibold text-text-normal outline-none transition-colors",
            "hover:bg-bg-hover focus-visible:ring-2 focus-visible:ring-white/20",
            "data-[state=open]:bg-bg-hover",
          )}
        >
          <PresenceDot presence={current?.presence ?? "online"} size="sm" />
          <span className="max-w-[14rem] truncate">
            {current?.name ?? "All threads"}
          </span>
          <Chevron />
        </button>
      </DropdownMenu.Trigger>

      <DropdownMenu.Portal>
        <DropdownMenu.Content
          align="start"
          sideOffset={6}
          className={cn(
            "z-[200] min-w-[var(--radix-dropdown-menu-trigger-width)] max-w-[20rem]",
            "rounded-btn border border-border-subtle bg-bg-overlay p-1",
            "shadow-[var(--lens-shadow-elevated)]",
          )}
        >
          <DropdownMenu.Label className="px-2 py-1.5 text-[10px] font-semibold uppercase tracking-wide text-text-faint">
            Projects
          </DropdownMenu.Label>

          <DropdownMenu.Item
            onSelect={() => onSelect?.("")}
            className={cn(
              "flex h-8 cursor-pointer items-center gap-2 rounded-row px-2 text-[13px] outline-none",
              "text-text-muted data-[highlighted]:bg-[color:var(--lens-fill-active)] data-[highlighted]:text-text-normal",
              !activeProjectId && "text-text-normal",
            )}
          >
            <span className="grid h-2.5 w-2.5 place-items-center text-text-faint">∗</span>
            <span className="min-w-0 flex-1 truncate">All threads</span>
            {!activeProjectId && <Check />}
          </DropdownMenu.Item>

          {projects.map((p) => {
            const active = p.projectId === activeProjectId;
            return (
              <DropdownMenu.Item
                key={p.projectId}
                onSelect={() => onSelect?.(p.projectId)}
                className={cn(
                  "flex h-8 cursor-pointer items-center gap-2 rounded-row px-2 text-[13px] outline-none",
                  "text-text-muted data-[highlighted]:bg-[color:var(--lens-fill-active)] data-[highlighted]:text-text-normal",
                  active && "text-text-normal",
                )}
              >
                <PresenceDot presence={p.presence} size="sm" />
                <span className="min-w-0 flex-1 truncate">{p.name}</span>
                {active && <Check />}
              </DropdownMenu.Item>
            );
          })}

          {onAddProject && (
            <>
              <DropdownMenu.Separator className="my-1 h-px bg-border-subtle" />
              <DropdownMenu.Item
                onSelect={() => onAddProject()}
                className={cn(
                  "flex h-8 cursor-pointer items-center gap-2 rounded-row px-2 text-[13px] outline-none",
                  "text-text-muted data-[highlighted]:bg-[color:var(--lens-fill-active)] data-[highlighted]:text-text-normal",
                )}
              >
                <span className="grid h-3.5 w-3.5 place-items-center text-[14px] leading-none text-text-faint">
                  +
                </span>
                <span className="min-w-0 flex-1 truncate">New project</span>
              </DropdownMenu.Item>
            </>
          )}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}

function Chevron() {
  return (
    <svg
      viewBox="0 0 12 12"
      aria-hidden="true"
      className="h-3 w-3 text-text-muted"
    >
      <path
        d="M3 4.5 6 7.5 9 4.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function Check() {
  return (
    <svg viewBox="0 0 12 12" aria-hidden="true" className="h-3 w-3 shrink-0 text-text-normal">
      <path
        d="M2.5 6.5 5 9l4.5-5.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
