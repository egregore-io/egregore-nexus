// SettingsModal — Appearance settings for the Nexus web console.
//
// Opened by clicking the Settings cog in the Sidebar "me" footer.
// Uses Radix Dialog for focus-trap + Escape handling.
//
// Appearance controls:
//   - Accent: radiogroup with swatches (mono | green | amber | ember)
//   - Backdrop dim: segmented control radiogroup (0.45 Light | 0.66 Medium | 0.85 Heavy)
//
// DOM pref application (accent + backdropDim) is owned by AppShell, not here.
import * as Dialog from "@radix-ui/react-dialog";

import { type Accent, type BackdropDim, useAppearance, useSetAppearance } from "@app/uiPrefs";

// --------------------------------------------------------------------------
// helpers

const ACCENTS: { value: Accent; label: string; cssVar: string }[] = [
  { value: "mono", label: "Monochrome", cssVar: "var(--lens-accent)" },
  { value: "green", label: "Green", cssVar: "var(--lens-online)" },
  { value: "amber", label: "Amber", cssVar: "var(--lens-busy)" },
  { value: "ember", label: "Ember", cssVar: "var(--lens-alert)" },
];

const DIM_OPTIONS: { value: BackdropDim; label: string }[] = [
  { value: 0.45, label: "Light" },
  { value: 0.66, label: "Medium" },
  { value: 0.85, label: "Heavy" },
];

// --------------------------------------------------------------------------
// SettingsContent — rendered inside the dialog

function SettingsContent() {
  const { accent, backdropDim } = useAppearance();
  const { setAccent, setBackdropDim } = useSetAppearance();

  // DOM side-effects (accent dataset + --lens-backdrop-dim) are handled
  // centrally by useApplyAppearance() in AppShell — call store setters only.
  function handleAccent(value: Accent) {
    setAccent(value);
  }

  function handleDim(value: BackdropDim) {
    setBackdropDim(value);
  }

  return (
    <div className="flex flex-col gap-6">
      {/* Appearance section */}
      <section>
        <h3 className="mb-3 text-[11px] font-semibold uppercase tracking-[0.06em] text-text-faint">
          Appearance
        </h3>

        {/* Accent */}
        <div className="mb-4 flex items-center gap-3">
          <span className="w-28 text-[13px] text-text-muted">Accent</span>
          <div
            role="radiogroup"
            aria-label="Accent colour"
            className="flex items-center gap-2"
          >
            {ACCENTS.map(({ value, label, cssVar }) => (
              <button
                key={value}
                type="button"
                role="radio"
                aria-checked={accent === value}
                aria-label={label}
                title={label}
                onClick={() => handleAccent(value)}
                className={[
                  "h-6 w-6 rounded-full border-2 outline-none transition-all",
                  "focus-visible:ring-2 focus-visible:ring-white/20",
                  accent === value
                    ? "border-text-normal scale-110"
                    : "border-transparent opacity-70 hover:opacity-100",
                ].join(" ")}
                style={{ "--sw": cssVar, backgroundColor: "var(--sw)" } as React.CSSProperties}
              />
            ))}
          </div>
        </div>

        {/* Backdrop dim */}
        <div className="flex items-center gap-3">
          <span className="w-28 text-[13px] text-text-muted">Backdrop dim</span>
          <div
            role="radiogroup"
            aria-label="Backdrop dim"
            className="flex overflow-hidden rounded-btn border border-border-subtle"
          >
            {DIM_OPTIONS.map(({ value, label }, idx) => (
              <button
                key={value}
                type="button"
                role="radio"
                aria-checked={backdropDim === value}
                onClick={() => handleDim(value)}
                className={[
                  "px-3 py-1 text-[12px] font-medium outline-none transition-colors",
                  "focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-white/20",
                  idx > 0 ? "border-l border-border-subtle" : "",
                  backdropDim === value
                    ? "bg-[color:var(--lens-fill-active)] text-text-normal"
                    : "text-text-muted hover:bg-[color:var(--lens-fill-hover)] hover:text-text-normal",
                ].join(" ")}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

// --------------------------------------------------------------------------
// SettingsModal — controlled open/close

export interface SettingsModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function SettingsModal({ open, onOpenChange }: SettingsModalProps) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        {/* Backdrop */}
        <Dialog.Overlay className="fixed inset-0 z-[400] bg-[color:var(--lens-scrim)] backdrop-blur-[1px]" />

        {/* Panel */}
        <Dialog.Content
          className={[
            "fixed left-1/2 top-1/2 z-[400] w-[440px] max-w-[calc(100vw-2rem)]",
            "-translate-x-1/2 -translate-y-1/2",
            "rounded-btn border border-border-subtle bg-bg-secondary",
            "shadow-[var(--lens-shadow-elevated)]",
            "p-6",
            "outline-none",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          ].join(" ")}
        >
          {/* Header */}
          <div className="mb-5 flex items-center justify-between">
            <Dialog.Title className="text-[15px] font-semibold text-text-normal">
              Settings
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

          {/* Body */}
          <SettingsContent />
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
