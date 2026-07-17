// SourcesView — notification sources management panel.
//
// Matches the AdminView layout (table + action bar). Shows: source name,
// topic, enabled state, last-fired relative time. Per-row actions: enable/
// disable toggle, rotate token, remove. A register form adds new sources.
// Token reveal: after register or rotate the plaintext token is shown once
// (copy-to-clipboard). A "copy recipe" button per row copies the push CLI
// snippet so the operator can wire a producer immediately.
import { useState } from "react";

import { cn } from "@shared/ui";

import { PaneHead } from "./PaneHead";
import {
  useSources,
  useRegisterSource,
  useEnableSource,
  useDisableSource,
  useRotateSource,
  useRemoveSource,
} from "./liveData";

// ── token reveal banner ───────────────────────────────────────────────────────

interface TokenRevealProps {
  label: string;
  token: string;
  onDismiss: () => void;
}

function TokenReveal({ label, token, onDismiss }: TokenRevealProps) {
  const [copied, setCopied] = useState(false);

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(token);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard not available in test env
    }
  }

  return (
    <div
      role="status"
      aria-live="polite"
      className={cn(
        "mb-4 rounded-btn border border-border-subtle bg-bg-tertiary px-4 py-3",
        "text-[13px]",
      )}
    >
      <div className="mb-1.5 flex items-center justify-between gap-3">
        <span className="font-semibold text-text-normal">{label}</span>
        <button
          type="button"
          aria-label="Dismiss token"
          onClick={onDismiss}
          className="text-[11px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
        >
          Dismiss
        </button>
      </div>
      <p className="mb-2 text-[12px] text-text-muted">
        Store this token now — it will NOT be shown again.
      </p>
      <div className="flex items-center gap-2">
        <code className="min-w-0 flex-1 overflow-x-auto rounded-2 border border-border-subtle bg-bg-secondary px-2 py-1 font-mono text-[12px] text-text-normal">
          {token}
        </code>
        <button
          type="button"
          onClick={() => void handleCopy()}
          className={cn(
            "shrink-0 rounded-pill border border-border-subtle px-3 py-[5px]",
            "text-[12px] font-semibold outline-none transition-colors",
            copied
              ? "bg-[color:var(--lens-fill-active)] text-text-normal"
              : "text-text-muted hover:border-[color:var(--lens-focus-halo)] hover:text-text-normal",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          )}
        >
          {copied ? "Copied!" : "Copy"}
        </button>
      </div>
    </div>
  );
}

// ── register form ─────────────────────────────────────────────────────────────

interface RegisterFormProps {
  onRegister: (name: string, topic?: string) => void;
  isPending: boolean;
}

function RegisterForm({ onRegister, isPending }: RegisterFormProps) {
  const [name, setName] = useState("");
  const [topic, setTopic] = useState("");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimName = name.trim();
    if (!trimName) return;
    onRegister(trimName, topic.trim() || undefined);
    setName("");
    setTopic("");
  }

  return (
    <form
      onSubmit={handleSubmit}
      aria-label="Register source"
      className="mb-5 flex flex-wrap items-end gap-3"
    >
      <div className="flex flex-col gap-1">
        <label
          htmlFor="src-name"
          className="text-[12px] font-semibold text-text-muted"
        >
          Source name
        </label>
        <input
          id="src-name"
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="github-ci"
          required
          className={cn(
            "w-[180px] rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px]",
            "text-[13px] text-text-normal placeholder:text-text-faint outline-none transition-colors",
            "hover:border-[color:var(--lens-focus-halo)] focus-visible:ring-2 focus-visible:ring-white/20",
          )}
        />
      </div>
      <div className="flex flex-col gap-1">
        <label
          htmlFor="src-topic"
          className="text-[12px] font-semibold text-text-muted"
        >
          Topic{" "}
          <span className="font-normal text-text-faint">(optional)</span>
        </label>
        <input
          id="src-topic"
          type="text"
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          placeholder="auto"
          className={cn(
            "w-[150px] rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px]",
            "text-[13px] text-text-normal placeholder:text-text-faint outline-none transition-colors",
            "hover:border-[color:var(--lens-focus-halo)] focus-visible:ring-2 focus-visible:ring-white/20",
          )}
        />
      </div>
      <button
        type="submit"
        disabled={isPending}
        className={cn(
          "rounded-pill border border-border-subtle bg-[color:var(--lens-fill-active)] px-4 py-[7px]",
          "text-[13px] font-semibold text-text-normal outline-none transition-colors",
          "hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover focus-visible:ring-2 focus-visible:ring-white/20",
          "disabled:opacity-40 disabled:cursor-not-allowed",
        )}
      >
        + Register
      </button>
    </form>
  );
}

// ── copy recipe helper ────────────────────────────────────────────────────────

function buildRecipe(sourceName: string): string {
  return `nexus push ${sourceName} -m "your message here"`;
}

function CopyRecipeButton({ sourceName }: { sourceName: string }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(buildRecipe(sourceName));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // clipboard unavailable
    }
  }

  return (
    <button
      type="button"
      aria-label={`Copy recipe for ${sourceName}`}
      onClick={() => void handleCopy()}
      title={buildRecipe(sourceName)}
      className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
    >
      {copied ? "Copied!" : "Copy recipe"}
    </button>
  );
}

// ── SourcesView ───────────────────────────────────────────────────────────────

export function SourcesView() {
  const { sources, isLoading } = useSources();
  const register = useRegisterSource();
  const enable = useEnableSource();
  const disable = useDisableSource();
  const rotate = useRotateSource();
  const remove = useRemoveSource();

  // Token reveal: after register or rotate show the token once.
  const [revealedToken, setRevealedToken] = useState<{
    label: string;
    token: string;
  } | null>(null);

  function handleRegister(name: string, topic?: string) {
    register.mutate(
      { name, topic },
      {
        onSuccess: (data) => {
          setRevealedToken({
            label: `Token for "${data.source.name}"`,
            token: data.token,
          });
        },
      },
    );
  }

  function handleRotate(name: string) {
    rotate.mutate(
      { name },
      {
        onSuccess: (data) => {
          setRevealedToken({
            label: `New token for "${data.name}"`,
            token: data.token,
          });
        },
      },
    );
  }

  const headers = ["Name", "Topic", "Enabled", "Last fired", ""];

  return (
    <section className="flex min-h-0 flex-1 flex-col">
      <PaneHead title="Sources" glyph="⇥" topic="notification sources · tokens" />
      <div className="mx-auto w-full max-w-[70rem] flex-1 overflow-y-auto px-6 py-[18px] lens-scroll">
        <div className="mb-3.5 flex items-center justify-between">
          <h2 className="text-[15px] font-semibold text-text-normal">Sources</h2>
        </div>

        {/* Token reveal banner */}
        {revealedToken && (
          <TokenReveal
            label={revealedToken.label}
            token={revealedToken.token}
            onDismiss={() => setRevealedToken(null)}
          />
        )}

        {/* Register form */}
        <RegisterForm
          onRegister={handleRegister}
          isPending={register.isPending}
        />

        {/* Sources table */}
        {isLoading ? null : sources.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-2 rounded-btn border border-border-subtle px-6 py-14 text-center text-text-muted">
            <h3 className="text-[15px] font-semibold text-text-normal">
              No sources yet
            </h3>
            <p className="max-w-[44ch] text-[13px]">
              Register a source above, then use the token to push events via{" "}
              <code className="font-mono text-[12px]">nexus push</code>.
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
              {sources.map((src, i) => {
                const last = i === sources.length - 1;
                const cell = "px-3.5 py-2.5 text-[13px] text-text-read";
                return (
                  <tr
                    key={src.name}
                    className="hover:[&>td]:bg-[color:var(--lens-fill-hover)]"
                  >
                    {/* Name */}
                    <td
                      className={cn(
                        cell,
                        "font-semibold text-text-normal",
                        !last && "border-b border-border-subtle",
                      )}
                    >
                      {src.name}
                    </td>

                    {/* Topic */}
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>
                      <span className="rounded-2 border border-border-subtle px-[7px] py-px text-[11px] text-text-read">
                        {src.topic}
                      </span>
                    </td>

                    {/* Enabled */}
                    <td className={cn(cell, !last && "border-b border-border-subtle")}>
                      <span
                        className={cn(
                          "rounded-2 border px-[7px] py-px text-[11px]",
                          src.enabled
                            ? "border-border-subtle text-text-read"
                            : "border-border-subtle text-text-muted",
                        )}
                      >
                        {src.enabled ? "enabled" : "disabled"}
                      </span>
                    </td>

                    {/* Last fired */}
                    <td className={cn(cell, "text-text-muted", !last && "border-b border-border-subtle")}>
                      {src.lastFiredAt ? timeAgo(src.lastFiredAt) : "—"}
                    </td>

                    {/* Actions */}
                    <td
                      className={cn(
                        "px-3.5 py-2.5 text-[13px]",
                        !last && "border-b border-border-subtle",
                      )}
                    >
                      <span className="flex items-center gap-3">
                        {/* Enable/disable toggle */}
                        {src.enabled ? (
                          <button
                            type="button"
                            aria-label={`Disable ${src.name}`}
                            onClick={() => disable.mutate({ name: src.name })}
                            className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                          >
                            Disable
                          </button>
                        ) : (
                          <button
                            type="button"
                            aria-label={`Enable ${src.name}`}
                            onClick={() => enable.mutate({ name: src.name })}
                            className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                          >
                            Enable
                          </button>
                        )}

                        {/* Rotate */}
                        <button
                          type="button"
                          aria-label={`Rotate ${src.name}`}
                          onClick={() => handleRotate(src.name)}
                          className="text-[12px] text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
                        >
                          Rotate
                        </button>

                        {/* Copy recipe */}
                        <CopyRecipeButton sourceName={src.name} />

                        {/* Remove */}
                        <button
                          type="button"
                          aria-label={`Remove ${src.name}`}
                          onClick={() => remove.mutate({ name: src.name })}
                          className={cn(
                            "text-[12px] font-semibold text-[color:var(--lens-alert)] underline-offset-2 outline-none transition-colors",
                            "hover:underline focus-visible:ring-2 focus-visible:ring-white/20",
                          )}
                        >
                          Remove
                        </button>
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

// Inline relative-time formatter — reuses the same logic as liveData.ts's timeAgo.
// Kept here so SourcesView is self-contained; the identical impl in liveData
// is not exported (it's a module-private helper there).
function timeAgo(when: number | undefined): string {
  if (!when) return "";
  const secs = Math.max(0, Math.round((Date.now() - when) / 1000));
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.round(hrs / 24)}d ago`;
}
