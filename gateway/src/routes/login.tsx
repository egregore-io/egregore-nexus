// /login — name-picker form, shown to first-time or logged-out visitors.
// Styling mirrors SettingsModal / AdminView (Lens design tokens; Radix-free, minimal).
import { useState } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { gatewayFetch } from "@app/gatewayClient";

// ── LoginPage — exported for unit testing ────────────────────────────────────

export interface LoginPageProps {
  /** Called after a successful POST /api/login. */
  onSuccess: () => void;
}

export function LoginPage({ onSuccess }: LoginPageProps) {
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    if (!password) return;

    setPending(true);
    setError(null);

    try {
      const res = await gatewayFetch("/api/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: trimmed, password }),
      });

      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        setError(body.error ?? "Login failed — try again.");
        return;
      }

      onSuccess();
    } catch {
      setError("Network error — try again.");
    } finally {
      setPending(false);
    }
  }

  return (
    <div className="fixed inset-0 flex items-center justify-center bg-bg-primary">
      <div className="w-[400px] max-w-[calc(100vw-2rem)] rounded-btn border border-border-subtle bg-bg-secondary p-8 shadow-[var(--lens-shadow-elevated)]">
        <h1 className="mb-6 text-[15px] font-semibold text-text-normal">
          Enter the console
        </h1>

        <form onSubmit={handleSubmit} noValidate>
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="nexus-login-name"
              className="text-[12px] font-medium text-text-muted"
            >
              Name
            </label>
            <input
              id="nexus-login-name"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="your name"
              autoFocus
              className="w-full rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px] text-[13px] text-text-normal outline-none focus:ring-2 focus:ring-[color:var(--lens-focus-halo)]"
            />
          </div>
          <div className="mt-3 flex flex-col gap-1.5">
            <label
              htmlFor="nexus-login-password"
              className="text-[12px] font-medium text-text-muted"
            >
              Password
            </label>
            <input
              id="nexus-login-password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="password"
              className="w-full rounded-2 border border-border-subtle bg-bg-tertiary px-3 py-[7px] text-[13px] text-text-normal outline-none focus:ring-2 focus:ring-[color:var(--lens-focus-halo)]"
            />
          </div>

          {error && (
            <p role="alert" className="mt-2 text-[12px] text-red-400">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={pending}
            className="mt-4 w-full rounded-btn bg-[color:var(--lens-fill-active)] px-4 py-[9px] text-[13px] font-semibold text-white disabled:opacity-50"
          >
            {pending ? "Entering…" : "Enter"}
          </button>
        </form>
      </div>
    </div>
  );
}

// ── Route wrapper ─────────────────────────────────────────────────────────────

function LoginRoute() {
  const navigate = useNavigate();
  return (
    <LoginPage
      onSuccess={() => { void navigate({ to: "/" }); }}
    />
  );
}

export const Route = createFileRoute("/login")({
  component: LoginRoute,
});
