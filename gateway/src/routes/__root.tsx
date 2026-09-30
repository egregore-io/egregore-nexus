import {
  HeadContent,
  Outlet,
  Scripts,
  createRootRoute,
  useNavigate,
  useRouterState,
} from "@tanstack/react-router";
import type { ReactNode } from "react";
import { useEffect } from "react";

// Obsidian Void tokens + Tailwind entry. Importing here loads the stylesheet
// into every route (the Vite/Tailwind plugin turns this into the app CSS).
import "@shared/ui/tokens.css";

import { PRE_PAINT_SRC } from "@app/prePaint";
import { gatewayFetch } from "@app/gatewayClient";
import { AppProviders } from "@app/providers";
import { AppShell } from "@modules/shell";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Nexus — web console" },
    ],
    // Fonts: Inter (UI) + JetBrains Mono (code/kbd/toolcalls).
    links: [
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      {
        rel: "preconnect",
        href: "https://fonts.gstatic.com",
        crossOrigin: "anonymous",
      },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap",
      },
    ],
    scripts: [
      { children: PRE_PAINT_SRC },
      // The canonical Nexus mark renderer (vendored verbatim from the design
      // export). Powers <NexusMark/> (static, randomized per page load) and
      // <NexusLoader/> (travel variant, loading states only).
      { src: "/nexus-mark.js", defer: true },
    ],
  }),
  component: RootComponent,
});

/**
 * Auth gate: on every root mount, check whether the visitor has a valid
 * nexus_human session (GET /api/login → 200) and redirect to /login if not.
 * The check is a client-side effect so it doesn't block SSR; /login itself is
 * always reachable (we bail if already there).
 */
function AuthGate() {
  const navigate = useNavigate();

  useEffect(() => {
    // Only run in the browser (useEffect never runs on the server).
    if (window.location.pathname === "/login") return;

    void gatewayFetch("/api/login", { method: "GET", credentials: "include" })
      .then((res) => {
        if (res.status === 401) {
          void navigate({ to: "/login" });
        }
      })
      .catch(() => {
        // Network error: don't redirect (offline / dev with no server); fail open.
      });
  }, [navigate]);

  return null;
}

function RootComponent() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const isLogin = pathname === "/login";

  return (
    <RootDocument>
      <div className="app-root" data-testid="app-root">
        <AppProviders>
          <AuthGate />
          {isLogin ? <Outlet /> : <AppShell />}
        </AppProviders>
      </div>
    </RootDocument>
  );
}

function RootDocument({ children }: { children: ReactNode }) {
  if (import.meta.env.NEXUS_WEBUI_STANDALONE === "true") return <>{children}</>;
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}
