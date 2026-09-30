// App providers — mounted once in the root route.
//
// - QueryClientProvider: server-state cache (display reads land here; committed
//   bus/session events patch/invalidate it once realtime is wired). Created per-render-tree via
//   useState so SSR and the client each get a stable, isolated client.
// - TooltipProvider: a single Radix tooltip context for the whole app (shared
//   open-delay + skip-delay behaviour).
//
// Theme is CSS-first (tokens.css imported at the root), so there is no JS theme
// provider here — a user stylesheet re-skins by overriding `--lens-*` tokens.
import { QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import type { ReactNode } from "react";

import { TooltipProvider } from "@shared/ui";
import { qk } from "@shared/queryKeys";
import { gatewayUrl } from "@app/gatewayClient";

const FLEET_STATUS_TOPIC = "sys.fleet.status";
const RECONNECT_MS = 2_000;

/**
 * Keep the canonical member cache aligned with daemon-owned fleet events.
 *
 * The socket is presentation-only: it never owns lifecycle state and a dropped
 * connection simply falls back to the bounded visible-tab refresh in
 * `liveData.ts`.
 */
export function GatewayEventInvalidator() {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (typeof window === "undefined" || typeof WebSocket === "undefined") return;

    let stopped = false;
    let socket: WebSocket | undefined;
    let reconnect: ReturnType<typeof setTimeout> | undefined;

    const connect = () => {
      if (stopped) return;
      const edge = new URL(gatewayUrl("/api/agui/ws"), window.location.href);
      edge.protocol = edge.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(edge.toString());
      socket.addEventListener("open", () => {
        socket?.send(JSON.stringify({
          t: "subscribe",
          topic: FLEET_STATUS_TOPIC,
          afterSeq: 0,
        }));
      });
      socket.addEventListener("message", (message) => {
        try {
          const frame = JSON.parse(String(message.data)) as {
            type?: string;
            event?: { topic?: string };
          };
          if (frame.type !== "developer.event" || frame.event?.topic !== FLEET_STATUS_TOPIC) {
            return;
          }
          void queryClient.invalidateQueries({ queryKey: qk.members("all") });
        } catch {
          // Ignore malformed frames; the bounded fallback read remains authoritative.
        }
      });
      socket.addEventListener("close", () => {
        if (!stopped) reconnect = setTimeout(connect, RECONNECT_MS);
      });
    };

    connect();
    return () => {
      stopped = true;
      if (reconnect) clearTimeout(reconnect);
      socket?.close();
    };
  }, [queryClient]);

  return null;
}

function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Realtime invalidation drives freshness; avoid noisy refetch storms.
        staleTime: 30_000,
        refetchOnWindowFocus: false,
        retry: 1,
      },
    },
  });
}

export function AppProviders({ children }: { children: ReactNode }) {
  const [queryClient] = useState(makeQueryClient);
  return (
    <QueryClientProvider client={queryClient}>
      <GatewayEventInvalidator />
      <TooltipProvider delayDuration={200} skipDelayDuration={300}>
        {children}
      </TooltipProvider>
    </QueryClientProvider>
  );
}
