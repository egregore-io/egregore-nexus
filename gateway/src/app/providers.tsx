// App providers — mounted once in the root route.
//
// - QueryClientProvider: server-state cache (display reads land here; committed
//   mutations invalidate it). Created per-render-tree via
//   useState so SSR and the client each get a stable, isolated client.
// - TooltipProvider: a single Radix tooltip context for the whole app (shared
//   open-delay + skip-delay behaviour).
//
// Theme is CSS-first (tokens.css imported at the root), so there is no JS theme
// provider here — a user stylesheet re-skins by overriding `--lens-*` tokens.
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import type { ReactNode } from "react";

import { TooltipProvider } from "@shared/ui";

function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Individual read hooks opt into bounded HTTP refresh as needed.
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
      <TooltipProvider delayDuration={200} skipDelayDuration={300}>
        {children}
      </TooltipProvider>
    </QueryClientProvider>
  );
}
