import type { HookDiagnosticsReader } from "../api/http";

let activeDiagnostics: HookDiagnosticsReader | undefined;

export function setGatewayHookDiagnostics(
  diagnostics: HookDiagnosticsReader | undefined,
): void {
  activeDiagnostics = diagnostics;
}

export function gatewayHookDiagnostics(): HookDiagnosticsReader | undefined {
  return activeDiagnostics;
}
