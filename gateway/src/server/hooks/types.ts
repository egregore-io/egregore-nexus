export type HookEventName = string;
export type HookFailurePolicy = "continue" | "reject";

export interface LocalHookHandlerManifest {
  kind: "local";
  entry: string;
  entrypoint: string;
  run: string[];
  passEnv: string[];
}

export interface HookManifest {
  version: 1;
  id: string;
  event: HookEventName;
  order: number;
  timeoutMs: number;
  onFailure: HookFailurePolicy;
  enabled: boolean;
  manifestPath: string;
  handler: LocalHookHandlerManifest;
}

export interface HookRegistrySnapshot {
  generation: string;
  hooks: readonly HookManifest[];
  createdAt: number;
}

export interface HookRegistryReload {
  activated: boolean;
  snapshot: HookRegistrySnapshot;
  errors: readonly string[];
}
