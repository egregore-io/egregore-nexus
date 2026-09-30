// Static v1 command registry for the AG-UI websocket command surface.
//
// SPEC-ws-full-command-access §13 (lens repo): one harness end-to-end (claude),
// static map only, `compact` is a gateway verb for EVERY harness (never
// advertised twice), flat args schema. The catalog is versioned by
// CATALOG_VERSION — bump it whenever a descriptor here changes so clients
// re-fetch instead of routing off a stale map.

export const CATALOG_VERSION = "v1";

// class=gateway verbs dispatch to these REST handlers through the same fetch
// handler the socket already uses for `input`.
const GATEWAY_VERB_ROUTES = {
  compact: "/api/conversation/compact",
};

const GATEWAY_COMMANDS = [
  {
    name: "compact",
    class: "gateway",
    title: "Compact context",
    description: "Trigger native context compaction on the target session.",
    invocation: "structured",
    needsTurn: true,
    args: [],
  },
];

// class=harness commands are rendered to the harness's native slash invocation
// and delivered ONCE through the prompt ingress (typed into a headed PTY by
// the daemon, exactly like operator passthrough text).
const HARNESS_COMMANDS = {
  claude: [
    {
      name: "model",
      class: "harness",
      title: "Set model",
      description: "Switch the model the harness runs on.",
      invocation: "structured",
      needsTurn: false,
      args: [{ name: "model", type: "string", required: true, enum: null }],
    },
    {
      name: "clear",
      class: "harness",
      title: "Clear context",
      description: "Clear the harness conversation context.",
      invocation: "structured",
      needsTurn: false,
      args: [],
    },
  ],
};

/**
 * The full catalog for one session. Unknown / unresolvable harnesses get the
 * gateway verbs only — discovery never hard-errors on an exotic session.
 */
export function catalogForHarness(harness) {
  const known = typeof harness === "string" && Object.hasOwn(HARNESS_COMMANDS, harness)
    ? harness
    : undefined;
  return {
    harness: known ?? (typeof harness === "string" && harness ? harness : null),
    catalogVersion: CATALOG_VERSION,
    commands: [...GATEWAY_COMMANDS, ...(known ? HARNESS_COMMANDS[known] : [])],
  };
}

/** Resolve one descriptor by name within a session's catalog. */
export function findCommand(harness, name) {
  return catalogForHarness(harness).commands.find((command) => command.name === name);
}

/** REST path for a gateway verb, or undefined for harness-class commands. */
export function gatewayVerbPath(name) {
  return GATEWAY_VERB_ROUTES[name];
}

/**
 * Validate an invocation args object against a descriptor's flat param list
 * (spec §13.3). Returns an error message, or undefined when valid.
 */
export function validateCommandArgs(descriptor, args) {
  const provided = args ?? {};
  if (typeof provided !== "object" || Array.isArray(provided)) {
    return "args must be a flat JSON object";
  }
  const params = new Map(descriptor.args.map((param) => [param.name, param]));
  for (const key of Object.keys(provided)) {
    if (!params.has(key)) return `unknown arg: ${key}`;
  }
  for (const param of descriptor.args) {
    const value = provided[param.name];
    if (value === undefined || value === null) {
      if (param.required) return `missing required arg: ${param.name}`;
      continue;
    }
    if (typeof value !== param.type) {
      return `arg ${param.name} must be a ${param.type}`;
    }
    if (typeof value === "string" && !value.trim()) {
      return `arg ${param.name} must not be empty`;
    }
    if (Array.isArray(param.enum) && param.enum.length > 0 && !param.enum.includes(value)) {
      return `arg ${param.name} must be one of: ${param.enum.join(", ")}`;
    }
  }
  return undefined;
}

/**
 * Render a harness-class command to its native slash invocation, args in
 * descriptor order. Callers validate first; this never re-checks.
 */
export function renderHarnessInvocation(descriptor, args) {
  const provided = args ?? {};
  const parts = [`/${descriptor.name}`];
  for (const param of descriptor.args) {
    const value = provided[param.name];
    if (value === undefined || value === null) continue;
    parts.push(String(value));
  }
  return parts.join(" ");
}
