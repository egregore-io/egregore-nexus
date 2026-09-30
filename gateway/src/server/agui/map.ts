// the pure AG-UI mappers (no React, no I/O).
//
// This file now holds only the IN mapping (`runInputToSend`) and re-exports the
// two lane renderers from their dedicated modules:
//   - `mapMessage.ts`     — Lane A: `messageToAguiEvents` (Message → AG-UI)
//   - `mapAgentUpdate.ts` — Lane B: `acpToAguiEvents` + bracket machinery
//
// IN: `runInputToSend(input, target)` turns an AG-UI `RunAgentInput` into a
// contract `SendRequest` (latest user message → `{ to:<target>, body:<text> }`).
//
// The event vocabulary is byte-compatible with egregore-lens's `aguiReader.ts`:
// TEXT_MESSAGE_START/CONTENT/END, REASONING_MESSAGE_START/CONTENT/END,
// TOOL_CALL_START/ARGS/RESULT. `plan` has no native AG-UI event → STATE_DELTA (a
// JSON-Patch the reader treats as a state event); `commands` → CUSTOM
// `nexus.commands` (the reader ignores CUSTOM names other than `lens.component`,
// so nothing is dropped). Run lifecycle (RUN_STARTED/FINISHED/ERROR) is emitted
// by the endpoint orchestrator, not here.
import type { RunAgentInput } from "@ag-ui/client";
import type { SendRequest, SendTarget } from "@shared/types";
import { assertMessageBody } from "@server/messagePost/validate";

// Re-export lane renderers so existing importers of `@server/agui/map` continue
// to resolve without changes until they are individually updated.
export type { AgentUpdateEvent, AguiBracket } from "@server/agui/mapAgentUpdate";
export {
  acpToAguiEvents,
  closeRun,
  newBracket,
  NEXUS_COMMANDS_EVENT,
} from "@server/agui/mapAgentUpdate";
export { messageToAguiEvents } from "@server/agui/mapMessage";

// --- helpers ----------------------------------------------------------------

const asRecord = (v: unknown): Record<string, unknown> =>
  typeof v === "object" && v !== null ? (v as Record<string, unknown>) : {};

const str = (v: unknown): string | undefined =>
  typeof v === "string" ? v : undefined;

/** Extract plain text from an AG-UI user-message `content` (string | parts[]). */
function userText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        const p = asRecord(part);
        return p.type === "text" ? (str(p.text) ?? "") : "";
      })
      .join("");
  }
  return "";
}

/**
 * Map an AG-UI `RunAgentInput` → a contract `SendRequest`: take the latest
 * `role:"user"` message and post its text to `target`. Throws if there is no
 * user message (an empty turn is a caller error, not a silent no-op).
 */
export function runInputToSend(
  input: RunAgentInput,
  target: SendTarget,
): SendRequest {
  const messages = input.messages ?? [];
  let latestUser: unknown;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (asRecord(messages[i]).role === "user") {
      latestUser = messages[i];
      break;
    }
  }
  if (latestUser === undefined) {
    throw new Error("runInputToSend: no user message in RunAgentInput");
  }
  const body = userText(asRecord(latestUser).content);
  assertMessageBody(body);
  return { to: target, body };
}
