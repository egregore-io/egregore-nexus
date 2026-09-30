import type { BaseEvent } from "@ag-ui/client";

import { runFinished, runStarted, agentSessionThreadId } from "../agui/_sseCore";
import {
  acpToAguiEvents,
  closeRun,
  newBracket,
  type AgentUpdateEvent,
  type AguiBracket,
} from "../agui/mapAgentUpdate";
import type { SessionFanoutFrame } from "./sessionFanout";

export interface AguiViewFrame {
  cursor: string;
  epoch: string;
  events: BaseEvent[];
}

export class AguiSessionView {
  private bracket: AguiBracket = newBracket();
  private open = false;
  private runId = "";

  constructor(private readonly sessionId: string) {}

  project(frame: SessionFanoutFrame): AguiViewFrame | null {
    if (frame.lane === "terminal") return null;
    if (frame.lane === "gap") {
      this.open = false;
      this.bracket = newBracket();
      return { cursor: frame.cursor, epoch: frame.epoch, events: [] };
    }
    const update = frame.event as AgentUpdateEvent;
    const events: BaseEvent[] = [];
    if (update.kind === "turn_end") {
      if (this.open) {
        events.push(...closeRun(this.bracket), runFinished(agentSessionThreadId(this.sessionId), this.runId));
        this.open = false;
        this.bracket = newBracket();
      }
      return { cursor: frame.cursor, epoch: frame.epoch, events };
    }
    if (!this.open) {
      this.open = true;
      this.runId = `turn_${this.sessionId}_${frame.id}`;
      events.push(runStarted(agentSessionThreadId(this.sessionId), this.runId));
    }
    const mapped = acpToAguiEvents(update, this.bracket);
    this.bracket = mapped.bracket;
    events.push(...mapped.events);
    return { cursor: frame.cursor, epoch: frame.epoch, events };
  }
}
