import type { SessionFanoutFrame } from "./sessionFanout";

export interface NexusViewFrame {
  cursor: string;
  epoch: string;
  event: unknown;
}

export function nexusView(frame: SessionFanoutFrame): NexusViewFrame | null {
  if (frame.lane === "terminal") return null;
  if (frame.lane === "gap") {
    return { cursor: frame.cursor, epoch: frame.epoch, event: { type: "resync", reason: frame.reason } };
  }
  return { cursor: frame.cursor, epoch: frame.epoch, event: frame.event };
}
