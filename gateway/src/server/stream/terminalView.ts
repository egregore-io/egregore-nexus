import type { SessionFanoutFrame } from "./sessionFanout";

export function terminalView(frame: SessionFanoutFrame) {
  if (frame.lane !== "terminal") return null;
  return {
    cursor: frame.cursor,
    epoch: frame.epoch,
    chunkBase64: frame.chunkBase64,
    encoding: frame.encoding,
  };
}
