// ProvenanceChip — renders the web console message duality (spec §6):
//   "from <sender> · via #<thread>"  (or "· via @<topic>" for pub).
//
// Provenance is the in-band sender tag carried on every Message. The chip is a
// quiet, monochrome metadata line — it must not compete with the message body.
import type { HTMLAttributes } from "react";

import type { Provenance } from "@shared/types";

import { cn } from "../cn";

export interface ProvenanceChipProps extends HTMLAttributes<HTMLSpanElement> {
  provenance: Provenance;
}

export function ProvenanceChip({
  provenance,
  className,
  ...rest
}: ProvenanceChipProps) {
  const { from, thread, topic } = provenance;
  const via = thread ? `#${thread}` : topic ? `@${topic}` : null;
  return (
    <span
      className={cn(
        "inline-flex items-baseline gap-1 text-[11px] leading-none text-text-muted",
        className,
      )}
      {...rest}
    >
      <span className="font-semibold text-text-normal/90">{from}</span>
      {via && (
        <>
          <span aria-hidden="true" className="opacity-50">
            ·
          </span>
          <span className="opacity-90">
            via <span className="font-medium">{via}</span>
          </span>
        </>
      )}
    </span>
  );
}
