// Facepile — an overlapping stack of small member faces with presence dots
// (prototype `.facepile`/`.face`). Used in the pane head. Tokens only.
import type { PresenceValue } from "../components/PresenceDot";
import { cn } from "../cn";

export interface Face {
  /** Single-character glyph shown in the face. */
  glyph: string;
  presence?: PresenceValue;
}

export interface FacepileProps {
  faces: Face[];
  className?: string;
  "aria-label"?: string;
}

const dotColor: Record<string, string> = {
  online: "bg-online",
  busy: "bg-busy",
  offline: "bg-offline",
};

export function Facepile({
  faces,
  className,
  "aria-label": ariaLabel,
}: FacepileProps) {
  return (
    <span
      className={cn("inline-flex", className)}
      aria-label={ariaLabel ?? `${faces.length} members`}
    >
      {faces.map((f, i) => (
        <span
          key={i}
          className={cn(
            "relative grid h-[22px] w-[22px] place-items-center rounded-full",
            "border-[1.5px] border-bg-primary bg-surface-raised",
            "text-[10px] font-semibold text-text-read",
            i === 0 ? "ml-0" : "-ml-1.5",
          )}
        >
          {f.glyph}
          <span
            className={cn(
              "absolute -bottom-px -right-px h-[7px] w-[7px] rounded-full border-[1.5px] border-bg-primary",
              dotColor[String(f.presence ?? "offline")] ?? "bg-offline",
            )}
          />
        </span>
      ))}
    </span>
  );
}
