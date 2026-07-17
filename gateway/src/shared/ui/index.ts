// ui-kit barrel — Obsidian Void primitives + the shared conversation timeline.
// Import from `@shared/ui` (never reach into individual files from feature code).
export { cn } from "./cn";
export { lensTokens, lensVar } from "./tailwind-preset";
export type { LensTokenName } from "./tailwind-preset";

export { Button } from "./components/Button";
export type { ButtonProps, ButtonVariant, ButtonSize } from "./components/Button";

export { IconButton } from "./components/IconButton";
export type {
  IconButtonProps,
  IconButtonVariant,
  IconButtonSize,
} from "./components/IconButton";

export { Surface, Panel } from "./components/Surface";
export type {
  SurfaceProps,
  SurfaceTone,
  PanelProps,
} from "./components/Surface";

export { SidebarItem, sidebarItemClasses } from "./components/SidebarItem";
export type {
  SidebarItemProps,
  SidebarItemVisualOptions,
} from "./components/SidebarItem";

export { NexusMark, NexusLoader } from "./components/NexusMark";
export type { NexusMarkProps, NexusLoaderProps } from "./components/NexusMark";

export { PresenceDot } from "./components/PresenceDot";
export type { PresenceDotProps, PresenceValue } from "./components/PresenceDot";

export { Avatar } from "./components/Avatar";
export type { AvatarProps, AvatarSize } from "./components/Avatar";

export { Badge } from "./components/Badge";
export type { BadgeProps, BadgeTone } from "./components/Badge";

export { Tooltip, TooltipProvider } from "./components/Tooltip";
export type { TooltipProps } from "./components/Tooltip";

export { ScrollArea } from "./components/ScrollArea";
export type { ScrollAreaProps } from "./components/ScrollArea";

export { MessageTimeline } from "./conversation/MessageTimeline";
export type { MessageTimelineProps } from "./conversation/MessageTimeline";

export { ProvenanceChip } from "./conversation/ProvenanceChip";
export type { ProvenanceChipProps } from "./conversation/ProvenanceChip";

export { Chip } from "./conversation/Chip";
export type { ChipProps, ChipVariant } from "./conversation/Chip";

export { Facepile } from "./conversation/Facepile";
export type { FacepileProps, Face } from "./conversation/Facepile";

export {
  InlineCode,
  Mention,
  CodeBlock,
  StreamingIndicator,
  Caret,
  Thinking,
  ToolCall,
} from "./conversation/MessageParts";
export type {
  CodeBlockProps,
  ToolCallProps,
} from "./conversation/MessageParts";
