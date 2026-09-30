// Shell module barrel — the app frame + its parts.
export { AppShell } from "./AppShell";
export type { AppShellProps } from "./AppShell";
export { TopBar } from "./TopBar";
export type { TopBarProps } from "./TopBar";
export { Sidebar } from "./Sidebar/Sidebar";
export type { SidebarProps } from "./Sidebar/Sidebar";
export { ProjectSwitcher } from "./Sidebar/ProjectSwitcher";
export type { ProjectSwitcherProps } from "./Sidebar/ProjectSwitcher";
export { ContextPanelHost } from "./ContextPanelHost";
export type { ContextPanelHostProps } from "./ContextPanelHost";
export {
  ContextSlotProvider,
  ContextPanelContent,
} from "./contextSlot";
export * from "./nav";
