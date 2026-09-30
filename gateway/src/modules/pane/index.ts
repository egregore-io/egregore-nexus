// Pane module barrel — the conversation pane (head + thread + composer), the
// Pub/Admin views, the context-panel views, and the live read-view data hooks.
export { PaneHead } from "./PaneHead";
export type { PaneHeadProps } from "./PaneHead";
export { Thread } from "./Thread";
export type { ThreadProps } from "./Thread";
export { Composer } from "./Composer";
export type { ComposerProps } from "./Composer";
export { ConversationPane, LiveConversationPane, LiveChannelPane, MissingPane } from "./ConversationPane";
export type { ConversationPaneProps, LiveConversationPaneProps, LiveChannelPaneProps } from "./ConversationPane";
export type { ConversationView } from "./conversationView";
export {
  useAguiConversation,
  useAgentSession,
  reduceAguiEvents,
  newConversationState,
  parseAguiData,
  openAguiSource,
  observeSessionUrl,
  observeWebSocketUrl,
  resolveAguiTransport,
} from "./aguiConversation";
export type {
  AguiAgent,
  AguiConversationState,
  AguiEventSource,
  AguiInputFrame,
  AguiTransport,
  UseAguiConversationOptions,
  UseAgentSessionOptions,
} from "./aguiConversation";
export { useGatewayMessageHistory, useGatewayMessageHistory as useMessagePosts } from "./messageHistory";
export type {
  GatewayHistoryCursor,
  GatewayHistoryLoader,
  GatewayHistoryRow,
  GatewayMessagePoster,
  UseGatewayMessageHistoryOptions,
  UseGatewayMessageHistoryOptions as UseMessagePostsOptions,
} from "./messageHistory";
export { PubView } from "./PubView";
export { AdminView } from "./AdminView";
export { SourcesView } from "./SourcesView";
export {
  MembersContext,
  AgentContext,
  PubContext,
  AdminContext,
} from "./ContextViews";

export {
  useMembers,
  useThreads,
  useNotifications,
  useRoutingRules,
  usePubFeed,
  useAdminAgents,
  useSpawnAgent,
  useAgentOp,
  useGrantTier,
  useAddThreadMember,
  useRemoveThreadMember,
  useRenameThread,
  useArchiveThread,
  useDeleteThread,
  useRoster,
  useAgentFacts,
  usePubRuleFacts,
  useTierFacts,
  useChannelView,
  useDmView,
  useAgentSessionView,
  useSources,
  useRegisterSource,
  useEnableSource,
  useDisableSource,
  useRotateSource,
  useRemoveSource,
} from "./liveData";
export type { FeedRow, AdminRow, MemberItem, Fact } from "./liveData";

export * from "./types";
