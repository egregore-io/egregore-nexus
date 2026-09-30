// IndexView — the web console landing at `/`.
//
// The `-` filename prefix tells TanStack Router this is NOT a route module, so it
// is excluded from the generated route tree and the route-file transform —
// keeping the component unit-testable in isolation (jsdom).
//
// The landing opens on #backend: it renders that channel's conversation pane
// (live AG-UI `observe` stream — empty until a turn arrives) with the live
// Members ctx-view. No seed.
import {
  LiveChannelPane,
  MembersContext,
  useChannelView,
  useRoster,
} from "@modules/pane";
import { ContextPanelContent } from "@modules/shell";

export function IndexView() {
  const view = useChannelView("backend");
  const { members } = useRoster("backend");
  return (
    <>
      <LiveChannelPane view={view} thread="backend" />
      <ContextPanelContent>
        <MembersContext members={members} />
      </ContextPanelContent>
    </>
  );
}
