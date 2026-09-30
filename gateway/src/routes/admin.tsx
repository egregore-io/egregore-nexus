// Admin route — `/admin`.
//
// The agents table (launch · roles · status · project) with the tiers ctx-view,
// both fed from the live read-view (`useAdminAgents`/`useTierFacts`). Gated
// behind tier once auth is wired; every action will enqueue a daemon command
// intent. No seed.
import { createFileRoute } from "@tanstack/react-router";

import { useActiveProject } from "@app/activeProject";
import {
  AdminContext,
  AdminView,
  useAdminAgents,
  useAssignProject,
  useAgentOp,
  useSpawnAgent,
  useGrantTier,
  useTierFacts,
} from "@modules/pane";
import { useProjects } from "@modules/shell/useShellNav";
import { ContextPanelContent } from "@modules/shell";

export const Route = createFileRoute("/admin")({ component: AdminRoute });

function AdminRoute() {
  const { rows } = useAdminAgents();
  const { facts } = useTierFacts();
  const { data: projectItems } = useProjects();
  const activeProject = useActiveProject() ?? undefined;
  const assignProject = useAssignProject();
  const spawnAgent = useSpawnAgent();
  const agentOp = useAgentOp();
  const grantTier = useGrantTier();

  const projects = projectItems?.map((p) => p.name);

  return (
    <>
      <AdminView
        agents={rows}
        projects={projects}
        activeProject={activeProject}
        onAssignProject={(name, project) => assignProject.mutate({ name, project })}
        onGrantTier={(name, tier) => grantTier.mutate({ name, tier })}
        onLaunch={(kind, name) => spawnAgent.mutate({ kind, name })}
        onEvict={(name) => agentOp.mutate({ name, op: "evict" })}
        onKill={(name) => agentOp.mutate({ name, op: "kill" })}
        onDelete={(name) => agentOp.mutate({ name, op: "delete" })}
      />
      <ContextPanelContent>
        <AdminContext facts={facts} />
      </ContextPanelContent>
    </>
  );
}
