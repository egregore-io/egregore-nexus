import { handleConversationLogsRequest } from "../../routes/api/conversation.logs";
import { handleConversationPromptRequest } from "../../routes/api/conversation.prompt";
import { handleLoginRequest } from "../../routes/api/login";
import { handleProjectsAssignRequest } from "../../routes/api/projects.assign";
import { handleProjectsRequest } from "../../routes/api/projects";

const WEBUI_API_PATHS = new Set([
  "/api/login",
  "/api/conversation/prompt",
  "/api/conversation/logs",
  "/api/projects",
  "/api/projects/assign",
]);

export function isWebuiApiPath(pathname: string): boolean {
  return WEBUI_API_PATHS.has(pathname);
}

/**
 * Serves the small compatibility surface used by the separately-built WebUI.
 * All handlers remain Gateway-owned; the browser never reaches daemon IPC or
 * either transport store directly.
 */
export async function dispatchWebuiApi(request: Request): Promise<Response> {
  switch (new URL(request.url).pathname) {
    case "/api/login": return handleLoginRequest(request);
    case "/api/conversation/prompt": return handleConversationPromptRequest(request);
    case "/api/conversation/logs": return handleConversationLogsRequest(request);
    case "/api/projects": return handleProjectsRequest(request);
    case "/api/projects/assign": return handleProjectsAssignRequest(request);
    default:
      return new Response(JSON.stringify({ error: { code: "not_found", message: "not found" } }), {
        status: 404,
        headers: { "content-type": "application/json" },
      });
  }
}
