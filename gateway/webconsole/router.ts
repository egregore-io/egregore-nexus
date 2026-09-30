import { createRouter } from "@tanstack/react-router";

import { Route as rootRoute } from "../src/routes/__root";
import { Route as indexRouteImport } from "../src/routes/index";
import { Route as adminRouteImport } from "../src/routes/admin";
import { Route as agentRouteImport } from "../src/routes/agent.$handle";
import { Route as channelRouteImport } from "../src/routes/c.$channel";
import { Route as dmRouteImport } from "../src/routes/dm.$agent";
import { Route as loginRouteImport } from "../src/routes/login";
import { Route as membersRouteImport } from "../src/routes/members";
import { Route as pubRouteImport } from "../src/routes/pub";
import { Route as searchRouteImport } from "../src/routes/search";
import { Route as sourcesRouteImport } from "../src/routes/sources";

const child = <T extends { update: (options: object) => unknown }>(route: T, id: string, path: string) =>
  route.update({ id, path, getParentRoute: () => rootRoute });

const routeTree = rootRoute.addChildren([
  child(indexRouteImport, "/", "/"),
  child(adminRouteImport, "/admin", "/admin"),
  child(agentRouteImport, "/agent/$handle", "/agent/$handle"),
  child(channelRouteImport, "/c/$channel", "/c/$channel"),
  child(dmRouteImport, "/dm/$agent", "/dm/$agent"),
  child(loginRouteImport, "/login", "/login"),
  child(membersRouteImport, "/members", "/members"),
  child(pubRouteImport, "/pub", "/pub"),
  child(searchRouteImport, "/search", "/search"),
  child(sourcesRouteImport, "/sources", "/sources"),
] as never[]);

export function createWebconsoleRouter() {
  return createRouter({
    routeTree,
    scrollRestoration: true,
    defaultPreload: "intent",
  });
}
