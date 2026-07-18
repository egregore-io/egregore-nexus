# @egregore/nexus-gateway

The canonical Nexus REST, WebSocket, AG-UI, and MCP backend.

```bash
npm install --global @egregore/nexus-gateway
nexus gateway install
nexus webconsole launch
nexus update --check
```

This package installs the `nexus` CLI and bundles the `nexus-webui` command. Install the complete
stack with `@egregore/nexus`.

`nexus gateway install` ensures the daemon service first and registers the Gateway with the native
per-user supervisor. `nexus webconsole launch` starts dependencies, waits for health, and opens the
browser. `nexus update` updates the CLI, Gateway, and bundled Webconsole together.
