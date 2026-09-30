# @egregore/nexus

Install the complete Nexus local stack:

```bash
npm install --global @egregore/nexus
```

This installs `nexus`, `nexus-gateway`, and `nexus-webui`. The native CLI is selected automatically
for Linux, macOS, Windows, and WSL. If npm's global command directory is missing from `PATH`, the
installer prints one copyable shell command and does not edit your profile.

```bash
nexus daemon install
nexus webconsole launch
nexus update --check
```

The complete installation registers both daemon and Gateway in dependency order when you run
`nexus daemon install`. Webconsole remains on demand. `nexus update` updates and verifies the
complete installation transactionally, restoring the prior version on a failed health check.
