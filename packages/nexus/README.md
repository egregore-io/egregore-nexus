# @egregore/nexus

Install the complete Nexus local stack:

```bash
npm install --global @egregore/nexus
```

This installs `nexus`, `nexus-gateway`, and `nexus-webui`. The native CLI is selected automatically
for Linux, Windows, and WSL. The beta.5 npm release does not support macOS.
If npm's global command directory is missing from `PATH`, the
installer prints one copyable shell command and does not edit your profile.

```bash
nexus                       # first interactive run offers background startup for all three
nexus webconsole launch
nexus update --check
```

Accepting the one-time prompt enables daemon, Gateway and Webconsole server now and at login,
without opening a browser. Declining is remembered. Automated and agent invocations never prompt;
explicit `nexus daemon install`, `nexus gateway install`, and `nexus webconsole install` remain
available. Direct daemon installation retains its existing daemon/Gateway behavior.
`nexus update` updates and verifies the
complete installation transactionally, restoring the prior version on a failed health check.
