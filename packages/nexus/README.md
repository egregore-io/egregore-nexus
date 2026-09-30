# @egregore/nexus

Install the complete Nexus local stack:

```bash
npm install --global @egregore/nexus
```

This installs `nexus`, `nexus-gateway`, and `nexus-webui`. The native CLI is selected automatically
for Linux, macOS, Windows, and WSL. If npm's global command directory is missing from `PATH`, the
installer prints one copyable shell command and does not edit your profile.

```bash
nexus daemon start
nexus gateway start
nexus-webui --gateway-url http://127.0.0.1:4100
```
