# @egregore/nexus-cli

The Nexus CLI and local transport daemon, with automatic native selection for Linux, macOS,
Windows, and WSL.

```bash
npm install --global @egregore/nexus-cli
nexus daemon install
nexus update --check
```

If npm's global command directory is not on `PATH`, installation prints the exact non-mutating
profile command for Bash, Zsh, Fish, or Windows PowerShell.

Install `@egregore/nexus` when you also want the Gateway and WebUI.

`nexus update` updates only this managed CLI/daemon installation. Gateway and Webconsole commands
remain discoverable but report that their package is unavailable.
