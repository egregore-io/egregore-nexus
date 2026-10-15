# @egregore/nexus-cli

The Nexus CLI and local transport daemon, with automatic native selection for Linux,
Windows, WSL, and experimental macOS x64/arm64. The macOS release gate is installation plus
`nexus --version` only; runtime unqualified. It does not qualify bus, launchd, providers,
GUI signing or notarization. Candidate and public exact-version/latest checks remain separate;
the lifecycle examples below are not macOS runtime guarantees.

```bash
npm install --global @egregore/nexus-cli
nexus                       # first interactive run offers per-user background startup
nexus update --check
```

If npm's global command directory is not on `PATH`, installation prints the exact non-mutating
profile command for Bash, Zsh, Fish, or Windows PowerShell.

Install `@egregore/nexus` when you also want the Gateway and WebUI.

The prompt detects installed components and remembers your answer. A daemon-only installation
does not download the missing Gateway/Webconsole. Scripts and agent sessions do not prompt;
`nexus daemon install` remains available for explicit setup.

`nexus update` updates only this managed CLI/daemon installation. Gateway and Webconsole commands
remain discoverable but report that their package is unavailable.
