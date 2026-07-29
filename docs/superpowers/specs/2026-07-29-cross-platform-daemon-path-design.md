# Cross-Platform Daemon Executable Path Design

## Problem

Nexus installs its daemon as a per-user background service. The generated Linux systemd unit,
macOS LaunchAgent, and Windows Task Scheduler wrapper configure `NEXUS_HOME`, but do not preserve
the executable search path available when the operator installs or updates Nexus.

On Linux, the user service manager can start Nexus before the graphical login imports the user's
environment. The daemon then inherits a system-only `PATH` and cannot revive a harness installed in
a user directory such as `~/.local/bin`, even though the same executable resolves in the
operator's shell. Restarting the daemon after login appears to repair the session because the new
daemon inherits the later environment.

## Goals

- Give every installed Nexus daemon a deterministic executable search path.
- Preserve the `PATH` visible during `nexus daemon install` or the equivalent update operation.
- Keep user-installed harnesses discoverable on Linux, macOS, and Windows.
- Provide safe platform fallbacks when the install process has no usable `PATH`.
- Heal existing installations when their service definition is regenerated.
- Keep runtime resurrection and session identity semantics unchanged.

## Non-Goals

- Persisting an absolute executable path for every harness.
- Delaying daemon startup until a graphical desktop session is ready.
- Discovering provider credentials or installing missing harness executables.
- Modifying an operator's global shell, systemd-manager, launchd, or Windows environment.

## Design

### Shared service path construction

Nexus will build a service execution path when it renders a background-service definition. The
builder accepts the Nexus binary, Nexus home directory, ambient install-time `PATH`, and target
platform.

It produces an ordered, de-duplicated list:

1. the directory containing the Nexus binary;
2. non-empty entries from the install-time `PATH`;
3. stable platform user fallbacks;
4. stable platform system fallbacks.

Empty entries are discarded because an empty path component makes executable lookup depend on the
service working directory. Existing entries retain their original order. Path-list serialization
uses the target platform's separator and service-format escaping.

Stable fallbacks are:

- Linux: `~/.local/bin`, `~/bin`, `/usr/local/bin`, `/usr/bin`, and `/bin`;
- macOS: `~/.local/bin`, `~/bin`, `/opt/homebrew/bin`, `/usr/local/bin`, `/usr/bin`, and `/bin`;
- Windows: `%APPDATA%\npm`, `%LOCALAPPDATA%\Microsoft\WindowsApps`, the Nexus binary directory,
  and the ambient user/system path captured by the installer.

Unavailable platform-specific user directories are omitted rather than guessed from another
user's environment.

### Service renderers

- The systemd unit writes the constructed value as a quoted `Environment=PATH=...` directive.
- The LaunchAgent writes `PATH` alongside `NEXUS_HOME` in `EnvironmentVariables`.
- The Task Scheduler wrapper assigns the constructed value to `$env:Path` before launching Nexus.

All renderers receive an explicit service-path value through a testable internal boundary. Public
installation functions capture the real ambient environment once and pass it into that boundary.
Tests therefore do not mutate process-global environment variables.

### Installation and update behavior

Service installation remains authoritative for the generated definition. Reinstalling after an
upgrade rewrites the definition with the current install-time path, reloads the platform service
manager where required, and uses the existing lifecycle behavior for starting or restarting the
daemon.

The change does not silently mutate a live service outside the existing install/update workflow.
The operator running the fixed installer receives the healed definition on the next service
installation or product update.

### Failure behavior

If the ambient `PATH` is absent or contains only empty entries, Nexus renders the stable platform
fallbacks. If a requested harness still cannot be found, launch continues to fail closed with a
clear executable-not-found error; Nexus does not guess a different harness or alter session
identity.

## Security

- Empty path components are removed.
- No shell evaluation or environment-variable expansion is introduced.
- Existing service-file escaping rules are extended to the `PATH` value.
- Capturing the install-time path does not grant new authority: service installation already runs
  as the same local user and writes that user's service definition.
- Secrets and provider credentials are never included in the path or service definition.

## Testing

Tests will first reproduce the missing-user-path behavior, then cover:

- path ordering, de-duplication, and removal of empty components;
- fallback construction when ambient `PATH` is unavailable;
- Linux systemd rendering with a user-installed executable directory;
- macOS LaunchAgent rendering and XML escaping;
- Windows Task Scheduler rendering and PowerShell escaping;
- preservation of the existing `NEXUS_HOME`, working-directory, restart, and command arguments;
- absence of revive/session changes.

The focused daemon lifecycle suite will run during development, followed by formatting, the full
Rust workspace tests, and the repository's required static gates.

## Compatibility

Service definitions remain per-user and use the same executable, arguments, restart policy, and
state directory as before. The only behavior change is that child harness lookup receives a
deterministic path derived from the installation environment plus stable platform fallbacks.
