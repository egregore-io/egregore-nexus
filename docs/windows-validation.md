# Windows validation

## Native Windows gate

The public [native-artifact workflow](../.github/workflows/npm-native-artifacts.yml)
is the release-blocking Windows gate. Its `win32-x64-msvc` job runs on a native
Windows runner, checks harness command contracts and the named-pipe listener,
runs the Gateway daemon-push relay tests, builds the native binary, and exercises
the packaged launcher and command surface. It runs on pushes to `main` and can
also be started with the workflow's manual dispatch. These targeted checks do not
claim full source-suite or scheduled-task lifecycle coverage.

For broader native validation, the checked-in
[Windows recipe](../testing/windows/nexus.toml) records the source-gate sequence:
Gateway dependency installation, Rust workspace tests and release build, repeated
native teardown tests, npm launcher smoke, Gateway typechecking and both builds,
the full Gateway test suite, Webconsole command-shim lifecycle, and
[packed CLI lifecycle](../testing/windows/core-lifecycle.ps1) checks. The recipe
declares the native executable, CLI tarball, headless Gateway package, and WebUI
distribution as artifacts. This repository does not provide a standalone recipe
runner; the recipe is a reference for reproducing those checks on native Windows.

## Reproducing the checks

Use a disposable native Windows environment with Git for Windows, the MSVC Rust
toolchain and Windows build tools, Node.js 24, pnpm 11, and PowerShell. Follow the
workflow for its exact setup and commands. Dependency installation requires
network access; live provider variants additionally depend on provider availability.

For the broader recipe, adapt its `C:\w` workspace, cache, artifact, and isolated
`NEXUS_HOME` paths to the validation environment. Its release step expects a
captured source revision in `.git/egregore-source.json`; an ordinary checkout does
not supply that metadata. Record the exact checkout revision and arrange the
equivalent build provenance before running that step. Preserve command ordering,
exit-status checks, concurrency limits, and test assertions when adapting paths.

The packed lifecycle fixture installs and removes the `EgregoreNexusDaemon`
scheduled task, starts and stops actual processes, and writes test state. It
refuses a pre-existing task and belongs in a disposable environment, not a
maintainer's active Nexus installation. Local validation is not permission to
publish an untested Windows target: source tests, release compilation, and actual
package/lifecycle smokes must still pass, with failures and unavailable checks
reported explicitly.

The Rust test and release steps expose Git for Windows' `usr\bin` tools explicitly: the
current `libsql-ffi` build script invokes `cp.exe`, which is not on a normal PowerShell PATH.
The recipe checks for it before invoking Cargo and appends its directory after MSVC tools so
Git's unrelated `link.exe` cannot shadow the native linker. This does not change the host or base image.
Gateway tests likewise receive Git's actual `sh.exe`; they build the Webconsole assets before
the packed-install test consumes them. JavaScript fixtures launch Node directly instead of asking
Windows to execute Unix npm shebang shims.

## Windows transport security

The Windows transport loader does not interpret Node's synthetic Unix permission bits as an ACL.
It captures the current token's user SID, file owner/DACL, native file identity, and content hash
through no-follow handles. The complete home-to-entry chain is checked again before spawning,
after asynchronous secret/database preparation. Reparse points, unknown ACE forms, absent DACLs,
wrong owners and mutation grants to other users/groups are refused. SYSTEM and Administrators
are trusted mutation principals; foreign read-only access is not considered mutation authority.
This is a conservative policy, not a generic effective-access evaluator or atomic spawn-by-handle.

Windows transport entries support `.exe`, or `.js`/`.mjs`/`.cjs` through the exact Gateway Node
executable with literal arguments. Batch files, PowerShell scripts, shell association and PATHEXT
fallback are not accepted. Windows PowerShell is required for the bounded native security probe;
missing tools, timeout and malformed output fail closed. Production does not rewrite transport ACLs.
An administrator-created file owned by the Administrators group must be assigned to the intended
user before it satisfies the current-user owner requirement.

The explicit transport environment still excludes arbitrary parent variables and exposes only
declared secrets. On Windows, Node/libuv additionally supplies its fixed OS-required environment
set (home/profile, logon/domain, system paths and temporary directory); see
[Node 24.18.0's `required_vars`](https://github.com/nodejs/node/blob/v24.18.0/deps/uv/src/win/process.c#L47-L59).
The native child test checks that exact set and rejects unrelated variables, rather than treating
every inherited variable as acceptable.

Disk-backed test teardown explicitly collects already-closed libsql statement objects before
unlink on Windows. This accommodates the pinned binding's deferred native finalization, not a
promise that `Client.close()` deterministically releases all OS handles. No retry loops or database
assertions are removed. The continuity snapshot utility gives new Windows artifacts a protected
current-user DACL and flushes file contents before rename; POSIX-only directory fsync durability
is not claimed for Windows.
