# Windows validation

The Rust test and release steps expose Git for Windows' `usr\bin` tools explicitly: the
current `libsql-ffi` build script invokes `cp.exe`, which is not on a normal PowerShell PATH.
The recipe checks for it before invoking Cargo and appends its directory after MSVC tools so
Git's unrelated `link.exe` cannot shadow the native linker. This does not change the host or base image.
Gateway tests likewise receive Git's actual `sh.exe`; they build the Webconsole assets before
the packed-install test consumes them. JavaScript fixtures launch Node directly instead of asking
Windows to execute Unix npm shebang shims.

The public native-artifact workflow is the release-blocking Windows gate. It runs the Rust and
Gateway contracts on a native Windows runner, including daemon IPC, daemon-to-Gateway projection,
package selection, and command smoke. External provider availability still determines which live
harness variants can run on a given Windows machine.

For broader validation, follow the source-gate recipe in a disposable native Windows environment.
This repository does not bundle a recipe runner. Adapt workspace and artifact paths to that
environment and retain exact source provenance, command ordering, exit checks, and assertions.
Source tests, release compilation, and package/lifecycle checks must pass before publication.

The canonical recipe is [`testing/windows/nexus.toml`](../testing/windows/nexus.toml). It runs
Rust formatting, Gateway dependency installation, Rust compilation/tests and a release build,
then Gateway typechecking, both builds, and tests. It returns `nexus.exe`, the headless
Gateway package, and the WebUI distribution as declared artifacts.

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
