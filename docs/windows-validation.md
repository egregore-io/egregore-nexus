# Windows validation

The public native-artifact workflow is the release-blocking Windows gate. It runs the Rust and
Gateway contracts on a native Windows runner, including daemon IPC, daemon-to-Gateway projection,
package selection, and command smoke. External provider availability still determines which live
harness variants can run on a given Windows machine.

For broader validation, follow the source-gate recipe in a disposable native Windows environment.
This repository does not bundle a recipe runner. Adapt workspace and artifact paths to that
environment and retain exact source provenance, command ordering, exit checks, and assertions.
Source tests, release compilation, and package/lifecycle checks must pass before publication.

The canonical recipe is [`testing/windows/nexus.toml`](../testing/windows/nexus.toml). It runs
Rust formatting, compilation, tests, and a release build, followed by Gateway installation,
typechecking, tests, and both Gateway and WebUI builds. It returns `nexus.exe`, the headless
Gateway package, and the WebUI distribution as declared artifacts.
