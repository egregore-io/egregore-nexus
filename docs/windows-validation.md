# Windows validation

Nexus includes an opt-in local Windows fixture for contributors working from a Linux host. It uses
the separate `egregore-windows-lab` controller to run the exact local Git workspace inside a
disposable Windows 11 Enterprise Evaluation QEMU/KVM guest.

The public native-artifact workflow is the release-blocking Windows gate. It runs the Rust and
Gateway contracts on a native Windows runner, including daemon IPC, daemon-to-Gateway projection,
package selection, and command smoke. This QEMU fixture reproduces the broader source gate locally
and returns inspectable artifacts; external provider availability still determines which live
harness variants can run on a given Windows machine.

## Prerequisites

- Linux with KVM and QEMU
- OVMF and `swtpm`
- the separately installed `egregore-windows-lab` tool
- an official Windows 11 Enterprise Evaluation ISO with its SHA-256 recorded
- a sealed lab base created from that ISO

The ISO, VM disks, NVRAM, TPM state, credentials, and generated artifacts stay outside this
repository. The lab stages source onto guest NTFS and discards successful clones.

## Run it

Validate source capture and the recipe without booting a VM:

```bash
scripts/nexus-windows-validation --dry-run
```

Run the native Windows gates:

```bash
scripts/nexus-windows-validation
```

If `windows-lab` is not on `PATH`, or its `lab.toml` is not in the adjacent lab checkout or the
standard configuration directory, provide both explicitly:

```bash
NEXUS_WINDOWS_LAB_BIN=/path/to/windows-lab \
NEXUS_WINDOWS_LAB_CONFIG=/path/to/lab.toml \
scripts/nexus-windows-validation --dry-run
```

The canonical recipe is [`testing/windows/nexus.toml`](../testing/windows/nexus.toml). It runs
Rust formatting, compilation, tests, and a release build, followed by Gateway installation,
typechecking, tests, and both Gateway and WebUI builds. It returns `nexus.exe`, the headless
Gateway package, and the WebUI distribution as declared artifacts.
