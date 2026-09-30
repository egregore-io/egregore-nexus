//! Provider-owned native executable resolution.

use std::path::{Path, PathBuf};

/// Native OpenCode package target selected for the host OS and architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCodeNativeTarget {
    LinuxX64,
    LinuxArm64,
    MacX64,
    MacArm64,
    WindowsX64,
    WindowsArm64,
}

impl OpenCodeNativeTarget {
    /// Target for the currently compiled Nexus binary.
    pub fn current() -> Result<Self, &'static str> {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => Ok(Self::LinuxX64),
            ("linux", "aarch64") => Ok(Self::LinuxArm64),
            ("macos", "x86_64") => Ok(Self::MacX64),
            ("macos", "aarch64") => Ok(Self::MacArm64),
            ("windows", "x86_64") => Ok(Self::WindowsX64),
            ("windows", "aarch64") => Ok(Self::WindowsArm64),
            _ => Err("OpenCode native executable resolution does not support this OS/architecture"),
        }
    }

    /// Provider package suffix for this target.
    pub const fn package_suffix(self) -> &'static str {
        match self {
            Self::LinuxX64 => "linux-x64",
            Self::LinuxArm64 => "linux-arm64",
            Self::MacX64 => "darwin-x64",
            Self::MacArm64 => "darwin-arm64",
            Self::WindowsX64 => "windows-x64",
            Self::WindowsArm64 => "windows-arm64",
        }
    }

    const fn executable_name(self) -> &'static str {
        match self {
            Self::WindowsX64 | Self::WindowsArm64 => "opencode.exe",
            _ => "opencode",
        }
    }

    const fn is_windows(self) -> bool {
        matches!(self, Self::WindowsX64 | Self::WindowsArm64)
    }

    const fn os_label(self) -> &'static str {
        match self {
            Self::LinuxX64 | Self::LinuxArm64 => "Linux",
            Self::MacX64 | Self::MacArm64 => "macOS",
            Self::WindowsX64 | Self::WindowsArm64 => "Windows",
        }
    }

    fn package_names(self) -> [String; 2] {
        let base = format!("opencode-{}", self.package_suffix());
        let baseline = format!("{base}-baseline");
        match self {
            Self::LinuxX64 | Self::MacX64 | Self::WindowsX64 => [baseline, base],
            _ => [base, baseline],
        }
    }
}

/// Resolve the current host's native OpenCode executable.
pub fn resolve_opencode_executable(
    override_program: Option<&str>,
    path_env: Option<&str>,
) -> Result<String, String> {
    let target = OpenCodeNativeTarget::current().map_err(str::to_string)?;
    resolve_opencode_executable_for(override_program, path_env, target)
}

/// Resolve a native OpenCode executable for an explicit target.
///
/// This target-explicit form keeps the platform package contract testable from any development
/// host. Windows deliberately ignores npm `.cmd` shims because the supervised process must be the
/// provider's native `opencode.exe`.
pub fn resolve_opencode_executable_for(
    override_program: Option<&str>,
    path_env: Option<&str>,
    target: OpenCodeNativeTarget,
) -> Result<String, String> {
    if let Some(program) = override_program
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(resolved) = resolve_override(program, path_env, target) {
            if is_foreign_executable(Path::new(&resolved), target) {
                return Err(format!(
                    "NEXUS_OPENCODE_BIN '{resolved}' is not a {} executable; point it at the \
                     native OpenCode binary for this host",
                    target.os_label()
                ));
            }
            return Ok(resolved);
        }
        return Err(not_found_message(target, Some(program)));
    }

    let path_dirs: Vec<PathBuf> = path_env
        .into_iter()
        .flat_map(std::env::split_paths)
        .collect();
    let mut foreign = None;
    for candidate in
        package_candidates(&path_dirs, target).chain(direct_candidates(&path_dirs, target))
    {
        if !candidate.is_file() {
            continue;
        }
        // WSL appends the Windows PATH, so `%APPDATA%\npm\node_modules\opencode-ai` is visible
        // from Linux with the same staged filename as a Linux install. A PE cannot serve a Linux
        // plugin, so skip it and keep looking rather than launch it through interop.
        if is_foreign_executable(&candidate, target) {
            foreign.get_or_insert(candidate);
            continue;
        }
        return Ok(candidate.to_string_lossy().into_owned());
    }

    let mut message = not_found_message(target, None);
    if let Some(foreign) = foreign {
        message.push_str(&format!(
            " (skipped {} because it is not a {} executable)",
            foreign.display(),
            target.os_label()
        ));
    }
    Err(message)
}

/// Whether the file's header identifies it as an executable for a different operating system
/// than `target`. Only known-foreign formats are rejected; anything unrecognised is left to the
/// spawn to judge, so provider fixtures and future formats keep resolving.
fn is_foreign_executable(path: &Path, target: OpenCodeNativeTarget) -> bool {
    let mut magic = [0u8; 4];
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let Ok(read) = std::io::Read::read(&mut file, &mut magic) else {
        return false;
    };
    let magic = &magic[..read];
    let is_pe = magic.starts_with(b"MZ");
    let is_elf = magic.starts_with(b"\x7fELF");
    let is_mach_o = matches!(
        magic,
        [0xfe, 0xed, 0xfa, 0xce]
            | [0xfe, 0xed, 0xfa, 0xcf]
            | [0xce, 0xfa, 0xed, 0xfe]
            | [0xcf, 0xfa, 0xed, 0xfe]
            | [0xca, 0xfe, 0xba, 0xbe]
    );
    if target.is_windows() {
        is_elf || is_mach_o
    } else {
        is_pe
    }
}

fn resolve_override(
    program: &str,
    path_env: Option<&str>,
    target: OpenCodeNativeTarget,
) -> Option<String> {
    let path = Path::new(program);
    if path.is_absolute() || program.contains('/') || program.contains('\\') {
        if path.is_file() && (!target.is_windows() || has_exe_suffix(path)) {
            return Some(path.to_string_lossy().into_owned());
        }
        return None;
    }

    let executable = if target.is_windows() {
        program
            .strip_suffix(".exe")
            .map(|stem| format!("{stem}.exe"))
            .unwrap_or_else(|| format!("{program}.exe"))
    } else {
        program.to_string()
    };
    path_env
        .into_iter()
        .flat_map(std::env::split_paths)
        .map(|dir| dir.join(&executable))
        .find(|candidate| candidate.is_file())
        .map(|candidate| candidate.to_string_lossy().into_owned())
}

fn direct_candidates<'a>(
    path_dirs: &'a [PathBuf],
    target: OpenCodeNativeTarget,
) -> impl Iterator<Item = PathBuf> + 'a {
    path_dirs
        .iter()
        .map(move |dir| dir.join(target.executable_name()))
}

fn package_candidates<'a>(
    path_dirs: &'a [PathBuf],
    target: OpenCodeNativeTarget,
) -> impl Iterator<Item = PathBuf> + 'a {
    let package_names = target.package_names();
    path_dirs.iter().flat_map(move |dir| {
        let mut module_roots = vec![dir.join("node_modules")];
        if let Some(parent) = dir.parent() {
            module_roots.push(parent.join("lib").join("node_modules"));
            module_roots.push(parent.join("node_modules"));
        }

        let mut candidates = Vec::new();
        for modules in module_roots {
            // opencode-ai's postinstall selects and verifies the host package, then stages that
            // native executable under this stable cross-platform filename.
            candidates.push(modules.join("opencode-ai").join("bin").join("opencode.exe"));
            for package in &package_names {
                candidates.push(
                    modules
                        .join(package)
                        .join("bin")
                        .join(target.executable_name()),
                );
                candidates.push(
                    modules
                        .join("opencode-ai")
                        .join("node_modules")
                        .join(package)
                        .join("bin")
                        .join(target.executable_name()),
                );
            }
        }
        candidates
    })
}

fn has_exe_suffix(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
}

fn not_found_message(target: OpenCodeNativeTarget, requested: Option<&str>) -> String {
    let requested = requested
        .map(|value| format!(" '{value}'"))
        .unwrap_or_default();
    if target.is_windows() {
        format!(
            "native Windows OpenCode executable{requested} was not found; install opencode-ai or set NEXUS_OPENCODE_BIN to opencode.exe"
        )
    } else {
        format!(
            "OpenCode executable{requested} was not found on PATH; install opencode-ai or set NEXUS_OPENCODE_BIN"
        )
    }
}
