//! Native Codex executable resolution for headed app-server sessions.

#[cfg(windows)]
use std::path::{Path, PathBuf};

#[cfg(windows)]
const WINDOWS_CODEX_TARGET: &str = "x86_64-pc-windows-msvc";

#[cfg(windows)]
fn native_windows_codex_under(package_root: &Path) -> [PathBuf; 2] {
    let relative = Path::new("vendor")
        .join(WINDOWS_CODEX_TARGET)
        .join("bin")
        .join("codex.exe");
    [
        package_root
            .join("node_modules")
            .join("@openai")
            .join("codex-win32-x64")
            .join(&relative),
        package_root.join(relative),
    ]
}

#[cfg(windows)]
fn find_native_windows_codex(roots: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    roots.into_iter().find_map(|root| {
        native_windows_codex_under(&root)
            .into_iter()
            .find(|candidate| candidate.is_file())
    })
}

/// Resolve the headed Codex program to a native executable for the current platform.
///
/// On Windows, the npm `codex.cmd` shim is not a native process and cannot be supervised as the
/// app-server root. Nexus resolves the `codex.exe` selected by the official `@openai/codex`
/// package instead. Explicit executable paths remain authoritative for tests and vendored setups.
pub fn resolve_codex_executable(requested: &str) -> Result<String, String> {
    #[cfg(not(windows))]
    {
        Ok(requested.to_string())
    }

    #[cfg(windows)]
    {
        let requested_path = Path::new(requested);
        if requested_path.is_file() {
            return Ok(requested_path.to_string_lossy().into_owned());
        }
        if !requested.eq_ignore_ascii_case("codex") && !requested.eq_ignore_ascii_case("codex.exe")
        {
            return Ok(requested.to_string());
        }

        let path_dirs: Vec<PathBuf> = std::env::var_os("PATH")
            .map(|value| std::env::split_paths(&value).collect())
            .unwrap_or_default();
        if let Some(executable) = path_dirs
            .iter()
            .map(|dir| dir.join("codex.exe"))
            .find(|candidate| candidate.is_file())
        {
            return Ok(executable.to_string_lossy().into_owned());
        }

        let mut package_roots = Vec::new();
        if let Some(root) = std::env::var_os("CODEX_MANAGED_PACKAGE_ROOT") {
            package_roots.push(PathBuf::from(root));
        }
        for dir in &path_dirs {
            package_roots.push(dir.join("node_modules").join("@openai").join("codex"));
            if let Some(parent) = dir.parent() {
                package_roots.push(parent.join("node_modules").join("@openai").join("codex"));
            }
        }
        if let Some(app_data) = std::env::var_os("APPDATA") {
            package_roots.push(
                PathBuf::from(app_data)
                    .join("npm")
                    .join("node_modules")
                    .join("@openai")
                    .join("codex"),
            );
        }

        find_native_windows_codex(package_roots)
            .map(|path| path.to_string_lossy().into_owned())
            .ok_or_else(|| {
                "native Windows codex.exe was not found; install @openai/codex or set an explicit Codex executable path"
                    .to_string()
            })
    }
}
