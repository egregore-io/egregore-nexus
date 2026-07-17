use nexus_harness_codex::resolve_codex_executable;

#[cfg(not(windows))]
#[test]
fn non_windows_keeps_the_requested_executable() {
    assert_eq!(
        resolve_codex_executable("/opt/codex/bin/codex").unwrap(),
        "/opt/codex/bin/codex"
    );
}

#[cfg(windows)]
#[test]
fn windows_finds_the_official_optional_package_binary() {
    use std::path::PathBuf;

    let package_root =
        std::env::temp_dir().join(format!("nexus-codex-executable-{}", std::process::id()));
    let executable = package_root
        .join("node_modules")
        .join("@openai")
        .join("codex-win32-x64")
        .join("vendor")
        .join("x86_64-pc-windows-msvc")
        .join("bin")
        .join("codex.exe");
    std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
    std::fs::write(&executable, b"fixture").unwrap();

    let original_path = std::env::var_os("PATH");
    let original_root = std::env::var_os("CODEX_MANAGED_PACKAGE_ROOT");
    std::env::set_var("PATH", PathBuf::from(&package_root).join("empty-path"));
    std::env::set_var("CODEX_MANAGED_PACKAGE_ROOT", &package_root);

    let resolved = resolve_codex_executable("codex").unwrap();

    match original_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    match original_root {
        Some(value) => std::env::set_var("CODEX_MANAGED_PACKAGE_ROOT", value),
        None => std::env::remove_var("CODEX_MANAGED_PACKAGE_ROOT"),
    }
    let _ = std::fs::remove_dir_all(package_root);

    assert_eq!(resolved, executable.to_string_lossy());
}
