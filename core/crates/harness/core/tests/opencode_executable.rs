//! Provider package resolution stays beside its harness-core implementation.

use nexus_harness_core::native_executable::{
    resolve_opencode_executable_for, OpenCodeNativeTarget,
};
use std::path::PathBuf;

fn fixture_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "nexus-opencode-native-{label}-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

#[test]
fn windows_resolves_the_official_native_optional_package() {
    let root = fixture_root("windows-package");
    let executable = root
        .join("node_modules")
        .join("opencode-windows-x64")
        .join("bin")
        .join("opencode.exe");
    std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
    std::fs::write(&executable, b"fixture").unwrap();
    std::fs::write(root.join("opencode.cmd"), b"@echo off\r\n").unwrap();

    let resolved = resolve_opencode_executable_for(
        None,
        Some(root.to_string_lossy().as_ref()),
        OpenCodeNativeTarget::WindowsX64,
    )
    .unwrap();

    assert_eq!(resolved, executable.to_string_lossy());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn every_target_resolves_the_official_opencode_ai_staged_binary() {
    for target in [
        OpenCodeNativeTarget::LinuxX64,
        OpenCodeNativeTarget::LinuxArm64,
        OpenCodeNativeTarget::MacX64,
        OpenCodeNativeTarget::MacArm64,
        OpenCodeNativeTarget::WindowsX64,
        OpenCodeNativeTarget::WindowsArm64,
    ] {
        let root = fixture_root(&format!("{}-staged", target.package_suffix()));
        let executable = root
            .join("node_modules")
            .join("opencode-ai")
            .join("bin")
            .join("opencode.exe");
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(&executable, b"fixture").unwrap();

        let resolved =
            resolve_opencode_executable_for(None, Some(root.to_string_lossy().as_ref()), target)
                .unwrap();

        assert_eq!(resolved, executable.to_string_lossy());
        let _ = std::fs::remove_dir_all(root);
    }
}

/// WSL appends the Windows PATH, so an `opencode-ai` installed for Windows under `%APPDATA%\npm`
/// is visible from Linux under the same staged filename. Its PE header must disqualify it so the
/// Linux install elsewhere on PATH wins, even though package candidates outrank plain PATH hits.
#[test]
fn linux_skips_a_windows_pe_staged_binary_leaked_in_through_wsl_path() {
    let windows_root = fixture_root("wsl-windows-npm");
    let pe = windows_root
        .join("node_modules")
        .join("opencode-ai")
        .join("bin")
        .join("opencode.exe");
    std::fs::create_dir_all(pe.parent().unwrap()).unwrap();
    std::fs::write(&pe, b"MZ\x90\x00PE fixture").unwrap();

    let linux_root = fixture_root("wsl-linux-bun");
    let elf = linux_root.join("opencode");
    std::fs::write(&elf, b"\x7fELF fixture").unwrap();

    let path_env = std::env::join_paths([&linux_root, &windows_root]).unwrap();
    let resolved = resolve_opencode_executable_for(
        None,
        Some(path_env.to_string_lossy().as_ref()),
        OpenCodeNativeTarget::LinuxX64,
    )
    .unwrap();
    assert_eq!(resolved, elf.to_string_lossy());

    // With no Linux install at all, the skipped PE is named so the operator can see why.
    let error = resolve_opencode_executable_for(
        None,
        Some(windows_root.to_string_lossy().as_ref()),
        OpenCodeNativeTarget::LinuxX64,
    )
    .unwrap_err();
    assert!(error.contains("was not found on PATH"), "{error}");
    assert!(error.contains(&pe.to_string_lossy().to_string()), "{error}");
    assert!(error.contains("not a Linux executable"), "{error}");

    let _ = std::fs::remove_dir_all(windows_root);
    let _ = std::fs::remove_dir_all(linux_root);
}

#[test]
fn windows_skips_an_elf_binary_under_the_staged_name() {
    let root = fixture_root("windows-elf-staged");
    let elf = root
        .join("node_modules")
        .join("opencode-ai")
        .join("bin")
        .join("opencode.exe");
    std::fs::create_dir_all(elf.parent().unwrap()).unwrap();
    std::fs::write(&elf, b"\x7fELF fixture").unwrap();

    let error = resolve_opencode_executable_for(
        None,
        Some(root.to_string_lossy().as_ref()),
        OpenCodeNativeTarget::WindowsX64,
    )
    .unwrap_err();
    assert!(error.contains("not a Windows executable"), "{error}");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn explicit_override_pointing_at_a_foreign_binary_is_rejected_with_its_path() {
    let root = fixture_root("override-foreign");
    let pe = root.join("opencode.exe");
    std::fs::write(&pe, b"MZ\x90\x00PE fixture").unwrap();

    let error = resolve_opencode_executable_for(
        Some(pe.to_string_lossy().as_ref()),
        Some(""),
        OpenCodeNativeTarget::LinuxX64,
    )
    .unwrap_err();
    assert!(error.contains("NEXUS_OPENCODE_BIN"), "{error}");
    assert!(error.contains("not a Linux executable"), "{error}");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn windows_never_accepts_a_command_shim_as_the_native_binary() {
    let root = fixture_root("windows-shim");
    std::fs::write(root.join("opencode.cmd"), b"@echo off\r\n").unwrap();

    let error = resolve_opencode_executable_for(
        None,
        Some(root.to_string_lossy().as_ref()),
        OpenCodeNativeTarget::WindowsX64,
    )
    .unwrap_err();

    assert!(error.contains("native Windows OpenCode executable"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn macos_and_linux_resolve_their_native_path_binary() {
    for target in [
        OpenCodeNativeTarget::MacArm64,
        OpenCodeNativeTarget::LinuxX64,
    ] {
        let root = fixture_root(target.package_suffix());
        let executable = root.join("opencode");
        std::fs::write(&executable, b"fixture").unwrap();

        let resolved =
            resolve_opencode_executable_for(None, Some(root.to_string_lossy().as_ref()), target)
                .unwrap();

        assert_eq!(resolved, executable.to_string_lossy());
        let _ = std::fs::remove_dir_all(root);
    }
}

#[test]
fn every_supported_target_resolves_its_provider_package_binary() {
    for target in [
        OpenCodeNativeTarget::LinuxX64,
        OpenCodeNativeTarget::LinuxArm64,
        OpenCodeNativeTarget::MacX64,
        OpenCodeNativeTarget::MacArm64,
        OpenCodeNativeTarget::WindowsX64,
        OpenCodeNativeTarget::WindowsArm64,
    ] {
        let root = fixture_root(target.package_suffix());
        let binary = if matches!(
            target,
            OpenCodeNativeTarget::WindowsX64 | OpenCodeNativeTarget::WindowsArm64
        ) {
            "opencode.exe"
        } else {
            "opencode"
        };
        let executable = root
            .join("node_modules")
            .join(format!("opencode-{}", target.package_suffix()))
            .join("bin")
            .join(binary);
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(&executable, b"fixture").unwrap();

        let resolved =
            resolve_opencode_executable_for(None, Some(root.to_string_lossy().as_ref()), target)
                .unwrap();

        assert_eq!(resolved, executable.to_string_lossy());
        let _ = std::fs::remove_dir_all(root);
    }
}
