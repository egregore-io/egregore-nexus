use std::env;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Command;

fn main() {
    verify_repository_version();
    println!("cargo:rerun-if-env-changed=NEXUS_BUILD_REVISION");
    println!("cargo:rerun-if-changed=.cargo_vcs_info.json");

    let revision = configured_revision()
        .or_else(packaged_revision)
        .or_else(checkout_revision)
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=NEXUS_BUILD_REVISION={revision}");
}

fn verify_repository_version() {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")
        .map(std::path::PathBuf::from)
        .expect("Cargo must provide CARGO_MANIFEST_DIR");
    let version_path = manifest_dir.join("../../..").join("VERSION");
    println!("cargo:rerun-if-changed={}", version_path.display());

    let repository_version = match fs::read_to_string(&version_path) {
        Ok(value) => value,
        Err(error)
            if error.kind() == ErrorKind::NotFound
                && manifest_dir.join(".cargo_vcs_info.json").is_file() =>
        {
            return;
        }
        Err(error) => panic!(
            "cannot read canonical Nexus version at {}: {error}",
            version_path.display()
        ),
    };
    let repository_version = repository_version.replace("\r\n", "\n");
    let canonical = repository_version.trim();
    let cargo = env::var("CARGO_PKG_VERSION").expect("Cargo must provide CARGO_PKG_VERSION");
    assert_eq!(
        repository_version,
        format!("{canonical}\n"),
        "VERSION must contain one value followed by a newline"
    );
    assert_eq!(
        cargo, canonical,
        "Cargo package version {cargo} does not match canonical Nexus version {canonical}; run scripts/nexus-version sync"
    );
}

fn configured_revision() -> Option<String> {
    normalized_revision(env::var("NEXUS_BUILD_REVISION").ok()?.as_str())
}

fn packaged_revision() -> Option<String> {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")?;
    let text = fs::read_to_string(Path::new(&manifest_dir).join(".cargo_vcs_info.json")).ok()?;
    let key = text.find("\"sha1\"")?;
    let value = text[key + "\"sha1\"".len()..]
        .split_once(':')?
        .1
        .trim_start();
    let value = value.strip_prefix('"')?.split_once('"')?.0;
    normalized_revision(value)
}

fn checkout_revision() -> Option<String> {
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR")?;
    let output = Command::new("git")
        .arg("-C")
        .arg(manifest_dir)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    normalized_revision(std::str::from_utf8(&output.stdout).ok()?)
}

fn normalized_revision(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".+-_".contains(character))
    {
        return None;
    }
    Some(value.to_owned())
}
