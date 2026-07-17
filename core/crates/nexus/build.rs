use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=NEXUS_BUILD_REVISION");
    println!("cargo:rerun-if-changed=.cargo_vcs_info.json");

    let revision = configured_revision()
        .or_else(packaged_revision)
        .or_else(checkout_revision)
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=NEXUS_BUILD_REVISION={revision}");
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
