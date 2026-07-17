#![cfg(test)]

use std::path::{Path, PathBuf};

use super::*;

#[test]
fn daemon_ipc_paths_are_pinned_for_unix_and_windows() {
    assert_eq!(
        endpoint_path_for_platform(Path::new("/tmp/nexus"), false),
        PathBuf::from("/tmp/nexus/daemon-ipc.sock")
    );
    assert_eq!(
        endpoint_path_for_platform(Path::new(r"C:\Users\Example\.nexus"), true),
        PathBuf::from(r"\\.\pipe\nexus-daemon-ipc-301b7079ca4e6087")
    );
}
