#![cfg(test)]

use super::daemon_error_message;
use std::io;

#[test]
fn daemon_already_running_error_is_rendered_without_generic_prefix() {
    let err = io::Error::new(
        io::ErrorKind::AlreadyExists,
        "nexus daemon already running (pid 123)",
    );

    assert_eq!(
        daemon_error_message(&err),
        "nexus daemon already running (pid 123)"
    );
}

#[test]
fn daemon_other_errors_keep_generic_prefix() {
    let err = io::Error::new(io::ErrorKind::Other, "db failed");

    assert_eq!(daemon_error_message(&err), "error: db failed");
}
