//! Hermetic fake Codex app-server executable for integration tests.

#[cfg(unix)]
#[path = "support/fake_codex_app_server_unix.rs"]
mod unix;

#[cfg(unix)]
fn main() {
    unix::run();
}

#[cfg(not(unix))]
fn main() {
    eprintln!("fake Codex app-server fixture requires Unix sockets");
    std::process::exit(2);
}
