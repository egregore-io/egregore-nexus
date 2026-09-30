//! Hermetic cross-platform Codex app-server executable for integration tests.

#[path = "support/fake_codex_app_server_unix.rs"]
mod server;

fn main() {
    server::run();
}
