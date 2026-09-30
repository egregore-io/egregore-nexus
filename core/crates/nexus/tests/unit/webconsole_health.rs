#[test]
fn health_probe_avoids_chunked_node_response_framing() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::read_webconsole_health;

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fixture health server");
    let port = listener.local_addr().expect("fixture address").port();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept health probe");
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .expect("set fixture read timeout");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).expect("read health probe");
            assert!(read > 0, "health probe closed before its headers completed");
            request.extend_from_slice(&chunk[..read]);
        }

        let request = String::from_utf8(request).expect("health probe is HTTP text");
        let body = format!(
            concat!(
                r#"{{"ok":true,"service":"nexus-webui","pid":{},"host":"127.0.0.1","port":{},"#,
                r#""url":"http://127.0.0.1:{}","gateway":"http://127.0.0.1:4101","#,
                r#""executable":"/tmp/nexus-webui"}}"#
            ),
            std::process::id(),
            port,
            port,
        );
        let response = if request.starts_with("GET /health HTTP/1.0\r\n") {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}"
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
                body.len()
            )
        };
        stream
            .write_all(response.as_bytes())
            .expect("write health response");
        request
    });

    let health = read_webconsole_health("127.0.0.1", port)
        .expect("parse the real Node WebUI health response shape")
        .expect("health response exists");
    let request = server.join().expect("health fixture server completed");

    assert!(request.starts_with("GET /health HTTP/1.0\r\n"));
    assert!(health.ok);
    assert_eq!(health.service, "nexus-webui");
    assert_eq!(health.host, "127.0.0.1");
    assert_eq!(health.port, port);
}

#[test]
fn webconsole_uses_shared_detach_and_never_waits_for_browser_exit() {
    let source = include_str!("../../src/webconsole_lifecycle.rs");

    assert!(source.contains("lifecycle_process::spawn_detached"));
    assert!(source.contains("spawn_browser_opener"));
    assert!(!source.contains("fn detach_command("));
    assert!(!source.contains("Command::new(\"xdg-open\").arg(url).status()"));
}

#[cfg(unix)]
#[test]
fn browser_opener_returns_after_spawn_instead_of_process_exit() {
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::spawn_browser_opener;

    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("browser.pid");
    let script = format!(
        "printf '%s\\n' \"$$\" > '{}'; exec sleep 60",
        pid_path.display()
    );
    let mut command = Command::new("sh");
    command.args(["-c", &script]);
    let started = Instant::now();

    spawn_browser_opener(&mut command).unwrap();

    assert!(started.elapsed() < Duration::from_millis(500));
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut pid = None;
    while Instant::now() < deadline {
        pid = read_ready_pid(&pid_path);
        if pid.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let pid = pid.expect("browser pid became ready");
    assert_eq!(unsafe { libc::kill(pid, 0) }, 0);
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
}

#[cfg(unix)]
fn read_ready_pid(path: &std::path::Path) -> Option<i32> {
    let contents = std::fs::read(path).ok()?;
    let contents = std::str::from_utf8(&contents).ok()?;
    let pid_text = contents.strip_suffix('\n')?;
    if pid_text.is_empty() || pid_text.contains('\n') {
        return None;
    }
    let pid: i32 = pid_text.parse().ok()?;
    (pid > 0).then_some(pid)
}

#[cfg(unix)]
#[test]
fn browser_pid_readiness_requires_one_complete_valid_record() {
    use std::fs;

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("browser.pid");

    assert_eq!(read_ready_pid(&path), None);
    for contents in ["", "123"] {
        fs::write(&path, contents).unwrap();
        assert_eq!(read_ready_pid(&path), None, "contents: {contents:?}");
    }
    for contents in ["0\n", "-123\n", "not-a-pid\n", "2147483648\n", "123\n456\n"] {
        fs::write(&path, contents).unwrap();
        assert_eq!(read_ready_pid(&path), None, "contents: {contents:?}");
    }
    fs::write(&path, [0xff, b'\n']).unwrap();
    assert_eq!(read_ready_pid(&path), None);
    fs::write(&path, "123\n").unwrap();
    assert_eq!(read_ready_pid(&path), Some(123));
}
