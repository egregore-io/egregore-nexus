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
