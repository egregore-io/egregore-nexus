use hmac::{Hmac, Mac};
use nexus_notify::verify_timestamped_hmac;
use sha2::Sha256;

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn timestamped_signature_verifies_the_exact_public_v01_payload() {
    let secret = "s3cret";
    let timestamp = "1784000000000";
    let body = br#"{"source":"ci","payload":{"status":"green"}}"#;
    let signed = [timestamp.as_bytes(), b".", body].concat();
    let signature = format!("sha256={}", sign(secret, &signed));

    assert!(verify_timestamped_hmac(secret, timestamp, body, &signature));
    assert!(!verify_timestamped_hmac(
        secret, timestamp, br#"{}"#, &signature
    ));
    assert!(!verify_timestamped_hmac(
        secret,
        "1784000000001",
        body,
        &signature
    ));
    assert!(!verify_timestamped_hmac(
        secret,
        timestamp,
        body,
        signature.trim_start_matches("sha256=")
    ));
}
