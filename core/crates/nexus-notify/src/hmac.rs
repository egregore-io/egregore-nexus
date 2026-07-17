//! HMAC-SHA256 verification for gateway notification ingress (backend §7).
//!
//! **v4:** external producers never reach the daemon directly. They hit the TS gateway's public
//! API with an `X-Nexus-Signature` ([`NOTIFY_SIGNATURE_HEADER`]) HMAC over the **raw request
//! bytes**; the gateway or shared notification ingress code uses this helper to verify that
//! signature against the configured secret. A bad or malformed signature → `false`, which the
//! ingest path records as `hmac_ok=false` and drops (no agent path is ever touched by an unverified
//! body).
//!
//! [`NOTIFY_SIGNATURE_HEADER`]: nexus_contracts::notify::NOTIFY_SIGNATURE_HEADER

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verify an HMAC-SHA256 signature over `raw_body` using `secret`.
///
/// `signature` is the gateway-forwarded value of the [`NOTIFY_SIGNATURE_HEADER`] header: the
/// lowercase hex encoding of `HMAC-SHA256(secret, raw_body)`. Comparison is constant-time (via
/// the `hmac` crate's `verify_slice`). Returns `false` for any malformed input (bad hex, wrong
/// length) rather than erroring — the caller records `hmac_ok=false` and drops.
///
/// [`NOTIFY_SIGNATURE_HEADER`]: nexus_contracts::notify::NOTIFY_SIGNATURE_HEADER
pub fn verify_hmac(secret: &str, raw_body: &[u8], signature: &str) -> bool {
    let expected = match decode_hex(signature.trim()) {
        Some(bytes) => bytes,
        None => return false,
    };
    // `new_from_slice` accepts a key of any length; only an empty secret could surprise a caller,
    // and an empty secret still produces a deterministic MAC the gateway can match.
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(raw_body);
    mac.verify_slice(&expected).is_ok()
}

/// Verify the public v0.1 timestamped notification signature.
///
/// The signed bytes are the exact UTF-8 timestamp header, one ASCII `.`, and the exact raw HTTP
/// body. The wire signature must be `sha256=<64 hex chars>`. An empty configured secret is always
/// rejected so a missing service configuration can never become a valid shared key.
pub fn verify_timestamped_hmac(
    secret: &str,
    timestamp: &str,
    raw_body: &[u8],
    signature: &str,
) -> bool {
    if secret.trim().is_empty() {
        return false;
    }
    let Some(signature_hex) = signature.trim().strip_prefix("sha256=") else {
        return false;
    };
    if signature_hex.len() != 64 {
        return false;
    }
    let mut signed = Vec::with_capacity(timestamp.len() + 1 + raw_body.len());
    signed.extend_from_slice(timestamp.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(raw_body);
    verify_hmac(secret, &signed, signature_hex)
}

/// Decode a lowercase/uppercase hex string into bytes. Returns `None` on any non-hex char or an
/// odd length — a malformed signature can never verify, so it is treated as a bad signature.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compute the canonical signature the gateway would forward, for use in tests.
    pub(crate) fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let out = mac.finalize().into_bytes();
        out.iter().map(|b| format!("{:02x}", b)).collect()
    }

    #[test]
    fn good_signature_verifies() {
        let secret = "s3cret";
        let body = br#"{"source":"ci","payload":{}}"#;
        let sig = sign(secret, body);
        assert!(verify_hmac(secret, body, &sig));
    }

    #[test]
    fn bad_signature_fails() {
        let secret = "s3cret";
        let body = br#"{"source":"ci"}"#;
        let sig = sign(secret, body);
        // Wrong body.
        assert!(!verify_hmac(secret, br#"{"source":"evil"}"#, &sig));
        // Wrong secret.
        assert!(!verify_hmac("other", body, &sig));
        // Malformed hex.
        assert!(!verify_hmac(secret, body, "not-hex"));
        // Odd length.
        assert!(!verify_hmac(secret, body, "abc"));
    }
}
