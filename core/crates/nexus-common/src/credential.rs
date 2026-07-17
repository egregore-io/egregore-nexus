//! Credential hashing helpers shared by identity and CLI/admin surfaces.
//!
//! Plaintext runtime credentials are returned to the operator once and are never persisted. The
//! durable store keeps only the `sha256:<hex>` string produced here.

use sha2::{Digest, Sha256};

/// Hash a runtime credential secret for storage/comparison.
pub fn hash_runtime_credential(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("sha256:{hex}")
}
