//! `HarnessId` — validated, open-set harness identifier.
//!
//! The open-set replacement for the removed closed `Harness`
//! enum: an opaque lowercase token (e.g. `acme-agent`, `my_custom2`) that
//! serializes as a bare JSON string and maps to `string` in the generated
//! TypeScript. Unlike the enum, adding a new harness does NOT require a
//! contract bump — unknown tokens are valid ids; behavior lookup happens at
//! runtime via the harness registry, not in the type system.
//!
//! Token grammar: `^[a-z][a-z0-9_-]{0,63}$` — non-empty, starts with a
//! lowercase ASCII letter, then lowercase letters/digits/`_`/`-`, max 64 bytes.
//! Deserialization enforces the same grammar (invalid ids are rejected at the
//! wire boundary, fail-closed).

use serde::{Deserialize, Deserializer, Serialize};
use typeshare::typeshare;

/// Maximum byte length of a harness id token.
pub const HARNESS_ID_MAX_LEN: usize = 64;

/// Validated harness identifier (open set). Serializes as a bare string.
#[typeshare]
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct HarnessId(String);

/// Validation failure for a would-be [`HarnessId`] token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessIdError {
    /// The rejected input (truncated to 128 bytes for error hygiene).
    pub input: String,
    /// Human-readable reason the token was rejected.
    pub reason: &'static str,
}

impl std::fmt::Display for HarnessIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid harness id {:?}: {}", self.input, self.reason)
    }
}

impl std::error::Error for HarnessIdError {}

fn validate(token: &str) -> Result<(), &'static str> {
    if token.is_empty() {
        return Err("must be non-empty");
    }
    if token.len() > HARNESS_ID_MAX_LEN {
        return Err("must be at most 64 bytes");
    }
    let mut bytes = token.bytes();
    let first = bytes.next().expect("non-empty checked above");
    if !first.is_ascii_lowercase() {
        return Err("must start with a lowercase ASCII letter");
    }
    if !bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-') {
        return Err("may only contain lowercase ASCII letters, digits, '_' and '-'");
    }
    Ok(())
}

impl HarnessId {
    /// Validate `token` and wrap it. Fail-closed: no normalization is applied
    /// (callers must pass an already-lowercase token).
    pub fn new(token: impl Into<String>) -> Result<Self, HarnessIdError> {
        let token = token.into();
        match validate(&token) {
            Ok(()) => Ok(HarnessId(token)),
            Err(reason) => Err(HarnessIdError {
                input: token.chars().take(128).collect(),
                reason,
            }),
        }
    }

    /// The raw token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for HarnessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for HarnessId {
    type Err = HarnessIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        HarnessId::new(s)
    }
}

impl<'de> Deserialize<'de> for HarnessId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let token = String::deserialize(deserializer)?;
        HarnessId::new(token).map_err(serde::de::Error::custom)
    }
}

impl AsRef<str> for HarnessId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
