//! Canonical dotted entity-kind encoding.

use crate::{Kind, Locality};

/// Encode the parallel locality and closed nature axes as `locality.nature`.
pub fn dotted(locality: Locality, kind: Kind) -> String {
    format!("{}.{}", locality_token(locality), kind_token(kind))
}

/// Parse a canonical dotted entity kind, accepting legacy bare nature tokens as local.
pub fn parse(value: &str) -> Option<(Locality, Kind)> {
    let (locality, kind) = match value.split_once('.') {
        Some((locality, kind)) => (parse_locality(locality)?, parse_kind(kind)?),
        None => (Locality::Local, parse_kind(value)?),
    };
    Some((locality, kind))
}

fn locality_token(locality: Locality) -> &'static str {
    match locality {
        Locality::Local => "local",
        Locality::External => "external",
        Locality::Trusted => "trusted",
    }
}

fn kind_token(kind: Kind) -> &'static str {
    match kind {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

fn parse_locality(value: &str) -> Option<Locality> {
    match value {
        "local" => Some(Locality::Local),
        "external" => Some(Locality::External),
        "trusted" => Some(Locality::Trusted),
        _ => None,
    }
}

fn parse_kind(value: &str) -> Option<Kind> {
    match value {
        "agent" => Some(Kind::Agent),
        "human" => Some(Kind::Human),
        "notification" => Some(Kind::Notification),
        "app" => Some(Kind::App),
        _ => None,
    }
}
