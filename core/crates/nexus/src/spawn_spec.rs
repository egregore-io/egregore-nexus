//! Composition-root loader for **spawn-spec** (manifest-defined) ACP-pure harnesses.
//!
//! An ACP-pure harness is one whose entire integration is "spawn this command; it speaks ACP on
//! stdio". For those, no per-harness Rust is needed: one TOML manifest per harness at
//! `<nexus home>/harnesses/<id>.toml` describes the ACP spawn command, the contract side is a
//! non-headed [`GenericHarness`], and the runtime side is
//! [`nexus_agent::adapter::SpawnSpecAdapter`]. This module owns the manifest format and wires
//! both halves into the two registries at the composition root ([`install_spawn_specs`]).
//!
//! Manifest format (the file stem is the harness id, validated by [`HarnessId::new`]):
//!
//! ```toml
//! # ~/.nexus/harnesses/goose.toml
//! [command]
//! program = "goose"
//! args = ["acp"]
//! # cwd = "/optional/pinned/dir"   # default: the launch cwd
//!
//! [command.env]                    # optional; launch identity env is layered after this
//! GOOSE_MODE = "acp"
//! ```
//!
//! A runtime that needs bespoke bootstrap (hook installs, config injection) has outgrown a
//! manifest and should become a harness crate.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use nexus_agent::adapter::{Adapter, HarnessCommand, SpawnSpecAdapter};
use nexus_agent::AdapterRegistry;
use nexus_contracts::HarnessId;
use nexus_harness_core::{GenericHarness, Harness as HarnessContract};

use crate::harness_registry;

/// A harness fully described by a TOML manifest: the validated id, the leaked generic contract,
/// and the base spawn command the adapter will layer the launch context onto.
pub struct SpawnSpec {
    /// Validated harness id (the manifest's file stem).
    pub id: HarnessId,
    /// Contract half — a non-headed [`GenericHarness`] keyed by the manifest id.
    pub contract: &'static dyn HarnessContract,
    /// Base spawn command (program + args + optional cwd pin + manifest env).
    pub command: HarnessCommand,
}

impl std::fmt::Debug for SpawnSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `dyn Harness` has no `Debug` bound; its identifying token is enough here.
        f.debug_struct("SpawnSpec")
            .field("id", &self.id)
            .field("contract", &self.contract.agent_token())
            .field("command", &self.command)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    command: ManifestCommand,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestCommand {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// Parse one manifest without touching the filesystem.
///
/// `stem` is the file stem (the would-be harness id) and `source` is the TOML text. Keeping this
/// seam public lets harness tooling validate a manifest with the same parser the daemon uses.
pub fn parse_spawn_spec(stem: &str, source: &str) -> Result<SpawnSpec, String> {
    let id = HarnessId::new(stem).map_err(|err| format!("invalid harness id {stem:?}: {err}"))?;
    let manifest: Manifest =
        toml::from_str(source).map_err(|err| format!("invalid manifest: {err}"))?;
    if manifest.command.program.trim().is_empty() {
        return Err("command.program must be non-empty".to_string());
    }
    // `GenericHarness` stores `&'static str`. Manifests are read once at startup, so leaking the
    // token/contract (bounded by manifest count) is the cheap way to satisfy that. The headed
    // program stays empty: the manifest command belongs exclusively to the ACP adapter.
    let token: &'static str = Box::leak(stem.to_owned().into_boxed_str());
    let contract: &'static dyn HarnessContract =
        Box::leak(Box::new(GenericHarness::new(token, "")));
    Ok(SpawnSpec {
        id,
        contract,
        command: HarnessCommand {
            program: manifest.command.program,
            args: manifest.command.args,
            cwd: manifest.command.cwd,
            env: manifest.command.env.into_iter().collect(),
        },
    })
}

/// Directory holding spawn-spec manifests: `<nexus home>/harnesses`.
fn manifest_dir() -> Option<PathBuf> {
    crate::local_operator::nexus_home().map(|home| home.join("harnesses"))
}

/// Load every `*.toml` manifest under the manifest directory, sorted by id for deterministic
/// registration. A missing directory means "no extra harnesses"; an invalid manifest is logged
/// and skipped so one broken file cannot take down daemon startup.
pub fn load_spawn_specs() -> Vec<SpawnSpec> {
    let Some(dir) = manifest_dir() else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut specs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let source = match std::fs::read_to_string(&path) {
            Ok(source) => source,
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "skipping unreadable spawn-spec manifest");
                continue;
            }
        };
        match parse_spawn_spec(stem, &source) {
            Ok(spec) => specs.push(spec),
            Err(reason) => {
                tracing::warn!(path = %path.display(), %reason, "skipping invalid spawn-spec manifest");
            }
        }
    }
    specs.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
    specs
}

/// Composition-root install: load the manifests once and register each harness in **both**
/// registries — the contract via [`harness_registry::init_harness_registry`] (first install
/// wins; every composition-root site loads the same manifest set, so a second call is a no-op)
/// and the runtime as a [`SpawnSpecAdapter`] factory. Returns the installed ids.
pub fn install_spawn_specs(registry: &mut AdapterRegistry) -> Vec<HarnessId> {
    let specs = load_spawn_specs();
    let extras: Vec<(HarnessId, &'static dyn HarnessContract)> = specs
        .iter()
        .map(|spec| (spec.id.clone(), spec.contract))
        .collect();
    harness_registry::init_harness_registry(&extras);
    let mut ids = Vec::with_capacity(specs.len());
    for spec in specs {
        let SpawnSpec { id, command, .. } = spec;
        let factory_id = id.clone();
        registry.register(
            &id,
            Arc::new(move |ctx| {
                Arc::new(SpawnSpecAdapter::new(
                    factory_id.clone(),
                    command.clone(),
                    ctx,
                )) as Arc<dyn Adapter>
            }),
        );
        ids.push(id);
    }
    ids
}
