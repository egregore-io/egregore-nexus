# v0.1.0 database baselines

Nexus v0.1.0 defines its first supported database formats. It does not import, upgrade, rename, or
repair a database created by a pre-release build.

The daemon and Gateway own separate baselines:

- the daemon file store contains identity, runtime-resurrection descriptors, and unsettled-delivery
  continuity;
- the daemon also uses a boot-scoped in-memory transport store;
- the Gateway file store contains durable product history and browser-facing projections.

Both file stores must be newly created by v0.1.0. Pointing either process at a non-v0.1.0 database
fails closed before the schema is changed.

## Archive a pre-release installation

Stop both database owners before copying or moving their files:

```bash
nexus gateway stop --force
nexus daemon stop --force
```

Archive the complete Nexus home, then create a clean one. Keeping the archive intact preserves its
database sidecars, configuration, logs, and runtime evidence together.

```bash
NEXUS_HOME=${NEXUS_HOME:-"$HOME/.nexus"}
ARCHIVE="${NEXUS_HOME}.pre-v0.1.0.$(date +%Y%m%d-%H%M%S)"
mv "$NEXUS_HOME" "$ARCHIVE"
install -d -m 700 "$NEXUS_HOME"
printf 'Archived pre-release Nexus home at %s\n' "$ARCHIVE"
```

Review and recreate any required configuration deliberately. Do not copy `nexus.db`, `gateway.db`,
their `-wal`/`-shm` sidecars, discovery files, or schema markers into the new home.

Start the daemon and Gateway normally. Each owner creates and validates its own v0.1.0 baseline:

```bash
nexus daemon start
nexus gateway start
nexus daemon status
nexus gateway status
```

If either process reports an unsupported or incomplete schema, stop it and verify that its
configured database path points into the clean home. Nexus intentionally provides no conversion
command for pre-release databases.
