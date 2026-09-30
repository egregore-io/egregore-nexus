use nexus::update::install_context::{InstallContext, InstallMethod, InstalledFacet};
use nexus::update::package_manager::{package_manager_plan, CommandPlan, PackageTools};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[test]
fn npm_plans_exact_top_level_update_and_rollback_from_home() {
    let context = npm_context("@egregore/nexus");
    let tools = PackageTools {
        npm: Some("/usr/bin/npm".into()),
        cargo: None,
    };

    let plan = package_manager_plan(
        &context,
        &tools,
        PathBuf::from("/home/ada"),
        "0.1.2",
        "0.1.3",
        &BTreeMap::new(),
    )
    .unwrap();

    assert_eq!(plan.probe.program, PathBuf::from("/usr/bin/npm"));
    assert_eq!(
        plan.probe.args,
        ["view", "@egregore/nexus", "version", "--json"]
    );
    assert_eq!(plan.install.cwd, PathBuf::from("/home/ada"));
    assert_eq!(
        plan.install.args,
        [
            "install",
            "--global",
            "--no-audit",
            "--no-fund",
            "@egregore/nexus@0.1.3",
        ]
    );
    assert_eq!(
        plan.rollback.args.last().map(String::as_str),
        Some("@egregore/nexus@0.1.2")
    );
}

#[test]
fn gateway_package_updates_its_exact_top_level_graph() {
    let context = npm_context("@egregore/nexus-gateway");
    let tools = PackageTools {
        npm: Some("npm".into()),
        cargo: None,
    };

    let plan = package_manager_plan(
        &context,
        &tools,
        "/home/ada".into(),
        "0.1.2",
        "0.1.3",
        &BTreeMap::new(),
    )
    .unwrap();

    assert_eq!(
        plan.install.args.last().map(String::as_str),
        Some("@egregore/nexus-gateway@0.1.3")
    );
}

#[test]
fn cargo_defaults_to_two_jobs_and_preserves_an_explicit_limit() {
    let context = unmanaged_context(InstallMethod::Cargo);
    let tools = PackageTools {
        npm: None,
        cargo: Some("/home/ada/.cargo/bin/cargo".into()),
    };
    let default = package_manager_plan(
        &context,
        &tools,
        "/home/ada".into(),
        "0.1.2",
        "0.1.3",
        &BTreeMap::new(),
    )
    .unwrap();
    let explicit = package_manager_plan(
        &context,
        &tools,
        "/home/ada".into(),
        "0.1.2",
        "0.1.3",
        &BTreeMap::from([("CARGO_BUILD_JOBS".into(), "6".into())]),
    )
    .unwrap();

    assert_eq!(default.install.env_overrides["CARGO_BUILD_JOBS"], "2");
    assert_eq!(explicit.install.env_overrides["CARGO_BUILD_JOBS"], "6");
    assert_eq!(
        default.install.args,
        [
            "install",
            "egregore-nexus",
            "--locked",
            "--force",
            "--version",
            "0.1.3",
        ]
    );
}

#[test]
fn manual_and_development_builds_never_receive_mutation_plans() {
    for method in [InstallMethod::Manual, InstallMethod::Development] {
        let error = package_manager_plan(
            &unmanaged_context(method),
            &PackageTools::default(),
            "/home/ada".into(),
            "0.1.2",
            "0.1.3",
            &BTreeMap::new(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("manually managed"), "{error}");
    }
}

#[test]
fn rendered_commands_never_include_environment_values() {
    let plan = CommandPlan {
        program: "/usr/bin/npm".into(),
        args: vec!["view".into(), "@egregore/nexus".into()],
        cwd: "/home/ada".into(),
        env_overrides: BTreeMap::from([
            ("NODE_AUTH_TOKEN".into(), "npm_secret".into()),
            ("CARGO_BUILD_JOBS".into(), "2".into()),
        ]),
    };

    let rendered = plan.redacted_display();

    assert!(rendered.contains("/usr/bin/npm view @egregore/nexus"));
    assert!(!rendered.contains("npm_secret"));
    assert!(!rendered.contains("NODE_AUTH_TOKEN"));
}

fn npm_context(package: &str) -> InstallContext {
    InstallContext {
        method: InstallMethod::Npm,
        managed_package: Some(package.into()),
        managed_root: Some("/home/ada/npm/nexus".into()),
        launcher_path: Some("/home/ada/npm/nexus/bin/nexus.mjs".into()),
        executable: "/home/ada/npm/nexus-cli/native/linux/nexus".into(),
        facets: [
            InstalledFacet::Cli,
            InstalledFacet::Daemon,
            InstalledFacet::Gateway,
            InstalledFacet::Webconsole,
        ]
        .into_iter()
        .collect(),
        wsl: false,
    }
}

fn unmanaged_context(method: InstallMethod) -> InstallContext {
    InstallContext {
        method,
        managed_package: None,
        managed_root: None,
        launcher_path: None,
        executable: "/opt/bin/nexus".into(),
        facets: BTreeSet::from([InstalledFacet::Cli, InstalledFacet::Daemon]),
        wsl: false,
    }
}
