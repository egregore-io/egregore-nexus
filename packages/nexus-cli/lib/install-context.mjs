import { realpathSync } from "node:fs";

const contextKey = Symbol.for("@egregore/nexus.install-context");
const managedPackages = new Set([
  "@egregore/nexus-cli",
  "@egregore/nexus-gateway",
  "@egregore/nexus",
]);

export function setNpmLauncherContext({ packageName, packageRoot, launcherPath }) {
  if (!managedPackages.has(packageName)) {
    throw new Error(`unsupported Nexus managed package: ${packageName}`);
  }
  globalThis[contextKey] = Object.freeze({
    packageName,
    packageRoot: realpathSync(packageRoot),
    launcherPath: realpathSync(launcherPath),
  });
}

export function npmInstallContext({
  defaultPackageName,
  defaultPackageRoot,
  defaultLauncherPath,
  nativeBinary,
  env,
}) {
  const context = globalThis[contextKey] ?? {
    packageName: defaultPackageName,
    packageRoot: realpathSync(defaultPackageRoot),
    launcherPath: realpathSync(defaultLauncherPath),
  };
  return {
    ...env,
    NEXUS_INSTALL_METHOD: "npm",
    NEXUS_MANAGED_PACKAGE: context.packageName,
    NEXUS_MANAGED_PACKAGE_ROOT: context.packageRoot,
    NEXUS_LAUNCHER_PATH: context.launcherPath,
    NEXUS_NATIVE_BIN: realpathSync(nativeBinary),
  };
}
