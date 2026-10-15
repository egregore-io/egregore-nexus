#!/usr/bin/env node
// Install-only experimental macOS evidence. This never starts Nexus services.
import * as fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const REGISTRY = 'https://registry.npmjs.org/';
const NAMES = ['@egregore/nexus', '@egregore/nexus-cli', '@egregore/nexus-gateway'];
const SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const HELP = `Usage: node scripts/nexus-macos-npm-install-smoke.mjs
  --mode candidate|registry --version SEMVER --arch arm64|x64 --output PATH
  candidate: --tarballs DIR
  registry:  --selector exact|latest

Requires native macOS of the requested architecture (no Rosetta/platform override).
Uses fresh temporary HOME, npm prefix/cache and empty npmrc files; public npm only.
Candidate installs exactly three versioned tgzs with normal lifecycle scripts.
Registry installs only @egregore/nexus (exact version or latest), then verifies all
three package versions and public dist SHA-512s using script-free npm pack fetches.
Only the installed nexus --version is executed; no daemon, Gateway or provider.
Writes a JSON receipt to a new output path and removes temporary installed files.
--help performs no filesystem writes, network, installation or child execution.
`;

export function parseArgs(argv) {
  if (argv.length === 1 && ['--help', '-h'].includes(argv[0])) return { help: true };
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i];
    if (!['--mode', '--version', '--arch', '--output', '--tarballs', '--selector'].includes(key)) throw new Error(`unknown argument: ${key}`);
    if (Object.hasOwn(options, key.slice(2))) throw new Error(`duplicate argument: ${key}`);
    if (!argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error(`missing value for ${key}`);
    options[key.slice(2)] = argv[i + 1];
  }
  validateOptions(options);
  return options;
}

function validateOptions(options) {
  if (!['candidate', 'registry'].includes(options.mode)) throw new Error('--mode must be candidate or registry');
  if (typeof options.version !== 'string' || !SEMVER.test(options.version)) throw new Error('--version must be strict SemVer without a v prefix');
  if (!['arm64', 'x64'].includes(options.arch)) throw new Error('--arch must be arm64 or x64');
  if (typeof options.output !== 'string' || !options.output.trim() || options.output === '-') throw new Error('--output must name a new receipt file');
  if (options.mode === 'candidate') {
    if (typeof options.tarballs !== 'string' || !options.tarballs.trim()) throw new Error('candidate --tarballs is required');
    if (options.selector !== undefined) throw new Error('--selector is registry-only');
  } else {
    if (!['exact', 'latest'].includes(options.selector)) throw new Error('registry --selector must be exact or latest');
    if (options.tarballs !== undefined) throw new Error('--tarballs is candidate-only');
  }
}

// Deliberately built from an allowlist, never from the caller's environment.
export function cleanEnvironment(root, nodePath) {
  return {
    HOME: join(root, 'home'), TMPDIR: join(root, 'tmp'),
    PATH: [join(root, 'prefix/bin'), dirname(nodePath), '/usr/bin', '/bin', '/usr/sbin', '/sbin'].join(':'),
    LANG: 'C', LC_ALL: 'C', CI: 'true',
    npm_config_registry: REGISTRY,
    npm_config_userconfig: join(root, 'user.npmrc'),
    npm_config_globalconfig: join(root, 'global.npmrc'),
    npm_config_prefix: join(root, 'prefix'), npm_config_cache: join(root, 'cache'),
    npm_config_ignore_scripts: 'false', npm_config_audit: 'false', npm_config_fund: 'false',
    npm_config_update_notifier: 'false',
  };
}

function tarballPaths(options) {
  return ['nexus-cli', 'nexus-gateway', 'nexus'].map((name) => resolve(options.tarballs, `egregore-${name}-${options.version}.tgz`));
}

export function installArguments(options) {
  return ['install', '--global', '--foreground-scripts', '--no-audit', '--no-fund',
    ...(options.mode === 'candidate' ? tarballPaths(options) : [options.selector === 'exact' ? `@egregore/nexus@${options.version}` : '@egregore/nexus'])];
}

function contained(parent, child, label) {
  const rel = relative(parent, child);
  if (!rel || rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel)) throw new Error(`${label} escapes required installed directory: ${child}`);
  return child;
}

function readJSON(io, path) { return JSON.parse(io.readFileSync(path, 'utf8')); }
function digest(io, path, algorithm, encoding) { return createHash(algorithm).update(io.readFileSync(path)).digest(encoding); }
function integrity(io, path) { return `sha512-${digest(io, path, 'sha512', 'base64')}`; }

function installedPackages(io, globalRoot, version) {
  const umbrellaManifest = join(globalRoot, '@egregore/nexus/package.json');
  const requireInstalled = createRequire(umbrellaManifest);
  return NAMES.map((name) => {
    const manifest = io.realpathSync(name === NAMES[0] ? umbrellaManifest : requireInstalled.resolve(`${name}/package.json`));
    contained(globalRoot, manifest, name);
    const pkg = readJSON(io, manifest);
    if (pkg.name !== name || pkg.version !== version) throw new Error(`installed package name/version mismatch: ${name}, expected ${version}, got ${pkg.name}@${pkg.version}`);
    if (name === NAMES[0] && NAMES.slice(1).some((dep) => pkg.dependencies?.[dep] !== version)) throw new Error('umbrella dependency version is not exact expected version');
    const directory = dirname(manifest);
    if (name === NAMES[1]) {
      const launcher = io.realpathSync(requireInstalled.resolve(`${name}/bin/nexus.mjs`));
      contained(directory, launcher, 'resolved CLI launcher');
    }
    return { name, version: pkg.version, directory, manifest, legacyIntegrity: pkg._integrity, legacyResolved: pkg._resolved };
  });
}

function compareInstalledLocks(io, globalRoot, packages, artifacts) {
  const checks = [];
  const lockPaths = [join(globalRoot, '.package-lock.json'), join(globalRoot, 'package-lock.json')];
  for (const lockPath of lockPaths) {
    if (!io.existsSync(lockPath)) continue;
    const lock = readJSON(io, lockPath);
    for (const pkg of packages) {
      // Hidden npm lock keys are relative to node_modules; standard locks use node_modules/.
      const rel = relative(globalRoot, pkg.directory).split(sep).join('/');
      const entry = lock.packages?.[rel] ?? lock.packages?.[`node_modules/${rel}`];
      if (!entry) continue;
      const artifact = artifacts.find((a) => a.name === pkg.name);
      if (entry.version && entry.version !== pkg.version) throw new Error(`installed lock version mismatch for ${pkg.name}`);
      if (entry.integrity && entry.integrity !== artifact.integrity) throw new Error(`installed lock integrity mismatch for ${pkg.name}`);
      if (entry.resolved && artifact.tarball && entry.resolved !== artifact.tarball) throw new Error(`installed lock resolved mismatch for ${pkg.name}`);
      checks.push({ name: pkg.name, lockPath, integrity: entry.integrity ?? null, resolved: entry.resolved ?? null, integrityCompared: Boolean(entry.integrity) });
    }
  }
  for (const pkg of packages) {
    if (!pkg.legacyIntegrity) continue;
    const artifact = artifacts.find((a) => a.name === pkg.name);
    if (pkg.legacyIntegrity !== artifact.integrity) throw new Error(`installed manifest integrity mismatch for ${pkg.name}`);
    checks.push({ name: pkg.name, manifest: pkg.manifest, integrity: pkg.legacyIntegrity, resolved: pkg.legacyResolved ?? null, integrityCompared: true });
  }
  return { status: checks.length ? 'available' : 'unavailable-global-install-no-lock', checks,
    note: 'npm global installs may omit locks. Exact installed package names/versions are checked; npm performs install integrity verification. No installed-byte equivalence is claimed when integrity evidence is absent.' };
}

// Ordinary dependency injection supports unit fixtures, never CLI platform/path overrides.
export function runSmoke(options, dependencies = {}) {
  validateOptions(options);
  const platform = dependencies.platform ?? process.platform;
  const arch = dependencies.arch ?? process.arch;
  if (platform !== 'darwin') throw new Error('requires native process.platform darwin; refusing before side effects');
  if (arch !== options.arch) throw new Error(`native Node architecture mismatch: requested ${options.arch}, got ${arch}`);
  const io = dependencies.fs ?? fs;
  const output = resolve(options.output);
  if (io.existsSync(output)) throw new Error('--output already exists; refusing to overwrite');
  const receipt = { schemaVersion: 1, status: 'failed', mode: options.mode, version: options.version, arch: options.arch,
    selector: options.selector ?? null, startedAt: new Date().toISOString(), commands: [] };
  let root;
  let failure;
  try {
    const candidateArtifacts = options.mode === 'candidate' ? tarballPaths(options).map((path) => {
      if (!io.existsSync(path) || !io.lstatSync(path).isFile()) throw new Error(`candidate tarball must be an existing regular file (not symlink): ${path}`);
      const name = NAMES.find((n) => basename(path) === `${n.replace('@', '').replace('/', '-')}-${options.version}.tgz`);
      return { name, version: options.version, path, integrity: integrity(io, path) };
    }) : [];
    root = io.mkdtempSync(join(dependencies.tempDirectory ?? tmpdir(), 'nexus-macos-npm-install-'));
    const env = cleanEnvironment(root, dependencies.nodePath ?? process.execPath);
    for (const dir of ['home', 'tmp', 'prefix', 'cache', 'downloads']) io.mkdirSync(join(root, dir));
    for (const filename of ['user.npmrc', 'global.npmrc']) io.writeFileSync(join(root, filename), '', { mode: 0o600 });
    receipt.isolation = { root, home: env.HOME, prefix: env.npm_config_prefix, cache: env.npm_config_cache, registry: REGISTRY,
      environmentPolicy: 'explicit-allowlist-no-inherited-credentials-or-native-overrides', removedAfterRun: true };
    const run = dependencies.run ?? ((command, argv, opts) => spawnSync(command, argv, { ...opts, encoding: 'utf8', timeout: 600_000, maxBuffer: 32 * 1024 * 1024 }));
    const command = (executable, argv, acceptedStatuses = [0]) => {
      const result = run(executable, argv, { env, cwd: root });
      const entry = { executable, argv, status: result.status ?? null, signal: result.signal ?? null, stdout: result.stdout ?? '', stderr: result.stderr ?? '' };
      receipt.commands.push(entry);
      if (result.error || !acceptedStatuses.includes(result.status) || result.signal) throw new Error(`command failed: ${executable} ${argv.join(' ')}${result.error ? `: ${result.error.message}` : ''}`);
      return entry;
    };
    const unameSystem = command('uname', ['-s']).stdout.trim();
    const unameMachine = command('uname', ['-m']).stdout.trim();
    const expectedMachine = options.arch === 'arm64' ? 'arm64' : 'x86_64';
    if (unameSystem !== 'Darwin' || unameMachine !== expectedMachine) throw new Error(`native uname architecture mismatch: ${unameSystem} ${unameMachine}`);
    receipt.host = { platform, arch, unameSystem, unameMachine };
    const translation = command('/usr/sbin/sysctl', ['-in', 'sysctl.proc_translated'], [0, 1]);
    const translationValue = translation.stdout.trim();
    if (translation.status === 0 && translationValue === '1') throw new Error('Rosetta translation is forbidden; a native architecture host is required');
    const absentKey = translationValue === '' && (translation.stderr === '' || /^sysctl: unknown oid ['"]sysctl\.proc_translated['"]\s*$/.test(translation.stderr));
    if (!(translation.status === 0 && translationValue === '0' && translation.stderr === '') && !absentKey) throw new Error('cannot establish native execution: unexpected sysctl translation probe result');
    receipt.host.translation = { ...translation, state: translationValue === '0' ? 'native' : 'absent' };
    Object.assign(receipt.host, { swVers: command('sw_vers', []).stdout,
      nodeVersion: process.version, nodeExecutable: dependencies.nodePath ?? process.execPath,
      npmVersion: command('npm', ['--version']).stdout.trim(), registry: command('npm', ['config', 'get', 'registry']).stdout.trim() });
    if (receipt.host.registry !== REGISTRY) throw new Error('npm registry is not credential-free public registry');
    receipt.install = command('npm', installArguments(options));
    const reportedRoot = command('npm', ['root', '--global']).stdout.trim();
    if (!isAbsolute(reportedRoot)) throw new Error('npm global root must be an absolute installed path');
    const globalRoot = contained(io.realpathSync(env.npm_config_prefix), io.realpathSync(reportedRoot), 'npm global root');
    receipt.packages = installedPackages(io, globalRoot, options.version);
    const cli = receipt.packages.find((p) => p.name === '@egregore/nexus-cli');
    const umbrella = receipt.packages.find((p) => p.name === '@egregore/nexus');
    const nativePath = join(cli.directory, 'native', `darwin-${options.arch}`, 'nexus');
    const nativeRealPath = contained(cli.directory, io.realpathSync(nativePath), 'native binary');
    if (!io.lstatSync(nativePath).isFile() || !io.statSync(nativeRealPath).isFile()) throw new Error('native binary must be a packaged regular file, not a symlink');
    io.accessSync(nativeRealPath, fs.constants.X_OK);
    const magic = io.readFileSync(nativeRealPath).subarray(0, 4).toString('hex');
    if (!['cffaedfe', 'feedfacf'].includes(magic)) throw new Error(`native binary is not thin 64-bit Mach-O: ${magic}`);
    const file = command('/usr/bin/file', ['-b', nativeRealPath]).stdout;
    const lipo = command('/usr/bin/lipo', ['-archs', nativeRealPath]).stdout.trim();
    if (!file.includes('Mach-O') || !file.includes(expectedMachine) || lipo !== expectedMachine) throw new Error(`native Mach-O architecture mismatch: file=${file}, lipo=${lipo}`);
    receipt.native = { path: nativePath, realPath: nativeRealPath, magic, file, architectures: lipo.split(/\s+/), sha256: digest(io, nativeRealPath, 'sha256', 'hex') };
    const wrapper = join(env.npm_config_prefix, 'bin/nexus');
    const wrapperReal = io.realpathSync(wrapper);
    if (wrapperReal !== io.realpathSync(join(umbrella.directory, 'bin/nexus.mjs'))) throw new Error('installed prefix/bin/nexus does not resolve to installed umbrella launcher');
    receipt.cliVersion = command(wrapper, ['--version']);
    if (receipt.cliVersion.stdout !== `nexus ${options.version}\n`) throw new Error('installed nexus --version stdout must exactly match expected version plus newline');
    if (receipt.cliVersion.stderr !== '') throw new Error('installed nexus --version stderr must be empty');
    const artifacts = options.mode === 'candidate' ? candidateArtifacts : receipt.packages.map((pkg) => {
      const spec = `${pkg.name}@${pkg.version}`;
      const dist = JSON.parse(command('npm', ['view', spec, 'dist', '--json']).stdout);
      if (!/^sha512-[A-Za-z0-9+/]{86}==$/.test(dist.integrity ?? '')) throw new Error(`public dist integrity must be SHA-512 for ${spec}`);
      const url = new URL(dist.tarball);
      if (url.protocol !== 'https:' || url.hostname !== 'registry.npmjs.org' || url.username || url.password || url.port) throw new Error(`public dist tarball must be credential-free registry.npmjs.org for ${spec}`);
      const downloaded = JSON.parse(command('npm', ['pack', spec, '--ignore-scripts', '--pack-destination', join(root, 'downloads'), '--json']).stdout);
      if (!Array.isArray(downloaded) || downloaded.length !== 1 || typeof downloaded[0].filename !== 'string' || basename(downloaded[0].filename) !== downloaded[0].filename) throw new Error(`unexpected npm pack filename for ${spec}`);
      const path = join(root, 'downloads', downloaded[0].filename);
      if (!io.lstatSync(path).isFile()) throw new Error('downloaded registry tarball must be a regular file');
      const downloadIntegrity = integrity(io, path);
      if (downloadIntegrity !== dist.integrity) throw new Error(`public tarball SHA-512 integrity mismatch for ${spec}`);
      return { name: pkg.name, version: pkg.version, tarball: dist.tarball, integrity: dist.integrity, downloadIntegrity, path };
    });
    receipt.provenance = { source: options.mode === 'candidate' ? 'candidate-tarballs' : 'public-registry', artifacts,
      installedLock: compareInstalledLocks(io, globalRoot, receipt.packages, artifacts) };
    receipt.status = 'passed';
  } catch (error) {
    failure = error;
    receipt.error = error.message;
  } finally {
    if (root) io.rmSync(root, { recursive: true, force: true });
    receipt.finishedAt = new Date().toISOString();
    io.mkdirSync(dirname(output), { recursive: true });
    io.writeFileSync(output, `${JSON.stringify(receipt, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
  }
  if (failure) throw failure;
  return receipt;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const options = parseArgs(process.argv.slice(2));
    if (options.help) process.stdout.write(HELP);
    else {
      runSmoke(options);
      process.stdout.write(`macOS npm install smoke passed; receipt: ${resolve(options.output)}\n`);
    }
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
