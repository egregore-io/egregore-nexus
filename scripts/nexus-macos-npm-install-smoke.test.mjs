import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';

const helper = new URL('./nexus-macos-npm-install-smoke.mjs', import.meta.url);
const load = () => import(helper.href);
const args = ['--mode', 'candidate', '--version', '0.1.8', '--arch', 'arm64', '--output', '/tmp/receipt.json', '--tarballs', '/tmp/packs'];

test('CLI --help is side-effect free even with hostile ambient configuration', () => {
  const result = spawnSync(process.execPath, [helper.pathname, '--help'], {
    encoding: 'utf8', env: { PATH: '', HOME: '/does-not-exist', NEXUS_NATIVE_BIN: '/must-not-run', npm_config_registry: 'https://secret.invalid' },
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /--mode candidate\|registry/);
  assert.equal(result.stderr, '');
});

test('arguments enforce SemVer, native architectures, and unambiguous install modes', async () => {
  const { parseArgs } = await load();
  assert.equal(parseArgs(args).version, '0.1.8');
  for (const version of ['latest', 'v0.1.8', '01.1.8', '0.1.8;touch /tmp/x', '1.0.0-01']) {
    assert.throws(() => parseArgs(args.map((v) => v === '0.1.8' ? version : v)), /version|SemVer/);
  }
  for (const extra of [['--force'], ['--os', 'darwin'], ['--cpu', 'arm64'], ['--registry', 'https://private.invalid'], ['--native-bin', '/tmp/nexus']]) {
    assert.throws(() => parseArgs([...args, ...extra]), /unknown/);
  }
  assert.throws(() => parseArgs(args.map((v) => v === 'arm64' ? 'aarch64' : v)), /arch/);
  assert.throws(() => parseArgs(args.map((v) => v === '/tmp/receipt.json' ? '' : v)), /output/);
  assert.throws(() => parseArgs([...args, '--arch', 'x64']), /duplicate/);
  assert.throws(() => parseArgs([...args, '--selector', 'latest']), /selector/);
  const registry = ['--mode', 'registry', '--version', '1.2.3-rc.1+build.9', '--arch', 'x64', '--output', '/tmp/r'];
  assert.throws(() => parseArgs(registry), /selector/);
  assert.throws(() => parseArgs([...registry, '--selector', 'exact', '--tarballs', '/tmp/p']), /tarballs/);
  assert.equal(parseArgs([...registry, '--selector', 'latest']).selector, 'latest');
});

test('clean environment contains no inherited credentials, source fallbacks, or platform overrides', async () => {
  const { cleanEnvironment } = await load();
  const env = cleanEnvironment('/fresh', '/tools/node/bin/node', {
    PATH: '/secret/.cargo/bin', NPM_TOKEN: 'secret', NODE_AUTH_TOKEN: 'secret', NODE_OPTIONS: '--require=/evil',
    NEXUS_NATIVE_BIN: '/source/nexus', CARGO_HOME: '/secret', npm_config_arch: 'x64', npm_config_os: 'linux',
  });
  assert.equal(env.HOME, '/fresh/home');
  assert.equal(env.npm_config_registry, 'https://registry.npmjs.org/');
  assert.equal(env.npm_config_userconfig, '/fresh/user.npmrc');
  assert.equal(env.npm_config_globalconfig, '/fresh/global.npmrc');
  assert.equal(env.npm_config_ignore_scripts, 'false');
  assert.equal(env.npm_config_prefix, '/fresh/prefix');
  assert.equal(env.npm_config_cache, '/fresh/cache');
  assert.doesNotMatch(JSON.stringify(env), /secret|NEXUS|CARGO|NODE_OPTIONS|npm_config_arch|npm_config_os/);
  assert.equal(env.PATH, '/fresh/prefix/bin:/tools/node/bin:/usr/bin:/bin:/usr/sbin:/sbin');
});

test('install plans use exactly three versioned candidate tgzs, or only the public umbrella selector', async () => {
  const { installArguments } = await load();
  assert.deepEqual(installArguments({ mode: 'candidate', version: '0.1.8', tarballs: '/packs' }), [
    'install', '--global', '--foreground-scripts', '--no-audit', '--no-fund',
    '/packs/egregore-nexus-cli-0.1.8.tgz', '/packs/egregore-nexus-gateway-0.1.8.tgz', '/packs/egregore-nexus-0.1.8.tgz',
  ]);
  for (const selector of ['exact', 'latest']) {
    assert.deepEqual(installArguments({ mode: 'registry', selector, version: '0.1.8' }).slice(5), [selector === 'exact' ? '@egregore/nexus@0.1.8' : '@egregore/nexus']);
  }
});

function fixture(t, { mode = 'candidate', arch = 'arm64', selector = 'exact', mutate } = {}) {
  const base = fs.mkdtempSync(join(tmpdir(), 'nexus-mac-smoke-unit-'));
  t.after(() => fs.rmSync(base, { recursive: true, force: true }));
  const packs = join(base, 'packs');
  fs.mkdirSync(packs);
  for (const name of ['nexus', 'nexus-cli', 'nexus-gateway']) fs.writeFileSync(join(packs, `egregore-${name}-0.1.8.tgz`), `unit-only-${name}`);
  const calls = [];
  let globalRoot;
  const options = { mode, arch, selector: mode === 'registry' ? selector : undefined, version: '0.1.8', tarballs: mode === 'candidate' ? packs : undefined, output: join(base, 'receipt.json') };
  const deps = {
    platform: 'darwin', arch, tempDirectory: base,
    run(command, argv, { env, cwd }) {
      calls.push({ command, argv, env, cwd });
      let stdout = '';
      if (command === 'uname') stdout = argv[0] === '-s' ? 'Darwin\n' : `${arch === 'arm64' ? 'arm64' : 'x86_64'}\n`;
      else if (command === '/usr/sbin/sysctl') stdout = '0\n';
      else if (command === 'sw_vers') stdout = 'ProductVersion: 15.0\n';
      else if (command === 'npm' && argv[0] === '--version') stdout = '10.8.2\n';
      else if (command === 'npm' && argv[0] === 'config') stdout = 'https://registry.npmjs.org/\n';
      else if (command === 'npm' && argv[0] === 'install') {
        globalRoot = join(env.npm_config_prefix, 'lib/node_modules');
        const scope = join(globalRoot, '@egregore');
        for (const name of ['nexus', 'nexus-cli', 'nexus-gateway']) {
          const dir = join(scope, name);
          fs.mkdirSync(join(dir, 'bin'), { recursive: true });
          fs.writeFileSync(join(dir, 'package.json'), JSON.stringify({ name: `@egregore/${name}`, version: '0.1.8', dependencies: name === 'nexus' ? { '@egregore/nexus-cli': '0.1.8', '@egregore/nexus-gateway': '0.1.8' } : {} }));
          fs.writeFileSync(join(dir, 'bin/nexus.mjs'), '// fixture, never executed');
        }
        const native = join(scope, 'nexus-cli/native', `darwin-${arch}`, 'nexus');
        fs.mkdirSync(join(native, '..'), { recursive: true });
        fs.writeFileSync(native, Buffer.from('cffaedfe00000000', 'hex'), { mode: 0o755 });
        fs.mkdirSync(join(env.npm_config_prefix, 'bin'), { recursive: true });
        fs.symlinkSync(join(scope, 'nexus/bin/nexus.mjs'), join(env.npm_config_prefix, 'bin/nexus'));
        mutate?.({ globalRoot, scope, native, env });
      } else if (command === 'npm' && argv[0] === 'root') stdout = `${globalRoot}\n`;
      else if (command === 'npm' && argv[0] === 'view') stdout = JSON.stringify({ integrity: `sha512-${createHash('sha512').update('registry-unit-tarball').digest('base64')}`, tarball: `https://registry.npmjs.org/@egregore/${argv[1].split('/')[1].split('@')[0]}/-/unit-0.1.8.tgz` });
      else if (command === 'npm' && argv[0] === 'pack') {
        const directory = argv[argv.indexOf('--pack-destination') + 1];
        const filename = `${argv[1].replaceAll('/', '-').replaceAll('@', '')}.tgz`;
        fs.writeFileSync(join(directory, filename), 'registry-unit-tarball');
        stdout = JSON.stringify([{ filename }]);
      }
      else if (command === '/usr/bin/file') stdout = `Mach-O 64-bit executable ${arch === 'arm64' ? 'arm64' : 'x86_64'}\n`;
      else if (command === '/usr/bin/lipo') stdout = `${arch === 'arm64' ? 'arm64' : 'x86_64'}\n`;
      else if (command.endsWith('/bin/nexus')) stdout = 'nexus 0.1.8\n';
      else throw new Error(`unexpected command ${command} ${argv}`);
      return { status: 0, stdout, stderr: '' };
    },
  };
  return { base, options, deps, calls };
}

test('candidate fixture records local integrity, exact installed resolution, and version-only execution', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t);
  const receipt = runSmoke(f.options, f.deps);
  assert.equal(receipt.status, 'passed');
  assert.equal(receipt.schemaVersion, 1);
  assert.equal(receipt.packages.length, 3);
  assert.equal(receipt.native.sha256, createHash('sha256').update(Buffer.from('cffaedfe00000000', 'hex')).digest('hex'));
  assert.equal(receipt.provenance.source, 'candidate-tarballs');
  assert.equal(receipt.provenance.installedLock.status, 'unavailable-global-install-no-lock');
  assert.equal(receipt.provenance.artifacts.length, 3);
  assert.equal(f.calls.some((c) => c.argv[0] === 'view' || c.argv[0] === 'pack'), false);
  assert.ok(receipt.provenance.artifacts.every((a) => /^sha512-/.test(a.integrity)));
  assert.deepEqual(f.calls.filter((c) => c.command.endsWith('/bin/nexus')).map((c) => c.argv), [['--version']]);
  assert.ok(f.calls.every((c) => !JSON.stringify(c.env).includes('NEXUS_NATIVE_BIN')));
  assert.ok(f.calls.every((c) => c.cwd.includes('nexus-macos-npm-install-')));
  assert.equal(JSON.parse(fs.readFileSync(f.options.output)).status, 'passed');
  assert.equal(fs.existsSync(receipt.isolation.root), false, 'ephemeral home/prefix/cache removed');
});

test('registry fixture queries exact public dist metadata for all three packages', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t, { mode: 'registry', arch: 'x64', selector: 'latest' });
  const receipt = runSmoke(f.options, f.deps);
  assert.equal(receipt.status, 'passed');
  assert.equal(receipt.provenance.source, 'public-registry');
  const views = f.calls.filter((c) => c.command === 'npm' && c.argv[0] === 'view');
  assert.equal(views.length, 3);
  assert.ok(views.every((c) => c.argv[1].endsWith('@0.1.8')));
  assert.equal(f.calls.filter((c) => c.argv[0] === 'pack').length, 3);
  assert.ok(receipt.provenance.artifacts.every((a) => a.downloadIntegrity === a.integrity));
  assert.deepEqual(f.calls.find((c) => c.argv[0] === 'install').argv.slice(5), ['@egregore/nexus']);
});

test('wrong native host is refused before any command, install, or temp directory creation', async (t) => {
  const { runSmoke } = await load();
  for (const host of [{ platform: 'linux', arch: 'arm64' }, { platform: 'darwin', arch: 'x64' }]) {
    const f = fixture(t);
    const before = fs.readdirSync(f.base);
    assert.throws(() => runSmoke(f.options, { ...f.deps, ...host }), /native.*darwin|architecture/i);
    assert.equal(f.calls.length, 0);
    assert.deepEqual(fs.readdirSync(f.base).filter((p) => p !== 'receipt.json'), before);
  }
});

test('missing candidate input is refused without invoking npm install', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t);
  fs.unlinkSync(join(f.options.tarballs, 'egregore-nexus-cli-0.1.8.tgz'));
  assert.throws(() => runSmoke(f.options, f.deps), /tarball/);
  assert.equal(f.calls.some((c) => c.argv[0] === 'install'), false);
});

test('installed package versions and native paths cannot fall back outside the clean prefix', async (t) => {
  const { runSmoke } = await load();
  for (const mutate of [
    ({ scope }) => fs.writeFileSync(join(scope, 'nexus-cli/package.json'), JSON.stringify({ name: '@egregore/nexus-cli', version: '0.1.7' })),
    ({ native }) => { fs.unlinkSync(native); fs.symlinkSync(process.execPath, native); },
    ({ native }) => fs.writeFileSync(native, 'ELF-not-mach-o'),
  ]) {
    const f = fixture(t, { mutate });
    assert.throws(() => runSmoke(f.options, f.deps), /version|escape|Mach-O/);
    assert.equal(f.calls.some((c) => c.command.endsWith('/bin/nexus')), false);
    assert.equal(JSON.parse(fs.readFileSync(f.options.output)).status, 'failed');
  }
});

test('existing output is never overwritten', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t);
  fs.writeFileSync(f.options.output, 'keep');
  assert.throws(() => runSmoke(f.options, f.deps), /output.*exist/);
  assert.equal(fs.readFileSync(f.options.output, 'utf8'), 'keep');
  assert.equal(f.calls.length, 0);
});

test('strict wrapper output and lipo architecture mismatches fail the receipt', async (t) => {
  const { runSmoke } = await load();
  for (const alteration of ['stderr', 'version', 'arch']) {
    const f = fixture(t);
    const original = f.deps.run;
    f.deps.run = (...a) => {
      const result = original(...a);
      if (a[0].endsWith('/bin/nexus') && alteration === 'stderr') result.stderr = 'unexpected warning\n';
      if (a[0].endsWith('/bin/nexus') && alteration === 'version') result.stdout = 'nexus 0.1.7\n';
      if (a[0] === '/usr/bin/lipo' && alteration === 'arch') result.stdout = 'x86_64\n';
      return result;
    };
    assert.throws(() => runSmoke(f.options, f.deps), /version|stderr|architecture/);
  }
});

test('registry rejects mismatched tarball SHA-512, nonpublic URLs, and installed lock tampering', async (t) => {
  const { runSmoke } = await load();
  for (const alteration of ['digest', 'url', 'lock']) {
    const f = fixture(t, { mode: 'registry', mutate: alteration === 'lock' ? ({ globalRoot }) => {
      fs.writeFileSync(join(globalRoot, '.package-lock.json'), JSON.stringify({ packages: {
        '@egregore/nexus': { version: '0.1.8', integrity: 'sha512-tampered' },
      } }));
    } : undefined });
    const original = f.deps.run;
    f.deps.run = (...a) => {
      const result = original(...a);
      if (a[1][0] === 'view' && alteration !== 'lock') {
        const dist = JSON.parse(result.stdout);
        if (alteration === 'digest') dist.integrity = `sha512-${Buffer.alloc(64).toString('base64')}`;
        else dist.tarball = 'https://secret:password@private.invalid/a.tgz';
        result.stdout = JSON.stringify(dist);
      }
      return result;
    };
    assert.throws(() => runSmoke(f.options, f.deps), /integrity mismatch|credential-free/);
    assert.equal(JSON.parse(fs.readFileSync(f.options.output)).status, 'failed');
  }
});

test('installed lock integrity is compared when global npm supplies it', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t, { mutate: ({ globalRoot }) => {
    const packages = {};
    for (const name of ['nexus', 'nexus-cli', 'nexus-gateway']) packages[`@egregore/${name}`] = {
      version: '0.1.8', integrity: `sha512-${createHash('sha512').update(`unit-only-${name}`).digest('base64')}`,
    };
    fs.writeFileSync(join(globalRoot, '.package-lock.json'), JSON.stringify({ packages }));
  } });
  const receipt = runSmoke(f.options, f.deps);
  assert.equal(receipt.provenance.installedLock.status, 'available');
  assert.equal(receipt.provenance.installedLock.checks.length, 3);
  assert.ok(receipt.provenance.installedLock.checks.every((c) => c.integrityCompared));
});

test('resolution selects the CLI actually nested under the installed umbrella', async (t) => {
  const { runSmoke } = await load();
  const f = fixture(t, { mutate: ({ scope }) => {
    const nested = join(scope, 'nexus/node_modules/@egregore/nexus-cli');
    fs.mkdirSync(join(nested, '..'), { recursive: true });
    fs.renameSync(join(scope, 'nexus-cli'), nested);
    fs.mkdirSync(join(scope, 'nexus-cli'));
    fs.writeFileSync(join(scope, 'nexus-cli/package.json'), JSON.stringify({ name: '@egregore/nexus-cli', version: '0.0.1' }));
  } });
  const receipt = runSmoke(f.options, f.deps);
  assert.match(receipt.packages.find((p) => p.name === '@egregore/nexus-cli').directory, /nexus\/node_modules\/@egregore\/nexus-cli$/);
  assert.match(receipt.native.realPath, /nexus\/node_modules\/@egregore\/nexus-cli\/native/);
});

test('Rosetta translation and unexpected sysctl failures are refused before npm installation', async (t) => {
  const { runSmoke } = await load();
  for (const result of [
    { status: 0, stdout: '1\n', stderr: '' },
    { status: 1, stdout: '', stderr: 'permission denied\n' },
    { status: 0, stdout: 'unknown\n', stderr: '' },
  ]) {
    const f = fixture(t, { arch: 'x64' });
    const original = f.deps.run;
    f.deps.run = (...a) => a[0] === '/usr/sbin/sysctl' ? result : original(...a);
    assert.throws(() => runSmoke(f.options, f.deps), /Rosetta|translation/);
    assert.equal(f.calls.some((c) => c.argv[0] === 'install'), false);
  }
});

test('real Intel absent translation key is recorded without treating arbitrary errors as absent', async (t) => {
  const { runSmoke } = await load();
  for (const result of [
    { status: 0, stdout: '', stderr: '' },
    { status: 1, stdout: '', stderr: '' },
    { status: 1, stdout: '', stderr: "sysctl: unknown oid 'sysctl.proc_translated'\n" },
  ]) {
    const f = fixture(t, { arch: 'x64' });
    const original = f.deps.run;
    f.deps.run = (...a) => a[0] === '/usr/sbin/sysctl' ? result : original(...a);
    const receipt = runSmoke(f.options, f.deps);
    assert.equal(receipt.host.translation.state, 'absent');
  }
});
