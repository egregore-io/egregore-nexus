import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { validateModelFixtures } from './model-reporting-fixtures.mjs';

const checkerPath = fileURLToPath(new URL('./model-reporting-fixtures.mjs', import.meta.url));
const requiredEvents = {
  'codex.headed': ['thread/start', 'thread/resume'],
  'codex.acp': ['session/new', 'session/load'],
  'claude.headed': ['transcript/assistant'],
  'claude.acp': ['session/new', 'session/load'],
  'opencode.headed': ['message.updated'],
  'opencode.acp': ['session/new', 'session/load'],
  'hermes.headed': ['session/model'],
  'hermes.acp': ['session/new', 'session/load'],
};
const hash = (value) => createHash('sha256').update(Buffer.from(JSON.stringify(value))).digest('hex');

// Deliberately synthetic unit data. These objects are NOT native wire captures,
// source-artifact evidence, adapter tests, or grounds to enable any mode.
function syntheticFixtures() {
  const native = { version: 1, rows: {} };
  const rows = Object.entries(requiredEvents).map(([id, kinds], index) => {
    native.rows[id] = { events: kinds.map((kind) => ({ kind, payload: { synthetic: true, arbitraryModel: 'not-a-catalog-id' } })) };
    return {
      id, version: 'synthetic-unit-version', artifactSha256: 'a'.repeat(64),
      sourcePath: 'synthetic/not-a-real-artifact', nativeFixtureSha256: hash(native.rows[id]),
      meaning: ['configured', 'turn_selected', 'response_reported'][index % 3],
      proofKind: index % 2 ? 'artifact_serialization' : 'isolated_protocol_capture',
    };
  });
  return { manifest: { version: 1, rows }, native };
}

function rehash({ manifest, native }) {
  for (const row of manifest.rows) {
    if (native.rows[row.id] !== undefined) row.nativeFixtureSha256 = hash(native.rows[row.id]);
  }
}

function rejects(name, change, pattern) {
  test(name, () => {
    const fixture = syntheticFixtures();
    change(fixture);
    const errors = validateModelFixtures(fixture.manifest, fixture.native);
    assert.ok(Array.isArray(errors));
    assert.ok(errors.length > 0, 'expected validation errors');
    assert.match(errors.join('\n'), pattern);
  });
}

test('accepts all eight synthetic rows without interpreting arbitrary model fields', () => {
  const { manifest, native } = syntheticFixtures();
  assert.deepEqual(validateModelFixtures(manifest, native), []);
});

rejects('rejects missing manifest row', ({ manifest }) => manifest.rows.pop(), /hermes\.acp/);
rejects('rejects extra manifest row', ({ manifest }) => manifest.rows.push({ ...manifest.rows[0], id: 'other.acp' }), /other\.acp/);
rejects('rejects duplicate manifest row', ({ manifest }) => manifest.rows.push({ ...manifest.rows[0] }), /duplicate.*codex\.headed/i);
rejects('rejects missing native row', ({ native }) => delete native.rows['hermes.acp'], /hermes\.acp/);
rejects('rejects extra native row', ({ native }) => { native.rows['other.acp'] = { events: [] }; }, /other\.acp/);

test('malformed JSON object id produces validation errors without throwing', () => {
  const { manifest, native } = syntheticFixtures();
  manifest.rows[0].id = { toString: 1 };
  let errors;
  assert.doesNotThrow(() => { errors = validateModelFixtures(manifest, native); });
  assert.match(errors.join('\n'), /id/);
});

for (const field of ['id', 'version', 'sourcePath', 'proofKind', 'meaning']) {
  for (const value of ['', '   ', null, 1]) {
    rejects(`rejects invalid ${field}: ${JSON.stringify(value)}`, ({ manifest }) => { manifest.rows[0][field] = value; }, new RegExp(field));
  }
}
for (const field of ['artifactSha256', 'nativeFixtureSha256']) {
  for (const value of ['', 'a'.repeat(63), 'A'.repeat(64), 'g'.repeat(64), 123, null]) {
    rejects(`rejects malformed ${field}: ${JSON.stringify(value)}`, ({ manifest }) => { manifest.rows[0][field] = value; }, new RegExp(field));
  }
}
rejects('rejects unknown proof kind', ({ manifest }) => { manifest.rows[0].proofKind = 'trust_me'; }, /proofKind/);
rejects('rejects unknown meaning', ({ manifest }) => { manifest.rows[0].meaning = 'native'; }, /meaning/);
rejects('rejects altered native payload', ({ native }) => { native.rows['codex.headed'].events[0].payload.changed = true; }, /codex\.headed.*nativeFixtureSha256/);

for (const [id, kinds] of Object.entries(requiredEvents)) {
  for (const kind of kinds) {
    rejects(`rejects ${id} missing ${kind}`, (fixture) => {
      fixture.native.rows[id].events = fixture.native.rows[id].events.filter((event) => event.kind !== kind);
      rehash(fixture);
    }, new RegExp(kind));
  }
}

for (const value of [null, [], 'bad', {}, { version: 2, rows: [] }, { version: 1, rows: {} }, { version: 1, rows: [] }]) {
  rejects(`rejects malformed manifest ${JSON.stringify(value)}`, (fixture) => { fixture.manifest = value; }, /manifest/);
}
for (const value of [null, [], 'bad', {}, { version: 2, rows: {} }, { version: 1, rows: [] }]) {
  rejects(`rejects malformed native ${JSON.stringify(value)}`, (fixture) => { fixture.native = value; }, /native/);
}
for (const value of [null, [], 'bad']) {
  rejects(`rejects malformed manifest row ${JSON.stringify(value)}`, ({ manifest }) => { manifest.rows[0] = value; }, /manifest/);
  rejects(`rejects malformed native row ${JSON.stringify(value)}`, ({ native }) => { native.rows['codex.headed'] = value; }, /codex\.headed/);
}
for (const value of [undefined, null, {}, 'bad', []]) {
  rejects(`rejects malformed events ${JSON.stringify(value)}`, (fixture) => {
    fixture.native.rows['codex.headed'].events = value;
    rehash(fixture);
  }, /events/);
}
for (const value of [null, [], 'bad', {}, { kind: '', payload: { synthetic: true } }]) {
  rejects(`rejects malformed event ${JSON.stringify(value)}`, (fixture) => {
    fixture.native.rows['codex.headed'].events.push(value);
    rehash(fixture);
  }, /event/);
}
for (const value of [undefined, null, [], 'bad', 1, {}]) {
  rejects(`rejects invalid payload ${JSON.stringify(value)}`, (fixture) => {
    fixture.native.rows['codex.headed'].events[0].payload = value;
    rehash(fixture);
  }, /payload/);
}
for (const target of ['manifest', 'manifestRow', 'native', 'nativeRow', 'event']) {
  rejects(`rejects unknown ${target} key`, (fixture) => {
    const objects = { manifest: fixture.manifest, manifestRow: fixture.manifest.rows[0], native: fixture.native, nativeRow: fixture.native.rows['codex.headed'], event: fixture.native.rows['codex.headed'].events[0] };
    objects[target].unexpected = true;
    rehash(fixture);
  }, /unexpected/);
}

function cli(args) {
  return spawnSync(process.execPath, [checkerPath, ...args], { encoding: 'utf8' });
}

function withFiles(run) {
  const directory = mkdtempSync(join(tmpdir(), 'model-fixture-unit-'));
  try {
    const fixture = syntheticFixtures();
    const artifacts = join(directory, 'artifacts.json');
    const native = join(directory, 'native.json');
    writeFileSync(artifacts, JSON.stringify(fixture.manifest));
    writeFileSync(native, JSON.stringify(fixture.native));
    run({ directory, artifacts, native });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test('--help succeeds before validating any other arguments or reading their paths', () => {
  const result = spawnSync(process.execPath, [checkerPath, '--artifacts', '/missing/artifacts.json', '--native', '/missing/native.json', '--unknown', '--help'], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Usage:/);
  assert.equal(result.stderr, '');
});

test('CLI accepts synthetic integrity data and states provenance limitation', () => withFiles(({ artifacts, native }) => {
  const result = cli(['--artifacts', artifacts, '--native', native]);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /integrity/i);
  assert.match(result.stdout, /not.*native.*provenance/i);
}));

test('help documents hash canonicalization and required-event-only scope', () => {
  const result = cli(['--help']);
  assert.equal(result.status, 0);
  assert.match(result.stdout, /JSON\.stringify\(native\.rows\[id\]\)/);
  assert.match(result.stdout, /successful native execution/i);
});

for (const args of [[], ['--artifacts'], ['--native'], ['--artifacts', 'x'], ['--native', 'x'], ['--wat'], ['x'], ['--artifacts', '--native', 'x'], ['--artifacts', 'x', '--native', 'y', '--native', 'z'], ['--artifacts', 'x', '--artifacts', 'y', '--native', 'z']]) {
  test(`CLI rejects invalid arguments ${JSON.stringify(args)}`, () => {
    const result = cli(args);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /missing|unknown|duplicate|requires/i);
    assert.doesNotMatch(result.stderr, /at .+\.mjs:/);
  });
}
for (const option of ['artifacts', 'native']) {
  for (const problem of ['missing', 'directory', 'invalid JSON']) {
    test(`CLI reports ${option} ${problem}`, () => withFiles((paths) => {
      let path = paths[option];
      if (problem === 'missing') path = join(paths.directory, 'missing.json');
      if (problem === 'directory') path = paths.directory;
      if (problem === 'invalid JSON') writeFileSync(path, '{invalid-json');
      const result = cli(['--artifacts', option === 'artifacts' ? path : paths.artifacts, '--native', option === 'native' ? path : paths.native]);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, new RegExp(option));
      assert.match(result.stderr, /read|JSON/i);
      assert.doesNotMatch(result.stderr, /at .+\.mjs:/);
    }));
  }
}

test('CLI rejects invalid manifest with a useful error', () => withFiles(({ artifacts, native }) => {
  writeFileSync(artifacts, JSON.stringify({ version: 1, rows: [] }));
  const result = cli(['--artifacts', artifacts, '--native', native]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /manifest/);
}));
