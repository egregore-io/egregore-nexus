import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const REQUIRED_EVENTS = {
  'codex.headed': ['thread/start', 'thread/resume'],
  'codex.acp': ['session/new', 'session/load'],
  'claude.headed': ['transcript/assistant'],
  'claude.acp': ['session/new', 'session/load'],
  'opencode.headed': ['message.updated'],
  'opencode.acp': ['session/new', 'session/load'],
  'hermes.headed': ['session/model'],
  'hermes.acp': ['session/new', 'session/load'],
};
const ROW_FIELDS = ['id', 'version', 'artifactSha256', 'sourcePath', 'nativeFixtureSha256', 'meaning', 'proofKind'];
const MEANINGS = ['configured', 'turn_selected', 'response_reported'];
const PROOF_KINDS = ['artifact_serialization', 'isolated_protocol_capture'];
const SHA256 = /^[a-f0-9]{64}$/;
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const nonblank = (value) => typeof value === 'string' && value.trim().length > 0;

/**
 * Return integrity/schema errors; [] means valid, NOT permission to enable a mode.
 * Inputs are parsed JSON documents. Native-row hash canonicalization is exactly:
 * SHA256(Buffer.from(JSON.stringify(native.rows[id]))), using UTF-8 and lowercase
 * hexadecimal output. Property order is preserved, not sorted; whitespace in the
 * input JSON is irrelevant. artifactSha256 is format-checked only: sourcePath and
 * artifact provenance require a separate review, not filesystem access here.
 * Event coverage and nonempty object payloads do not prove successful native
 * execution. Adapter-native parsing and model semantics belong in codec tests.
 */
export function validateModelFixtures(manifest, native) {
  const errors = [];
  function objectWithKeys(value, keys, label) {
    if (!isObject(value)) {
      errors.push(`${label} must be an object`);
      return false;
    }
    for (const key of Object.keys(value)) {
      if (!keys.includes(key)) errors.push(`${label}: unknown key ${key}`);
    }
    return true;
  }

  const manifestObject = objectWithKeys(manifest, ['version', 'rows'], 'manifest');
  const nativeObject = objectWithKeys(native, ['version', 'rows'], 'native');
  if (manifestObject && manifest.version !== 1) errors.push('manifest.version must be 1');
  if (nativeObject && native.version !== 1) errors.push('native.version must be 1');
  const manifestRows = manifestObject && Array.isArray(manifest.rows);
  const nativeRows = nativeObject && isObject(native.rows);
  if (manifestObject && !manifestRows) errors.push('manifest.rows must be a nonempty array');
  if (nativeObject && !nativeRows) errors.push('native.rows must be an object');

  const seen = new Set();
  const rowsById = new Map();
  if (manifestRows) {
    if (manifest.rows.length === 0) errors.push('manifest.rows must be a nonempty array');
    for (const [index, row] of manifest.rows.entries()) {
      const label = `manifest.rows[${index}]`;
      if (!objectWithKeys(row, ROW_FIELDS, label)) continue;
      for (const field of ROW_FIELDS) {
        if (!nonblank(row[field])) errors.push(`${label}.${field} must be a nonempty string`);
      }
      for (const field of ['artifactSha256', 'nativeFixtureSha256']) {
        if (typeof row[field] !== 'string' || !SHA256.test(row[field])) {
          errors.push(`${label}.${field} must be 64 lowercase hexadecimal characters`);
        }
      }
      if (!MEANINGS.includes(row.meaning)) errors.push(`${label}.meaning must be one of ${MEANINGS.join(', ')}`);
      if (!PROOF_KINDS.includes(row.proofKind)) errors.push(`${label}.proofKind must be one of ${PROOF_KINDS.join(', ')}`);
      if (!nonblank(row.id)) continue;
      if (!Object.hasOwn(REQUIRED_EVENTS, row.id)) {
        errors.push(`${label}.id is unknown: ${row.id}`);
        continue;
      }
      if (seen.has(row.id)) errors.push(`manifest: duplicate row ${row.id}`);
      seen.add(row.id);
      rowsById.set(row.id, row);
    }
    for (const id of Object.keys(REQUIRED_EVENTS)) {
      if (!seen.has(id)) errors.push(`manifest: missing row ${id}`);
    }
  }

  if (nativeRows) {
    for (const id of Object.keys(native.rows)) {
      if (!Object.hasOwn(REQUIRED_EVENTS, id)) errors.push(`native.rows: unknown row ${id}`);
    }
    for (const [id, required] of Object.entries(REQUIRED_EVENTS)) {
      const label = `native.rows[${id}]`;
      if (!Object.hasOwn(native.rows, id)) {
        errors.push(`native.rows: missing row ${id}`);
        continue;
      }
      const row = native.rows[id];
      if (!objectWithKeys(row, ['events'], label)) continue;
      const present = new Set();
      if (!Array.isArray(row.events) || row.events.length === 0) {
        errors.push(`${label}.events must be a nonempty array`);
      }
      if (Array.isArray(row.events)) {
        for (const [index, event] of row.events.entries()) {
          const eventLabel = `${label}.events[${index}]`;
          if (!objectWithKeys(event, ['kind', 'payload'], eventLabel)) continue;
          if (!nonblank(event.kind)) errors.push(`${eventLabel}.kind must be a nonempty string`);
          if (!isObject(event.payload) || Object.keys(event.payload).length === 0) {
            errors.push(`${eventLabel}.payload must be a nonempty object`);
          } else if (nonblank(event.kind)) {
            present.add(event.kind);
          }
        }
      }
      for (const kind of required) {
        if (!present.has(kind)) errors.push(`${label}: missing required event ${kind} with object payload`);
      }
      const manifestRow = rowsById.get(id);
      if (manifestRow) {
        const digest = createHash('sha256').update(Buffer.from(JSON.stringify(row))).digest('hex');
        if (manifestRow.nativeFixtureSha256 !== digest) errors.push(`${id}: nativeFixtureSha256 does not match native row payload`);
      }
    }
  }
  return errors;
}

const HELP = `Usage: node model-reporting-fixtures.mjs --artifacts PATH --native PATH

Check a version-1 artifact manifest and version-1 native fixture document.
--artifacts PATH  JSON manifest with rows containing id, version, artifactSha256,
                  sourcePath, nativeFixtureSha256, meaning, and proofKind.
--native PATH     JSON document with rows keyed by harness.mode, each containing
                  a nonempty events array of {kind, payload} records.
--help            Show help without validating options or reading input paths.

Required rows: ${Object.keys(REQUIRED_EVENTS).join(', ')}.
Required events: ACP session/new + session/load; headed Codex thread/start +
thread/resume; headed Claude transcript/assistant; headed OpenCode message.updated;
headed Hermes session/model. Payloads must be nonempty objects.
Meaning: ${MEANINGS.join(', ')}.
Proof kind: ${PROOF_KINDS.join(', ')}.

nativeFixtureSha256 = SHA256(Buffer.from(JSON.stringify(native.rows[id]))).
Use UTF-8 and lowercase hex. Object property order is preserved, not sorted;
original JSON whitespace is ignored. artifactSha256 is format-checked only.
This checks integrity and required event coverage, not successful native execution
or native provenance. Review source artifacts separately; adapter codec tests
must check native payload semantics. Passing this checker does not enable a mode.`;

function main(args) {
  // Help takes precedence even over malformed options, before all path access.
  if (args.includes('--help')) {
    console.log(HELP);
    return 0;
  }
  const options = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const option = args[index];
    if (!['--artifacts', '--native'].includes(option)) throw new Error(`Unknown option: ${option}`);
    if (options.has(option)) throw new Error(`Duplicate option: ${option}`);
    const path = args[index + 1];
    if (!nonblank(path) || path.startsWith('--')) throw new Error(`${option} requires a path`);
    options.set(option, path);
  }
  for (const option of ['--artifacts', '--native']) {
    if (!options.has(option)) throw new Error(`Missing required option: ${option}`);
  }
  function readJson(option) {
    const path = options.get(option);
    let contents;
    try {
      contents = readFileSync(path, 'utf8');
    } catch (error) {
      throw new Error(`Cannot read ${option} at ${path}: ${error.message}`);
    }
    try {
      return JSON.parse(contents);
    } catch (error) {
      throw new Error(`Invalid JSON for ${option} at ${path}: ${error.message}`);
    }
  }
  const errors = validateModelFixtures(readJson('--artifacts'), readJson('--native'));
  if (errors.length > 0) {
    console.error(errors.map((error) => `Error: ${error}`).join('\n'));
    return 1;
  }
  console.log('Fixture integrity validated for 8 rows; this does not establish native provenance or enable any mode.');
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    process.exitCode = main(process.argv.slice(2));
  } catch (error) {
    console.error(`Error: ${error.message}`);
    process.exitCode = 1;
  }
}
