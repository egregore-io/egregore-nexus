#!/usr/bin/env bash
# Local notification-sources acceptance smoke (offline, hermetic): builds the daemon, spins up an
# isolated embedded-store daemon, registers a source, subscribes an external pull consumer, runs the
# LOCAL example, and asserts the event is received. The remote edge (custom_app_remote.{py,mjs})
# needs the gateway running and is exercised separately; this proves the trusted-local path end to
# end without touching ~/.nexus or a running host daemon.
#
#   bash examples/sources/_smoke.sh
#
set -uo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
CORE=$REPO/core
TARGET_DIR=${CARGO_TARGET_DIR:-$CORE/target}
BIN=$TARGET_DIR/debug/nexus
TMPD=$(mktemp -d "${TMPDIR:-/tmp}/notif-smoke-XXXX")
GATEWAY_PORT=$(( 24000 + RANDOM % 2000 ))
export NEXUS_HOME=$TMPD/nexus-home
export NEXUS_DB_PATH=$NEXUS_HOME/nexus.db
export NEXUS_STREAM_DB_PATH=$TMPD/stream.db
export NEXUS_GATEWAY_BIND=127.0.0.1
export NEXUS_GATEWAY_PORT=$GATEWAY_PORT
export NEXUS_NO_AUTOSTART=1
# Debug builds need a larger Tokio worker stack; release binaries are unaffected.
export RUST_MIN_STACK=${RUST_MIN_STACK:-33554432}
# This smoke starts as the isolated local operator. Do not inherit the invoking agent/harness
# identity: a developer running it from an active Nexus session would otherwise submit setup
# commands under that unrelated client key and fail the caller-registration gate.
unset NEXUS_NAME NEXUS_CLIENT_KEY NEXUS_PROJECT NEXUS_AGENT NEXUS_AGENT_ID NEXUS_TIER NEXUS_KIND
unset NEXUS_SESSION_ID CLAUDE_CODE_SESSION_ID NEXUS_RUNTIME_CREDENTIAL
cleanup() {
  for pid in "${LISTENER:-}" "${DAEMON:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$TMPD"
}
trap cleanup EXIT
fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }

echo "1) build daemon"
( cd "$CORE" && CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p egregore-nexus >/dev/null 2>&1 ) || fail "build"
[ -x "$BIN" ] || fail "no binary"

echo "2) start isolated embedded-store daemon"
mkdir -p "$NEXUS_HOME"
"$BIN" daemon run >"$TMPD/daemon.log" 2>&1 &
DAEMON=$!
for _ in $(seq 1 100); do
  "$BIN" daemon status >/dev/null 2>&1 && break
  sleep 0.1
done
"$BIN" daemon status >/dev/null 2>&1 || { cat "$TMPD/daemon.log"; fail "daemon never became ready"; }

echo "3) operator registers source myapp --topic deploys"
"$BIN" source register myapp --topic deploys >/dev/null || fail "register source"

echo "4) register external listener and subscribe to deploys"
NEXUS_NAME=listener NEXUS_AGENT=other NEXUS_PROJECT=smoke NEXUS_CLIENT_KEY=ck_listener \
  "$BIN" register --name listener --agent other --project smoke --client-key ck_listener \
  >/dev/null || fail "register listener"
NEXUS_NAME=listener NEXUS_AGENT=other NEXUS_PROJECT=smoke NEXUS_CLIENT_KEY=ck_listener \
  "$BIN" subscribe deploys >/dev/null || fail "subscribe"

echo "5) start the listener, then run the LOCAL example (nexus push myapp --json)"
NEXUS_NAME=listener NEXUS_AGENT=other NEXUS_PROJECT=smoke NEXUS_CLIENT_KEY=ck_listener \
  "$BIN" --json listen --timeout-ms 5000 --max 10 --once >"$TMPD/listen.json" \
  2>"$TMPD/listen.err" &
LISTENER=$!
sleep 0.5
PATH="$TARGET_DIR/debug:$PATH" python3 "$HERE/custom_app_local.py" \
  >"$TMPD/example.out" 2>"$TMPD/example.err" || {
    cat "$TMPD/example.err"
    fail "local example"
  }
wait "$LISTENER" || {
  cat "$TMPD/listen.err"
  fail "listener"
}
LISTENER=

echo "6) listener must contain the example's deploy event exactly once"
python3 - "$TMPD/listen.json" <<'PY' || fail "expected one received deploy event"
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
items = payload.get("dms", []) + payload.get("threads", [])
matches = [item for item in items if item.get("body", "").startswith("web v1.4.2 → prod")]
if len(matches) != 1:
    raise SystemExit(f"received {len(matches)} matching events: {payload}")
print(f"   received message_id={matches[0]['id']} kind={matches[0]['kind']}")
PY

echo "SMOKE PASS — local source push reaches the subscribed pull consumer exactly once."
