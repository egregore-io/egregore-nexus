#!/usr/bin/env bash
# Generate gateway/src/shared/types/contracts.gen.ts from the nexus-contracts crate.
#
# Two steps: (1) run the typeshare CLI over src/ (config maps i64/u32/f32/Value), (2) append the
# hand-authored wire unions in gen/postamble.ts (RequestIdWire / SendTargetWire / WsEventWire) that
# typeshare 1.13 cannot emit for internally-tagged / untagged serde enums. The Rust types reference
# these aliases via #[typeshare(serialized_as = "...")]. Re-run after any contract change.
#
# Requires: `cargo install typeshare-cli` (pin a version in CI).
set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$CRATE_DIR/../../../gateway/src/shared/types/contracts.gen.ts"

mkdir -p "$(dirname "$OUT")"

typeshare "$CRATE_DIR/src" \
  --lang=typescript \
  --config-file="$CRATE_DIR/typeshare.toml" \
  --output-file="$OUT"

printf '\n' >> "$OUT"
cat "$CRATE_DIR/gen/postamble.ts" >> "$OUT"

# typeshare 1.13 can render blank rustdoc separators as ` * `, which makes the canonical generated
# file fail `git diff --check`. Normalize generator-owned trailing whitespace here; never hand-edit
# the generated TypeScript.
TMP_OUT="${OUT}.tmp"
sed 's/[[:space:]]*$//' "$OUT" > "$TMP_OUT"
mv "$TMP_OUT" "$OUT"

echo "wrote $OUT"
