#!/usr/bin/env bash
set -u

if [ -z "${NEXUS_NAME:-}" ] || [ -z "${NEXUS_CLIENT_KEY:-}" ]; then
  echo "Nexus bootstrap skipped: NEXUS_NAME or NEXUS_CLIENT_KEY is missing" >&2
  exit 0
fi

NEXUS_CLI=${NEXUS_CLI:-nexus}
"$NEXUS_CLI" register \
  --name "$NEXUS_NAME" \
  --agent "${NEXUS_AGENT:-other}" \
  --project "${NEXUS_PROJECT:-default}" \
  --client-key "$NEXUS_CLIENT_KEY" >/dev/null
