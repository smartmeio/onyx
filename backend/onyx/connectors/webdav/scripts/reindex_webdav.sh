#!/usr/bin/env bash
# LOCAL DEV ONLY - do not commit (excluded via .git/info/exclude).
#
# Starts a full re-index of every WebDAV connector. Polling does not see a
# renamed subfolder (see README, section 12.6), so a nightly full re-index
# brings those paths up to date. Unchanged files are skipped by the content
# hash, so only the downloads are repeated, not the embeddings.
#
# Auth: an Onyx Personal Access Token of an admin user
# (Settings > Access Tokens). Service account API keys need a paid plan.
# The token is ONYX_REINDEX_KEY in deployment/docker_compose/.env (gitignored).
# An ONYX_REINDEX_KEY already set in the environment wins over the .env.
# Only that one line is read: the .env is not sourced.
#
# Usage:
#     ./reindex_webdav.sh            # all WebDAV connectors
#     ONYX_URL=http://localhost:8088 ./reindex_webdav.sh
#
# Cron (every night at 03:00):
#     0 3 * * * .../webdav/scripts/reindex_webdav.sh >> "$HOME/Library/Logs/onyx-webdav-reindex.log" 2>&1

set -euo pipefail

ONYX_URL="${ONYX_URL:-http://localhost:3000}"
# scripts -> webdav -> connectors -> onyx -> backend -> repository root
REPO_ROOT="$(cd "$(dirname "$0")/../../../../.." && pwd)"
ENV_FILE="$REPO_ROOT/deployment/docker_compose/.env"

log() {  # Print one line with a timestamp, so the cron log is readable.
    echo "$(date '+%Y-%m-%d %H:%M:%S') $*"
}

TOKEN="${ONYX_REINDEX_KEY:-}"
if [[ -z "$TOKEN" && -r "$ENV_FILE" ]]; then
    # Last ONYX_REINDEX_KEY= line, without optional quotes.
    TOKEN="$(grep -E '^ONYX_REINDEX_KEY=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d "\"'[:space:]" || true)"
fi
if [[ -z "$TOKEN" ]]; then
    log "ERROR: ONYX_REINDEX_KEY not found in the environment or in $ENV_FILE. Create a Personal Access Token in Onyx (Settings > Access Tokens) and add ONYX_REINDEX_KEY=<token> to the .env."
    exit 1
fi

api() {  # Call the Onyx API with the token. Fails on HTTP errors and prints the response body.
    curl -sS --fail-with-body --max-time 60 \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        "$@"
}

if ! connectors_json="$(api "$ONYX_URL/api/manage/admin/connector")"; then
    log "ERROR: cannot list the connectors at $ONYX_URL: $connectors_json"
    exit 1
fi

# One line per WebDAV connector: "<connector_id> <credential_ids as JSON> <name>".
targets="$(python3 -c '
import json, sys
for c in json.load(sys.stdin):
    if c.get("source") == "webdav" and c.get("credential_ids"):
        print(c["id"], json.dumps(c["credential_ids"]).replace(" ", ""), c.get("name", ""))
' <<< "$connectors_json")"

if [[ -z "$targets" ]]; then
    log "No WebDAV connector with a credential found. Nothing to do."
    exit 0
fi

failed=0
while read -r connector_id credential_ids name; do
    body="{\"connector_id\": $connector_id, \"credential_ids\": $credential_ids, \"from_beginning\": true}"
    if response="$(api -X POST "$ONYX_URL/api/manage/admin/connector/run-once" -d "$body")"; then
        log "OK: full re-index started for connector $connector_id ($name): $response"
    else
        log "ERROR: connector $connector_id ($name): $response"
        failed=1
    fi
done <<< "$targets"

exit "$failed"
