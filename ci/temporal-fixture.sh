#!/usr/bin/env bash
# Run a command against a seeded, throwaway Temporal dev server.
#
#   ci/temporal-fixture.sh <command...>
#
# Starts `temporal server start-dev` (in-memory, headless, on a free port, with
# the CustomerTier/OrderCount search attributes and a second namespace),
# seeds it with test/seeder, exports TEMPORAL_FIXTURE_HOST=127.0.0.1:<port>,
# runs the command, then stops the server by PID. If TEMPORAL_FIXTURE_HOST is
# already set, the command just runs against that server.
#
# The Temporal CLI comes from ci/fetch-temporal-cli.sh (pinned + SHA-256
# checked), or from $TEMPORAL_CLI.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
if [ -n "${TEMPORAL_FIXTURE_HOST:-}" ]; then exec "$@"; fi

CLI="${TEMPORAL_CLI:-$("$REPO/ci/fetch-temporal-cli.sh")}"
SEEDER="$REPO/.tools/bin/temporal-seeder"
if [ ! -x "$SEEDER" ] || [ -n "$(find "$REPO/test/seeder" -newer "$SEEDER" -name '*.go' -o -newer "$SEEDER" -name 'go.mod')" ]; then
  (cd "$REPO/test/seeder" && CGO_ENABLED=0 go build -o "$SEEDER" .)
fi

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
PORT="$(free_port)"
LOG="$(mktemp -t vgi-temporal-dev.XXXXXX)"
"$CLI" server start-dev --headless --ip 127.0.0.1 --port "$PORT" \
  --http-port "$(free_port)" --metrics-port "$(free_port)" --log-level error \
  --namespace analytics \
  --search-attribute CustomerTier=Keyword --search-attribute OrderCount=Int \
  >"$LOG" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  rm -f "$LOG"
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 120); do
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "temporal dev server exited:"; cat "$LOG"; exit 1; }
  if "$CLI" operator namespace describe --address "127.0.0.1:$PORT" -n default >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "temporal dev server never became ready:"; cat "$LOG"; exit 1; }

"$SEEDER" -address "127.0.0.1:$PORT"
export TEMPORAL_FIXTURE_HOST="127.0.0.1:$PORT"
echo "temporal fixture at $TEMPORAL_FIXTURE_HOST (pid $SERVER_PID)" >&2
"$@"
