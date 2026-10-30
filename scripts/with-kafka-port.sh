#!/usr/bin/env bash
# Run a command while `kubectl port-forward svc/kafka 19092:19092` is alive, then stop the forward.
#
#   scripts/with-kafka-port.sh python scripts/consumer.py --seconds 8
#
# If something already listens on 19092 (your own port-forward), the command runs as is.
set -euo pipefail

KUBECTL=${KUBECTL:-kubectl --context flyte-devbox}
PORT=19092

if nc -z localhost "$PORT" 2>/dev/null; then
  exec "$@"
fi

$KUBECTL -n event-export port-forward svc/kafka "$PORT:$PORT" >/dev/null 2>&1 &
forward_pid=$!
trap 'kill "$forward_pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  nc -z localhost "$PORT" 2>/dev/null && break
  sleep 0.5
done
nc -z localhost "$PORT" 2>/dev/null || { echo "port-forward to kafka did not come up" >&2; exit 1; }

"$@"
