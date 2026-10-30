#!/usr/bin/env bash
# Verify one Flyte run across the three hops of the pipeline.
#
#   scripts/verify.sh <run-name> <expected ActionSucceeded count>
#
# Hop 1  k8s API      the executor created Events on the TaskActions
# Hop 2  exporter     the exporter received them and discarded none
# Hop 3  Kafka        a consumer reads them back, with flyte.org/* annotations intact
#
# Exit code 0 when every hop passes.
set -uo pipefail

run=${1:?usage: verify.sh <run-name> <expected-succeeded>}
expected_succeeded=${2:?usage: verify.sh <run-name> <expected-succeeded>}
KUBECTL=${KUBECTL:-kubectl --context flyte-devbox}
PY=${PY:-uv run --no-project --with confluent-kafka python -I}
here=$(cd "$(dirname "$0")" && pwd)

fail=0
pass() { printf '  PASS  %s\n' "$1"; }
flunk() { printf '  FAIL  %s\n' "$1"; fail=1; }

echo "== run $run"

# ---- Hop 1: k8s Events -------------------------------------------------------
# Give the run up to 2 minutes to reach its terminal events.
for _ in $(seq 1 24); do
  events=$($KUBECTL get events -n flyte --field-selector involvedObject.kind=TaskAction -o json \
    | jq -c --arg run "$run" '[.items[] | select(.metadata.annotations["flyte.org/run-name"] == $run)]')
  terminal=$(jq 'map(select(.reason == "ActionSucceeded" or .reason == "ActionFailed")) | length' <<<"$events")
  if [ "$terminal" -ge 1 ] && [ "$(jq 'map(select(.reason == "ActionSucceeded")) | length' <<<"$events")" -ge "$expected_succeeded" ]; then
    break
  fi
  sleep 5
done

total=$(jq length <<<"$events")
succeeded=$(jq 'map(select(.reason == "ActionSucceeded")) | length' <<<"$events")
failed=$(jq 'map(select(.reason == "ActionFailed")) | length' <<<"$events")
running_with_info=$(jq 'map(select(.reason == "ActionRunning" and .metadata.annotations["flyte.org/info"] != null)) | length' <<<"$events")
running=$(jq 'map(select(.reason == "ActionRunning")) | length' <<<"$events")

echo "-- hop 1: k8s Events on TaskActions"
echo "     total=$total succeeded=$succeeded failed=$failed running=$running"
[ "$succeeded" -eq "$expected_succeeded" ] && pass "ActionSucceeded = $expected_succeeded" || flunk "ActionSucceeded = $succeeded, expected $expected_succeeded"
if [ "$running" -gt 0 ]; then
  # Only an executor with PR #8116 (info events created directly) puts flyte.org/info on ActionRunning.
  [ "$running_with_info" -eq "$running" ] && pass "ActionRunning events carry flyte.org/info (executor image has the PR)" \
                                           || flunk "only $running_with_info/$running ActionRunning events carry flyte.org/info (old executor image?)"
fi

# ---- Hop 2: exporter -----------------------------------------------------------
logs=$($KUBECTL -n event-export logs deploy/event-exporter 2>/dev/null)
# Only Action* events on this run's TaskActions; the exporter also logs Pod events (Scheduled, Pulled, ...).
received=$(grep '"message":"Received event"' <<<"$logs" | grep '"reason":"Action' | grep -c "\"involvedObject\":\"$run-" || true)
discarded=$(grep 'discarded' <<<"$logs" | grep -c "\"event name\":\"$run-" || true)
throttled=$(grep -c 'client-side throttling' <<<"$logs" || true)

echo "-- hop 2: exporter"
echo "     received=$received discarded=$discarded throttled-log-lines=$throttled"
[ "$received" -ge "$total" ] && pass "exporter received every event ($received >= $total)" || flunk "exporter received $received of $total events"
[ "$discarded" -eq 0 ] && pass "nothing discarded as too old" || flunk "$discarded events discarded (raise kubeQPS / maxEventAgeSeconds)"

# ---- Hop 3: Kafka ----------------------------------------------------------------
out=$("$here/with-kafka-port.sh" $PY "$here/consumer.py" --run "$run" --seconds 8 2>&1)
kafka_total=$(grep -c '^p' <<<"$out" || true)
kafka_succeeded=$(awk '/== messages per reason/{f=1;next} f && / ActionSucceeded$/{print $1} /^$/{f=0}' <<<"$out")
kafka_succeeded=${kafka_succeeded:-0}
mismatch=$(sed -n 's/^== flyte.org\/info phase mismatch: //p' <<<"$out")
mismatch=${mismatch:-?}

echo "-- hop 3: Kafka (topic flyte-action-events)"
echo "     messages=$kafka_total succeeded=$kafka_succeeded info-mismatch=$mismatch"
[ "$kafka_succeeded" -eq "$expected_succeeded" ] && pass "Kafka has $expected_succeeded ActionSucceeded for this run" || flunk "Kafka has $kafka_succeeded ActionSucceeded, expected $expected_succeeded"
[ "$mismatch" = "0" ] && pass "flyte.org/info decodes to the same phase on every message" || flunk "flyte.org/info mismatch = $mismatch"
[ "$kafka_total" -ge "$total" ] && pass "every k8s Event reached Kafka ($kafka_total >= $total)" || flunk "Kafka has $kafka_total messages, k8s has $total events"

if [ "$fail" -eq 0 ]; then echo "== PASS"; else echo "== FAIL"; fi
exit "$fail"
