# Event export lab

This lab shows how action events leave a Flyte v2 cluster. Flyte never talks to Kafka or
any other message broker. Kubernetes and two off-the-shelf tools do the work.

An **action** is one task run inside a Flyte run. Each time an action changes phase
(running, succeeded, failed, ...), the Flyte executor emits an **action event**.

```text
+----------+     +-----------+     +----------------+     +-------+     +----------+
| Flyte v2 | --> | K8s Event | --> | event-exporter | --> | Kafka | --> | consumer |
+----------+     +-----------+     +----------------+     +-------+     +----------+
```

The path, step by step:

1. The Flyte executor writes each action event as a Kubernetes Event (`events.k8s.io/v1`).
   The Event is attached to the action's `TaskAction` object. The full payload sits in
   annotations named `flyte.org/*`.
2. kubernetes-event-exporter (resmoio, v1.7) watches those Events. It is a normal
   open-source tool, not part of Flyte.
3. The exporter sends each Event to a **sink**. This lab uses Kafka. The exporter also
   supports Loki, Elasticsearch, webhooks and other sinks.
4. Your consumer reads the events from Kafka.

Everything after the Kubernetes API server is off-the-shelf. An operator picks the
exporter and the sink.

The whole lab runs inside the devbox's k3s cluster, in namespace `event-export`. Nothing
here changes Flyte.

## Prerequisites

The **devbox** is the local Flyte cluster built from `docker/devbox-bundled` in the flyte
repo.

| What | Why | Check |
|---|---|---|
| A running devbox built from the PR branch (`feat/cloud_events`) | only this executor creates the Events | `make verify` checks this by behaviour, see below |
| `kubectl` with context `flyte-devbox` | every deploy and inspect step | `make check-env` |
| `flyte` CLI in `~/.venvs/flyte` (or set `FLYTE=...`) | submits the workloads | `make check-env` |
| `uv`, `jq`, `nc` | runs the consumer, counts JSON, probes the port | `make check-env` |

To build and start the devbox from the branch, run this from the flyte repo root:

```bash
make -C docker/devbox-bundled flyte FLYTE_ARCHES=arm64      # flyte-binary from this checkout
docker buildx build --builder flyte-devbox --load --platform linux/arm64 \
  --tag flyte-devbox:latest docker/devbox-bundled
make devbox-run
```

## Quick start

```bash
make up        # Kafka + exporter + topics          (~1 min)
make run       # 11 successful actions + 1 failing  (~1 min)
make verify    # the three-hop check, PASS/FAIL
make down
```

## What `make verify` proves

A **hop** is one arrow in the diagram. `make verify` checks three hops in one run. When a
hop fails, you know which component to look at.

| Hop | Where | Command behind it | Passes when |
|---|---|---|---|
| 1 | Kubernetes API | `kubectl get events --field-selector involvedObject.kind=TaskAction` | the hello run has 11 `ActionSucceeded` events, and every `ActionRunning` event has a `flyte.org/info` annotation |
| 2 | exporter | `kubectl logs deploy/event-exporter` | the exporter received every event and discarded none |
| 3 | Kafka | `scripts/consumer.py --run <run>` | the consumer reads back 11 `ActionSucceeded`, and `flyte.org/info` decodes to the same phase on every message |

Hop 1 also checks the **Flyte image**. Before [PR #8116](https://github.com/flyteorg/flyte/pull/8116), the executor sent non-terminal
events (such as `ActionRunning`) through the Kubernetes event recorder. That path cannot
carry annotations. So if `ActionRunning` events have no `flyte.org/info`, the devbox runs
an older executor.

## Explore

```bash
make events          # events per reason, straight from the API server
make logs            # exporter decisions: received / sent to sink / discarded
make consume         # read the topic from the beginning with a fresh consumer group
make kafka-offsets   # messages per partition
make kafka-groups    # every consumer group's bookmark (offset) and lag
make reset-topics    # clean slate for a demo
```

Three things to try, and what each one shows:

1. Run `make consume` twice. Both runs print every message. Kafka keeps the messages. A
   consumer group only moves its own bookmark (the offset).
2. Then run `make kafka-groups`. Each group shows `LAG 0`. Run `make run` again and look
   once more. The lag now equals the number of new messages.
3. Compare `make events` with the consumer summary. About one in four non-terminal events
   appears twice, with the same `run, action, attempt, phase, version`. Terminal events
   (the last event of an attempt, such as `ActionSucceeded`) do not repeat. Your consumer
   must deduplicate on that key.

## Pitfalls we hit

We reproduced every one of these on the devbox.

| Pitfall | Symptom | Fix in this lab |
|---|---|---|
| `maxEventAgeSeconds` defaults to 5 s | events are dropped without an error: `Event discarded as being older then maxEventAgeSeconds` | set it to 60 in `k8s/02-exporter.yaml`; start the exporter before the workload |
| exporter client rate limit (`kubeQPS` default 5) | `Waited for 5s due to client-side throttling`; 60 of 65 events dropped as too old | `kubeQPS: 100`, `kubeBurst: 200` |
| the exporter's `annotations:` rule matches the TaskAction, not the Event | a route on `flyte.org/*` annotations never matches | route on `apiVersion`, `kind`, `reason`; a `labels:` rule does match the TaskAction's `flyte.org/*` labels |
| the Kafka key is the Event UID | one action's events land in different partitions, so Kafka gives no order across them | order by `eventTime`, not by partition and not by `flyte.org/version` (it restarts at each phase) |
| `GenerateName` events + at-least-once reconcile | duplicate non-terminal events | deduplicate on `(run, action, attempt, phase, version)` |
| GC deletes finished TaskActions after `MaxTTL` (1 h) | a late `ActionAborted` arrives after `ActionSucceeded` for the same attempt | treat the first terminal event as final, or drop an `ActionAborted` whose TaskAction was already terminal |
| a bad message + auto-commit | the consumer crashes, and on restart the committed offset skips the message | decide how to handle poison pills before production (`scripts/consumer.py` does not) |

## Files

```text
Makefile                     the targets above
k8s/01-kafka.yaml            single-node Kafka (KRaft), two listeners: 9092 in-cluster, 19092 via port-forward
k8s/02-exporter.yaml         exporter RBAC, config (routes + sinks), Deployment
scripts/verify.sh            the three-hop check
scripts/with-kafka-port.sh   runs a command while a port-forward to Kafka stays open
scripts/consumer.py          reads the topic, prints one line per event and a summary
workloads/hello.py           11 actions that succeed (flyte.map)
workloads/events_probe.py    `boom` raises, giving one ActionFailed
workloads/.flyte/config.yaml points the flyte CLI at the devbox
```

Kafka stores its data in the pod's filesystem. The data is gone when the pod restarts.
That is fine for a lab, not for production.

## References

- Flyte PR #8116, "feat: Emit action events as Kubernetes Events", branch `feat/cloud_events`:
  <https://github.com/flyteorg/flyte/pull/8116>
- kubernetes-event-exporter (image `ghcr.io/resmoio/kubernetes-event-exporter:v1.7`):
  <https://github.com/resmoio/kubernetes-event-exporter>
- Kubernetes Event API (`events.k8s.io/v1`):
  <https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/>
- Apache Kafka image `apache/kafka:3.9.0`: <https://hub.docker.com/r/apache/kafka>
- confluent-kafka Python client, used by `scripts/consumer.py`:
  <https://github.com/confluentinc/confluent-kafka-python>
