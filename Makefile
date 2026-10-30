# Event export lab: Flyte v2 executor -> k8s Events -> kubernetes-event-exporter -> Kafka -> consumer
#
# Prerequisite: a running devbox whose flyte-binary was built from flyteorg/flyte/PR #8116 (see README).
# Every target is safe to run more than once.

KUBECTL ?= kubectl --context flyte-devbox
NS      := event-export
FLYTE   ?= $(HOME)/.venvs/flyte/bin/flyte
PY      ?= uv run --no-project --with confluent-kafka python -I
KAFKA   := $(KUBECTL) -n $(NS) exec deploy/kafka -- /opt/kafka/bin
BOOT    := --bootstrap-server localhost:9092

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show the targets, in the order you normally run them
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

.PHONY: check-env
check-env: ## Confirm kubectl reaches the devbox and the CLI tools exist
	@$(KUBECTL) get nodes >/dev/null && echo "kubectl: ok ($(KUBECTL))"
	@$(FLYTE) --version | head -1
	@uv --version && jq --version

.PHONY: up
up: ## Deploy Kafka and the exporter into namespace event-export, then create the topics
	$(KUBECTL) apply -f k8s/01-kafka.yaml
	$(KUBECTL) -n $(NS) rollout status deploy/kafka --timeout=300s
	$(MAKE) topics
	$(KUBECTL) apply -f k8s/02-exporter.yaml
	$(KUBECTL) -n $(NS) rollout status deploy/event-exporter --timeout=300s
	@echo; $(KUBECTL) -n $(NS) get pods

.PHONY: topics
topics: ## Create the two Kafka topics (no-op when they exist)
	$(KAFKA)/kafka-topics.sh $(BOOT) --create --if-not-exists --topic flyte-action-events --partitions 3 --replication-factor 1
	$(KAFKA)/kafka-topics.sh $(BOOT) --create --if-not-exists --topic flyte-action-events-layout --partitions 1 --replication-factor 1

.PHONY: run
run: ## Submit the two workloads (11 successes, 1 failure) and remember their run names
	@mkdir -p .runs
	cd workloads && $(FLYTE) run hello.py main | tee /dev/stderr | grep -o 'Created Run: [a-z0-9]*' | awk '{print $$3}' > ../.runs/hello
	cd workloads && $(FLYTE) run events_probe.py boom | tee /dev/stderr | grep -o 'Created Run: [a-z0-9]*' | awk '{print $$3}' > ../.runs/boom
	@echo; echo "hello run: $$(cat .runs/hello)"; echo "boom  run: $$(cat .runs/boom)"

.PHONY: verify
verify: ## Check all three hops for the last `make run` (k8s Events, exporter, Kafka)
	scripts/verify.sh "$$(cat .runs/hello)" 11
	scripts/verify.sh "$$(cat .runs/boom)" 0

.PHONY: consume
consume: ## Read the Kafka topic from the beginning with a fresh consumer group
	scripts/with-kafka-port.sh $(PY) scripts/consumer.py --seconds 8 $(ARGS)

.PHONY: events
events: ## Count the k8s Events on TaskActions, per reason
	$(KUBECTL) get events -n flyte --field-selector involvedObject.kind=TaskAction --no-headers | awk '{print $$3}' | sort | uniq -c

.PHONY: logs
logs: ## Show what the exporter did with Action events (received, sent, discarded)
	$(KUBECTL) -n $(NS) logs deploy/event-exporter | grep -E '"reason":"Action|sending event to sink|discarded|throttling' | tail -40

.PHONY: kafka-groups
kafka-groups: ## Show every consumer group's bookmark (offset) and lag per partition
	$(KAFKA)/kafka-consumer-groups.sh $(BOOT) --describe --all-groups 2>/dev/null | grep -E 'GROUP|flyte-action-events' || echo "(no consumer groups yet)"

.PHONY: kafka-offsets
kafka-offsets: ## Show how many messages each partition holds
	$(KAFKA)/kafka-get-offsets.sh $(BOOT) --topic flyte-action-events

.PHONY: reset-topics
reset-topics: ## Delete and recreate the topics for a clean demo (keeps Kafka and the exporter)
	-$(KAFKA)/kafka-topics.sh $(BOOT) --delete --topic flyte-action-events
	-$(KAFKA)/kafka-topics.sh $(BOOT) --delete --topic flyte-action-events-layout
	sleep 3
	$(MAKE) topics

.PHONY: down
down: ## Remove the exporter, Kafka and the namespace
	-$(KUBECTL) delete -f k8s/02-exporter.yaml
	-$(KUBECTL) delete -f k8s/01-kafka.yaml
	rm -rf .runs
