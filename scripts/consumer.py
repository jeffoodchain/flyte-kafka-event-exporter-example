"""Read Flyte action events from Kafka and check them.

Prerequisite: kubectl -n event-export port-forward svc/kafka 19092:19092

Run:
    uv pip install confluent-kafka
    python consumer.py              # read from the start, stop with Ctrl-C
    python consumer.py --seconds 30 # stop after 30s with no new message
    python consumer.py --run <run>  # only count one run (repeatable)

For each message it prints one line and, at the end, a summary:
    - messages per reason
    - duplicates on (run-name, action-name, attempt, phase, version)
    - messages where flyte.org/info does not decode back to the same phase
"""

import argparse
import collections
import json
import time

from confluent_kafka import Consumer

TOPIC = "flyte-action-events"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--seconds", type=float, default=0, help="stop after this many idle seconds; 0 = never")
    parser.add_argument("--run", action="append", default=[], help="only count these run names; repeatable")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"flyte-event-check-{int(time.time())}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    reasons = collections.Counter()
    seen = collections.Counter()
    bad_info = []
    last_message = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if args.seconds and time.monotonic() - last_message > args.seconds:
                    break
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            last_message = time.monotonic()

            event = json.loads(msg.value())
            ann = event.get("metadata", {}).get("annotations", {})
            if args.run and ann.get("flyte.org/run-name") not in args.run:
                continue
            reason = event.get("reason", "")
            reasons[reason] += 1

            key = tuple(ann.get(f"flyte.org/{k}", "") for k in ("run-name", "action-name", "attempt", "phase", "version"))
            seen[key] += 1

            info_phase = ""
            if "flyte.org/info" in ann:
                info_phase = json.loads(ann["flyte.org/info"]).get("phase", "")
                if info_phase != ann.get("flyte.org/phase"):
                    bad_info.append(key)

            print(
                f"p{msg.partition()} o{msg.offset()} {reason:28} "
                f"run={key[0]} action={key[1]} attempt={key[2]} phase={key[3]} version={key[4]} "
                f"info={'ok' if info_phase else 'missing'}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("\n== messages per reason")
    for reason, n in sorted(reasons.items()):
        print(f"{n:5} {reason}")
    dups = {k: n for k, n in seen.items() if n > 1}
    print(f"\n== duplicates on (run, action, attempt, phase, version): {len(dups)}")
    for k, n in dups.items():
        print(f"{n}x {k}")
    print(f"\n== flyte.org/info phase mismatch: {len(bad_info)}")


if __name__ == "__main__":
    main()
