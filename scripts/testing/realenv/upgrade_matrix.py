#!/usr/bin/env python3
"""Weekly Events compatibility check for two previous releases and head."""

import argparse
import json
from pathlib import Path

from server.events.tests.process_fixture import eventually

from ..kind.cluster import temporary_cluster
from ..kind.manifests import NAME
from ..kind.suite_events import EventsRun, check_queue
from .history import HistoryWriter, read_jsonl


# CI can tag locally imported Events release images this way. The repository
# currently has no independently published historical Events image series.
IMAGE_SLOTS = ("nabedra-events:previous-2", "nabedra-events:previous-1", "nabedra-events:ci")


def parse_images(value):
    images = tuple(value.split(","))
    if len(images) != 3 or any(not image or any(char.isspace() for char in image) for image in images):
        raise argparse.ArgumentTypeError("--images requires three image references in oldest, previous, head order")
    return images


def broker_status(fixture, ordinal):
    return next((state for state in fixture.pod(ordinal)["status"].get("containerStatuses", [])
                 if state["name"] == "broker"), {})


def set_image(fixture, ordinal, image):
    pod = fixture.pod(ordinal)
    if next(container["image"] for container in pod["spec"]["containers"] if container["name"] == "broker") == image:
        return
    previous = broker_status(fixture, ordinal).get("containerID")
    fixture.cluster.kube("set", "image", f"pod/{NAME}-{ordinal}", f"broker={image}")

    def restarted():
        state = broker_status(fixture, ordinal)
        return state.get("ready") and state["containerID"] != previous

    eventually(f"{NAME}-{ordinal} restarted with {image}", restarted, timeout=120)


def run_mixed(images, history_path):
    with HistoryWriter(history_path) as history, temporary_cluster(3) as cluster:
        fixture = EventsRun(cluster, history)
        fixture.deploy(images[0])
        cluster.load_image(*dict.fromkeys(images[1:]))
        with fixture.endpoints(0, 1, 2):
            fixture.setup_workload()
            fixture.publish(0, 0)
        for ordinal, image in enumerate(images[1:], 1):
            set_image(fixture, ordinal, image)
            with fixture.endpoints(0, 1, 2):
                _, leader = fixture.leader(fixture.group)
                fixture.wait_for_replica(ordinal, fixture.group, leader["membership"]["applied_index"], {1, 2, 3})
                fixture.publish(ordinal, ordinal)
        with fixture.endpoints(0, 1, 2):
            fixture.poll(2)

    with history_path.open(encoding="utf-8") as source:
        return check_queue(read_jsonl(source))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--images", type=parse_images, help="local image references in oldest, previous, head order")
    parser.add_argument("--history", type=Path, default=Path("kind-events-mixed.jsonl"))
    args = parser.parse_args()
    print(json.dumps({"tier": "weekly", "images": args.images or IMAGE_SLOTS}, indent=2), flush=True)
    if args.images:
        args.history.parent.mkdir(parents=True, exist_ok=True)
        count = run_mixed(args.images, args.history)
        print(f"Mixed Events cluster replayed {count} acknowledged events. History: {args.history}")


if __name__ == "__main__":
    main()
