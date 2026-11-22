#!/usr/bin/env python3

from pathlib import Path
import signal
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from .history import HistoryWriter, pair_operations, read_jsonl
from .nemesis import Container, Nemesis, NETEM_IMAGE, Process


class NemesisTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.path = Path(directory) / "history.jsonl"
        self.writer = self.enterContext(HistoryWriter(self.path))
        self.run = Mock()
        self.nemesis = Nemesis(self.writer, run=self.run)
        self.container = Container("broker")

    def commands(self):
        return [call.args[0] for call in self.run.call_args_list]

    def test_process_and_container_signals(self):
        child = Mock(spec=subprocess.Popen)
        child.poll.return_value = None
        target = Process("worker-one", child)
        self.nemesis.stop(target)
        self.nemesis.cont(target)
        self.nemesis.kill(target)
        self.assertEqual([call.args[0] for call in child.send_signal.call_args_list],
                         [signal.SIGSTOP, signal.SIGCONT, signal.SIGKILL])
        self.nemesis.stop(self.container)
        self.nemesis.cont(self.container)
        self.nemesis.kill(self.container)
        self.assertEqual(self.commands(), [
            ["docker", "kill", "--signal", "SIGSTOP", "broker"],
            ["docker", "kill", "--signal", "SIGCONT", "broker"],
            ["docker", "kill", "--signal", "SIGKILL", "broker"],
        ])
        with self.path.open() as source:
            ops = pair_operations(read_jsonl(source))
        self.assertEqual([op.completion.value["action"] for op in ops], ["stop", "cont", "kill"] * 2)
        self.assertTrue(all(op.invoke.f == "nemesis" and op.completion.type == "ok" for op in ops))

    def test_network_impairments_and_heal_reverse_order(self):
        self.nemesis.disconnect(self.container, "peer-net", connect_options=("--alias", "node-1"))
        self.nemesis.netem(self.container, delay_ms=25, loss=1.5, duplicate=2)
        self.nemesis.heal_all()
        self.nemesis.heal_all()
        prefix = ["docker", "run", "--rm", "--network", "container:broker", "--cap-add", "NET_ADMIN",
                  "--entrypoint", "tc", NETEM_IMAGE]
        self.assertRegex(NETEM_IMAGE, r"@sha256:[0-9a-f]{64}$")
        self.assertEqual(self.commands(), [
            ["docker", "network", "disconnect", "peer-net", "broker"],
            prefix + ["qdisc", "add", "dev", "eth0", "root", "handle", "90:", "netem",
                      "delay", "25ms", "loss", "1.5%", "duplicate", "2%"],
            prefix + ["qdisc", "del", "dev", "eth0", "root", "handle", "90:"],
            ["docker", "network", "connect", "--alias", "node-1", "peer-net", "broker"],
        ])
        with self.path.open() as source:
            ops = pair_operations(read_jsonl(source))
        self.assertEqual([op.invoke.value["action"] for op in ops],
                         ["disconnect", "netem", "clear_netem", "connect", "heal_all", "heal_all"])

    def test_timed_out_disconnect_is_healed(self):
        self.run.side_effect = [subprocess.TimeoutExpired("disconnect", 60), None]
        with self.assertRaises(subprocess.TimeoutExpired):
            self.nemesis.disconnect(self.container, "peer-net")
        self.nemesis.heal_all()
        self.assertEqual(self.commands()[-1], ["docker", "network", "connect", "peer-net", "broker"])
        with self.path.open() as source:
            ops = pair_operations(read_jsonl(source))
        self.assertEqual([op.completion.type for op in ops], ["info", "ok", "ok"])

    def test_heal_all_attempts_other_faults_after_a_failed_cleanup(self):
        self.nemesis.stop(self.container)
        self.nemesis.disconnect(self.container, "peer-net")
        self.run.side_effect = [subprocess.CalledProcessError(1, "connect"), None, None]
        with self.assertRaises(ExceptionGroup):
            self.nemesis.heal_all()
        self.assertEqual(self.commands()[-1], ["docker", "kill", "--signal", "SIGCONT", "broker"])
        self.nemesis.heal_all()
        self.assertEqual(len(self.commands()), 5)

    def test_invalid_netem_values_do_not_run_commands(self):
        for options in ({}, {"loss": -1}, {"duplicate": 101}, {"delay_ms": float("inf")}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.nemesis.netem(self.container, **options)
        self.run.assert_not_called()
