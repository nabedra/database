"""Process reservation and original-owner retention, with event-gated threads."""
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from meshdb import _callback_bridge as bridge_module
from meshdb import _callback_pool as pool_module
from test_callback_bridge import CALLBACK_TOKEN
from test_callback_pool import Authority, C, TOKEN, parsed

if (Path(__file__).parents[1] / 'src/meshdb/_serve_guard.py').exists():
    from meshdb import _serve_guard as guard
else:
    guard = None

CALLBACKS = (lambda p: 'resource', lambda r: None, lambda: [])
REJECTED = '^Serve runtime unavailable$'


class ServeGuardTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(guard, 'P03c2c serve guard implementation is missing')

    def test_concurrent_external_reservation_reentrant_rejection_and_stale_finish(self):
        start, accepted, release = (threading.Event() for _ in range(3))
        winners, rejected, constructed, errors = [], [], [], []
        losers_done, outcomes_lock = threading.Event(), threading.Lock()
        def contender():
            try:
                self.assertTrue(start.wait(2))
                try:
                    owner = guard._reserve()
                except RuntimeError as exc:
                    with outcomes_lock:
                        rejected.append(str(exc))
                        if len(rejected) == 7:
                            losers_done.set()
                    return
                winners.append(owner)
                constructed.append(owner._create_bridge(CALLBACKS, CALLBACK_TOKEN, TOKEN))
                accepted.set()
                try:
                    self.assertTrue(release.wait(5))
                finally:
                    owner._finish()
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=contender) for _ in range(8)]
        try:
            for thread in threads:
                thread.start()
            start.set()
            self.assertTrue(accepted.wait(2))
            # Reentrant acquisition fails while the winner holds its lifetime.
            with self.assertRaisesRegex(RuntimeError, REJECTED):
                guard._reserve()
        finally:
            try:
                self.assertTrue(losers_done.wait(2))
            finally:
                release.set()
                for thread in threads:
                    if thread.ident is not None:
                        thread.join(3)
        self.assertEqual(errors, [])
        self.assertTrue(all(not t.is_alive() for t in threads))
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(constructed), 1)
        self.assertEqual(rejected, ['Serve runtime unavailable'] * 7)
        newer = guard._reserve()
        try:
            # The new external runtime has no bridge, yet retains the slot.
            winners[0]._finish()
            with self.assertRaisesRegex(RuntimeError, REJECTED):
                guard._reserve()
        finally:
            newer._finish()

    def test_failure_before_resources_and_dead_bridge_still_require_finish(self):
        owner = guard._reserve()
        try:
            with patch.object(bridge_module.socket, 'socket', side_effect=OSError('secret')):
                with self.assertRaisesRegex(RuntimeError, '^Callback bridge startup failed$'):
                    owner._create_bridge(CALLBACKS, CALLBACK_TOKEN, TOKEN)
            with self.assertRaisesRegex(RuntimeError, REJECTED):
                guard._reserve()
        finally:
            owner._finish()
        owner = guard._reserve()
        try:
            bridge = owner._create_bridge(CALLBACKS, CALLBACK_TOKEN, TOKEN)
            bridge.close()
            self.assertFalse(bridge.alive)
            with self.assertRaisesRegex(RuntimeError, REJECTED):
                guard._reserve()
        finally:
            owner._finish()
        guard._reserve()._finish()

    def test_held_real_callback_survives_finish_until_original_threads_exit(self):
        entered, release = threading.Event(), threading.Event()
        stopped = []
        def callback(payload):
            entered.set()
            assert release.wait(15), 'callback watchdog'
            return 'resource'
        owner = guard._reserve()
        bridge = None
        with Authority() as authority:
            try:
                bridge = owner._create_bridge((callback, stopped.append, lambda: []), CALLBACK_TOKEN, TOKEN)
                originals = tuple(bridge._threads + bridge._pool._threads)
                bridge.activate(C, authority.url)
                bridge._pool.submit(parsed())
                self.assertTrue(entered.wait(2))
                begin = time.monotonic()
                owner._finish()
                self.assertLess(time.monotonic() - begin, 3.5)
                self.assertTrue(bridge.alive)
                with self.assertRaisesRegex(RuntimeError, REJECTED):
                    guard._reserve()
                self.assertEqual(tuple(bridge._threads + bridge._pool._threads), originals)
                self.assertEqual(stopped, [])
            finally:
                release.set()
                if bridge is not None:
                    bridge.close()
                    self.assertFalse(bridge.alive)
                owner._finish()
        guard._reserve()._finish()

    def test_partial_constructors_retain_real_original_threads(self):
        for pool_failure in (True, False):
            with self.subTest(pool_failure=pool_failure):
                entered, release = threading.Event(), threading.Event()
                owner = guard._reserve()
                originals = []
                real_start = threading.Thread.start
                original_run = pool_module._CallbackPool._run if pool_failure else bridge_module._CallbackBridge._run
                def run(instance, *args):
                    entered.set()
                    assert release.wait(15), 'startup watchdog'
                    original_run(instance, *args)
                def start(thread):
                    if len(originals) == (1 if pool_failure else 5):
                        raise OSError('secret startup detail')
                    real_start(thread)
                    originals.append(thread)
                target = pool_module._CallbackPool if pool_failure else bridge_module._CallbackBridge
                try:
                    with patch.object(target, '_run', run), patch.object(threading.Thread, 'start', start):
                        with self.assertRaisesRegex(RuntimeError, '^Callback bridge startup failed$'):
                            owner._create_bridge(CALLBACKS, CALLBACK_TOKEN, TOKEN)
                    self.assertTrue(entered.is_set())
                    bridge = owner._bridge
                    self.assertIsNotNone(bridge)
                    self.assertTrue(bridge.alive)
                    owner._finish()
                    self.assertTrue(any(t.is_alive() for t in originals))
                    with self.assertRaisesRegex(RuntimeError, REJECTED):
                        guard._reserve()
                finally:
                    release.set()
                    for thread in originals:
                        thread.join(3)
                    owner._finish()
                    self.assertTrue(all(not t.is_alive() for t in originals))
                guard._reserve()._finish()

    def test_unknown_owner_exit_fails_closed(self):
        owner = guard._reserve()
        bridge = owner._create_bridge(CALLBACKS, CALLBACK_TOKEN, TOKEN)
        try:
            with patch.object(bridge, 'close', side_effect=OSError('secret cleanup')):
                with self.assertRaisesRegex(RuntimeError, REJECTED) as caught:
                    owner._finish()
                self.assertIsNone(caught.exception.__context__)
                with self.assertRaisesRegex(RuntimeError, REJECTED):
                    guard._reserve()
            owner._finish()
            with patch.object(bridge_module._CallbackBridge, 'alive', property(
                    lambda self: (_ for _ in ()).throw(RuntimeError('secret')))):
                with self.assertRaisesRegex(RuntimeError, REJECTED) as caught:
                    guard._reserve()
                self.assertIsNone(caught.exception.__context__)
        finally:
            bridge.close()
            owner._finish()
        guard._reserve()._finish()


if __name__ == '__main__':
    unittest.main()
