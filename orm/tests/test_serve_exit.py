"""Explicit process receipts, using the real independently framed child."""
import json
from pathlib import Path
import signal
import tempfile
import threading
import unittest
from unittest.mock import patch

from nabedra import _serve_guard as g, _serve_process as p
import test_serve_process
from test_serve_process import CHILD, HOST


class ServeExitTests(unittest.TestCase):
    def startup(self):
        return dict(version=1, type='init', name='test',
                    initial_policy=dict(mode='fixed', desired=0, min_nodes=0, max_nodes=0),
                    provisioning='external', bridge=None)

    def run_child(self, mode, receipt, prepared=lambda _: None):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / 'config'
            config.write_text(json.dumps(dict(mode=mode, record=str(Path(tmp) / 'events'))))
            with patch.object(p, 'resolve_server_binary', return_value=str(CHILD)):
                p.run_process(config, self.startup(), prepared=prepared, stop=lambda: None,
                              _receipt=receipt)

    def test_unknown_exit_retains_exact_live_owner_and_recovers(self):
        reservation, receipt = g._reserve(), p._ExitReceipt()
        reservation._attach_process(receipt)
        real_poll = p.subprocess.Popen.poll
        failing = threading.Event()
        def poll(child):
            if failing.is_set():
                raise OSError('SECRET wait')
            return real_poll(child)
        def announce(owner, url, deadline):
            # Put the existing held fixture through its shutdown gate before
            # breaking only the owned OS-exit observation operation.
            owner.send({'version': 1, 'type': 'shutdown'}, deadline)
            owner.signals.append(signal.SIGTERM)
            failing.set()
        try:
            with patch.object(p._Owner, 'announce', announce), patch.object(p.subprocess.Popen, 'poll', poll):
                with self.assertRaises(p.NabedraError) as caught:
                    self.run_child('held', receipt)
                with self.assertRaises(p.NabedraError):
                    receipt._retry()
                self.assertFalse(receipt._ready())
            self.assertEqual(caught.exception.code, 'shutdown_failed')
            self.assertNotIn('SECRET', str(caught.exception))
            owner = receipt._owner
            self.assertIs(reservation._process, receipt)
            self.assertIsNone(real_poll(owner.child))
            self.assertFalse(receipt._exited)
            self.assertFalse(receipt._active)
            with self.assertRaisesRegex(RuntimeError, '^Serve runtime unavailable$'):
                reservation._finish()
            with self.assertRaises(RuntimeError):
                g._reserve()
            owner.child.kill()
            owner.child.wait(timeout=3)
            self.assertIsNotNone(owner.child.returncode)
            self.assertFalse(receipt._exited)
            # Recovery must honor the actual reap even if a later observation
            # still fails. It must not require a second successful OS poll.
            with patch.object(p.subprocess.Popen, 'poll', poll):
                receipt._retry()
            self.assertIs(receipt._owner, owner)
            self.assertTrue(receipt._ready())
            reservation._finish()
            g._reserve()._finish()
            with self.assertRaises(p.NabedraError):
                self.run_child('normal', receipt)
            self.assertIs(receipt._owner, owner)
        finally:
            failing.clear()
            if receipt._owner and receipt._owner.child:
                child = receipt._owner.child
                if real_poll(child) is None:
                    child.kill()
                child.wait(timeout=3)
            receipt._retry()
            reservation._finish()

    def test_dead_child_cannot_finish_active_run(self):
        receipt, reservation = p._ExitReceipt(), g._reserve()
        reservation._attach_process(receipt)
        entered, release = threading.Event(), threading.Event()
        errors = []
        def prepared(record):
            child = receipt._owner.child
            child.kill()
            child.wait(timeout=3)
            entered.set()
            assert release.wait(5), 'active run gate watchdog'
        def run():
            try:
                self.run_child('normal', receipt, prepared)
            except p.NabedraError:
                pass
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=run)
        try:
            thread.start()
            self.assertTrue(entered.wait(3))
            with self.assertRaises(RuntimeError):
                reservation._finish()
            with self.assertRaises(RuntimeError):
                receipt._retry()
            with self.assertRaises(RuntimeError):
                g._reserve()
        finally:
            release.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            reservation._finish()
        self.assertEqual(errors, [])
        self.assertTrue(receipt._ready())

    def test_no_spawn_cleanup_failure_and_exit_are_distinct(self):
        receipt, reservation = p._ExitReceipt(), g._reserve()
        reservation._attach_process(receipt)
        saved = signal.getsignal(signal.SIGTERM)
        real_close = p._Owner.close
        def close(owner):
            real_close(owner)
            raise OSError('SECRET close')
        try:
            with patch.object(p.selectors, 'DefaultSelector', side_effect=OSError('SECRET selector')), \
                    patch.object(p._Owner, 'close', close):
                with self.assertRaises(p.NabedraError) as caught:
                    self.run_child('normal', receipt)
            self.assertNotIn('SECRET', str(caught.exception))
            self.assertTrue(receipt._exited)
            self.assertFalse(receipt._cleaned)
            with self.assertRaises(RuntimeError):
                reservation._finish()
            receipt._retry()
            self.assertTrue(receipt._ready())
            self.assertEqual(signal.getsignal(signal.SIGTERM), saved)
        finally:
            receipt._retry()
            reservation._finish()

    def test_real_startup_failure_exit_with_close_failure(self):
        receipt = p._ExitReceipt()
        saved = signal.getsignal(signal.SIGTERM)
        real_close = p._Owner.close
        def close(owner):
            real_close(owner)
            raise OSError('SECRET')
        try:
            with patch.object(p._Owner, 'close', close):
                with self.assertRaises(p.NabedraError):
                    self.run_child('fatal', receipt)
            self.assertIsNotNone(receipt._owner.child.returncode)
            self.assertTrue(receipt._exited)
            self.assertFalse(receipt._cleaned)
            self.assertEqual(signal.getsignal(signal.SIGTERM), saved)
        finally:
            receipt._retry()
        self.assertTrue(receipt._ready())

    def test_reaped_returncode_is_exit_proof_even_if_later_poll_fails(self):
        receipt = p._ExitReceipt()
        reaped = threading.Event()
        shutdown, poll = p._Owner.shutdown, p.subprocess.Popen.poll
        def finish(owner):
            result = shutdown(owner)
            self.assertIsNotNone(owner.child.returncode)
            reaped.set()
            return result
        def observe(child):
            if reaped.is_set():
                raise OSError('SECRET late poll')
            return poll(child)
        with patch.object(p._Owner, 'shutdown', finish), patch.object(p.subprocess.Popen, 'poll', observe):
            with self.assertRaises(p.NabedraError) as caught:
                self.run_child('fatal', receipt)
        self.assertEqual(caught.exception.code, 'bind_failed')
        self.assertTrue(receipt._ready())

    def test_resource_close_failure_still_closes_other_resources_and_restores(self):
        receipt = p._ExitReceipt()
        saved = signal.getsignal(signal.SIGTERM)
        real_install = p._Owner.install
        real_selector_close = []
        def install(owner):
            real_install(owner)
            real_selector_close.append(owner.selector.close)
            owner.selector.close = lambda: (_ for _ in ()).throw(OSError('SECRET close'))
        try:
            with patch.object(p._Owner, 'install', install):
                with self.assertRaises(p.NabedraError) as caught:
                    self.run_child('fatal', receipt)
            self.assertNotIn('SECRET', str(caught.exception))
            owner = receipt._owner
            self.assertTrue(receipt._exited)
            self.assertFalse(receipt._cleaned)
            self.assertEqual(signal.getsignal(signal.SIGTERM), saved)
            for resource in (owner.channel, owner.wake, owner.writer,
                             owner.child.stdout, owner.child.stderr):
                if hasattr(resource, 'closed'):
                    self.assertTrue(resource.closed)
                else:
                    self.assertEqual(resource.fileno(), -1)
        finally:
            if real_selector_close:
                receipt._owner.selector.close = real_selector_close[0]
            receipt._retry()
        self.assertTrue(receipt._ready())

    def test_attached_unused_receipt_requires_explicit_no_spawn_completion(self):
        receipt, reservation = p._ExitReceipt(), g._reserve()
        reservation._attach_process(receipt)
        try:
            with self.assertRaises(RuntimeError):
                reservation._finish()
            self.assertFalse(receipt._ready())
            receipt._retry()
            self.assertTrue(receipt._ready())
            self.assertIsNone(receipt._owner)
        finally:
            reservation._finish()

    def test_saved_handler_still_owns_active_run(self):
        receipt, reservation = p._ExitReceipt(), g._reserve()
        reservation._attach_process(receipt)
        saved = signal.getsignal(signal.SIGTERM)
        observed = []
        def handler(signum, frame):
            observed.append(receipt._active)
            with self.assertRaises(RuntimeError):
                reservation._finish()
        def announce(owner, url, deadline):
            owner.signals.append(signal.SIGTERM)
        try:
            signal.signal(signal.SIGTERM, handler)
            with patch.object(p._Owner, 'announce', announce):
                self.run_child('normal', receipt)
            self.assertEqual(observed, [True])
        finally:
            signal.signal(signal.SIGTERM, saved)
            receipt._retry()
            reservation._finish()

    def test_signal_receipt_confirms_exit_and_cleanup(self):
        # Reuse the actual signal host and independent readiness watchdog.
        helper = test_serve_process.ServeProcessTests()
        source = HOST.replace("saved = signal.getsignal(signal.SIGTERM)",
            "receipt = p._ExitReceipt()\nsaved = signal.getsignal(signal.SIGTERM)")
        source = source.replace('prepared=prepared, stop=stop)',
                                'prepared=prepared, stop=stop, _receipt=receipt)')
        source += "\nassert receipt._ready()\nassert receipt._owner.child.returncode is not None\n"
        try:
            with patch('test_serve_process.HOST', source):
                host, config, record = helper.launch('external')
            helper.ready(host)
            host.send_signal(signal.SIGTERM)
            helper.finish(host, config, record)
        finally:
            helper.doCleanups()
