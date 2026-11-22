"""Regression checks for the Linux RF3 packet-drop fixture."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

import oltp_blackhole


class BlackholeTrafficTests(unittest.TestCase):
    def test_o06_o18_client_can_observe_recovery_within_the_failover_bound(self):
        clock = 0
        failover = 7.85

        def sleep(seconds):
            nonlocal clock
            clock += seconds

        class TimedClient:
            def __init__(self, url, token, timeout, ca):
                self.timeout = timeout

            def exchange(self, path, request):
                remaining = max(0, failover - clock)
                if 0 < clock < failover:
                    # The reconnect can probe the silent voter again.
                    remaining = max(1.5, remaining)
                sleep(min(remaining, self.timeout))
                if remaining > self.timeout:
                    raise TimeoutError('the client stopped waiting before failover')
                return 200, {'data': {'id': 0, 'value': 100}}

            def close(self):
                pass

        cluster = SimpleNamespace(api_urls={'api1': ''}, auth={'data_token': ''}, ca=None)
        with patch.object(oltp_blackhole, 'Http', TimedClient), \
                patch.object(oltp_blackhole, 'time', SimpleNamespace(monotonic=lambda: clock, sleep=sleep)), \
                patch.object(oltp_blackhole, 'record'):
            successes, trace = oltp_blackhole.traffic(cluster, ('az1_0', 'table'), False, 0, 8, 0)

        self.assertEqual(successes[0][0], failover)
        self.assertEqual(trace[0], dict(start=0, end=failover, status=200, error=None))


if __name__ == '__main__':
    unittest.main()
