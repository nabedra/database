"""PR #121 review: recovery suites, with the RV-REP-3 check made an observation.

usage: review121_wipe.py <wipe|voter-refresh|restart> <binary> <test executable>

`wipe` runs oltp_recovery_operations.py's replacement-wipe up to the relaunch
of the wiped candidate. Its 30 s exit wait is replaced: for 60 s the script
watches whether the process reopens a pool and whether it reports again that
it applied stable RF3 in every group (that is, votes and applies under the
identity whose storage was wiped). The other suites run unchanged.
"""
import pathlib
import sys

suite, binary, executable = sys.argv[1:4]
path = pathlib.Path(__file__).with_name('oltp_recovery_operations.py')
source = path.read_text()
if suite == 'wipe':
    relaunch = ("            cluster.launch_worker('replacement')\n"
                "            attempt = cluster.workers['replacement']\n"
                "            assert attempt.popen.wait(timeout=30) != 0\n")
    assert source.count(relaunch) == 1, 'the wipe suite changed'
    source = source.replace(relaunch, (
        "            stable_checkpoint.unlink()\n"
        "            cluster.launch_worker('replacement')\n"
        "            attempt = cluster.workers['replacement']\n"
        "            watch = time.monotonic() + 60\n"
        "            while attempt.alive() and not stable_checkpoint.exists() and time.monotonic() < watch:\n"
        "                time.sleep(0.2)\n"
        "            pool, rejoined, alive = (mount_root / 'pool').exists(), stable_checkpoint.exists(), attempt.alive()\n"
        "            print(f'r121 wipe: alive={alive} pool_reopened={pool} stable_rf3_again={rejoined}', flush=True)\n"
        "            print('\\n'.join(cluster.tail('replacement').splitlines()[-25:]), flush=True)\n"
        "            assert not pool and not rejoined, (\n"
        "                f'a wiped replacement came back under its committed voter identity: '\n"
        "                f'pool_reopened={pool} stable_rf3_again={rejoined}')\n"
        "            assert not alive, 'the wiped replacement neither exited nor rejoined'\n"))
name = {'wipe': 'replacement-wipe', 'voter-refresh': 'replacement-voter-refresh', 'restart': 'restart'}[suite]
sys.argv = [str(path), '--binary', binary, '--test-executable', executable, '--suite', name]
exec(compile(source, str(path), 'exec'), {'__name__': '__main__', '__file__': str(path)})
