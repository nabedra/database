import subprocess, os, re, sys, time  # rc 124/137: the suite hung, spinning in virtual time
W = sys.argv[1]
R = W + "/server/rt/src/"
M = [
 ("M01 storage points never stop", "sim/suspend.rs", "    if job.kind == Kind::Job {\n        let stop", "    if false && job.kind == Kind::Job {\n        let stop"),
 ("M02 crash lets suspended jobs run to the end", "sim/suspend.rs", "job.advance(world, Resume::Cancel);\n        }\n    }", "job.advance(world, Resume::RunToEnd);\n        }\n    }"),
 ("M03 crash does not touch suspended jobs", "sim.rs", "        self.faults.cancel(self, context);\n", ""),
 ("M04 the clock freeze lands before the read", "time.rs", "after_read(crate::sim::mono_now())", "{ crate::sim::clock_read(); crate::sim::mono_now() }"),
 ("M05 wall reads do not freeze", "time.rs", "after_read(crate::sim::wall_now())", "crate::sim::wall_now()"),
 ("M06 intervals ignore the floor", "time.rs", "tokio::time::interval(crate::sim::to_runtime_timer(period))", "tokio::time::interval(crate::sim::to_runtime_duration(period))"),
 ("M07 timeouts ignore the floor", "time.rs", "tokio::time::timeout(crate::sim::to_runtime_timer(duration), future)", "tokio::time::timeout(crate::sim::to_runtime_duration(duration), future)"),
 ("M08 deadlines ignore the floor", "sim.rs", "if self.timer_floor_ppm.is_none() || ahead <= 0", "if true || ahead <= 0"),
 ("M09 no timer slack", "sim.rs", "const TIMER_SLACK_NANOS: i128 = 1_000;", "const TIMER_SLACK_NANOS: i128 = 0;"),
 ("M10 drift may pass the floor", "sim.rs", "            assert_floor(drift_ppm, entry.clock.timer_floor_ppm);\n", ""),
 ("M11 wipe accepts a live node", "sim/volumes.rs", "            !self.is_up(node),\n", "            true || !self.is_up(node),\n"),
 ("M12 a wipe still changes the base under mkfs inodes", "sim/volumes.rs", "if base == old_fs.ino_base() && !lock(&self.volumes.state).mkfs_inodes", "if base == old_fs.ino_base()"),
 ("M13 mkfs inodes ignored", "sim/volumes.rs", "        if table.mkfs_inodes {", "        if false && table.mkfs_inodes {"),
 ("M14 mkfs inodes on by default", "sim/volumes.rs", "        if table.mkfs_inodes {", "        if !table.mkfs_inodes {"),
 ("M15 reads inside an extension may freeze", "sim/suspend.rs", "job.to_end.get() || super::in_ext()", "job.to_end.get()"),
 ("M16 helper polls use one seed", "sim/suspend.rs", "runtime_seeded(seed)", "runtime_seeded({ let _ = seed; 7 })"),
 ("M17 deferred wakers dropped", "sim/suspend.rs", "        tokio::task::yield_now().await;\n        outcome", "        outcome"),
 ("M18 a seeded thaw ignores later freezes", "sim/suspend.rs", "                && world.freezes(node) == freeze\n", "                && (world.freezes(node) == freeze || true)\n"),
 ("M19 seeded storage pauses on by default", "sim/suspend.rs", "storage_per_mille: 0,\n                storage_max: Duration::ZERO,", "storage_per_mille: 300,\n                storage_max: Duration::from_millis(2),"),
 ("M19b the same, and armed", "sim/suspend.rs", "armed: AtomicBool::new(false)", "armed: AtomicBool::new(true)"),
 ("M20 steps stay on the stack while suspended", "sim/suspend.rs", "world.services.take_steps(self.steps_below.get())", "{ let _ = &self.steps_below; world.services.take_steps(usize::MAX.min(world.services.mid_step())) }"),
 ("M21 after-points also on failure", "fs/fault.rs", "    if result.is_ok() {\n        crate::sim::suspend::storage_point", "    if true {\n        crate::sim::suspend::storage_point"),
 ("M22 before-point of sync_data missing", "fs/fault.rs", "pausable(FsOp::SyncData, &self.path, || self.sync(FsOp::SyncData))", "self.sync(FsOp::SyncData)"),
 ("M23 rename has no point", "fs/fault.rs", "        pausable(FsOp::Rename, from, || {\n            let mut state = self.state();\n            let result = state\n                .admit", "        (|| {\n            let mut state = self.state();\n            let result = state\n                .admit"),
 ("M24 dir sync has no point", "fs/fault.rs", "pausable(FsOp::SyncDir, dir, || self.state().sync_dir(dir))", "self.state().sync_dir(dir)"),
 ("M25 sync_all has no point", "fs/fault.rs", "pausable(FsOp::SyncAll, &self.path, || self.sync(FsOp::SyncAll))", "self.sync(FsOp::SyncAll)"),
 ("M26 inline waits do not resume jobs", "sim/services.rs", "if progress || world.faults.resume_one(world, context)", "if progress || (false && world.faults.resume_one(world, context))"),
 ("M27 a suspended poll is not parked on the freeze", "sim/suspend.rs", "                world.freeze(node);\n", ""),
 ("M28 hold ignored", "sim/suspend.rs", "At::Call(_) => stop.hold,", "At::Call(_) => stop.hold.map(|_| Duration::ZERO),"),
 ("M29 path prefix ignored", "sim/suspend.rs", ".is_none_or(|p| path.starts_with(p))", ".is_none_or(|p| path.starts_with(p) || true)"),
]
only = []
env = dict(os.environ, RUSTFLAGS="--cfg tokio_unstable", CARGO_TARGET_DIR=W + "/server/target/sim")
def sh(c, **k): return subprocess.run(c, shell=True, capture_output=True, text=True, **k)
sh(f"cd {W} && git checkout -q -- server")
for name, path, a, b in M:
    if only and name.split()[0] not in only: continue
    p = R + path
    if name.startswith("M19b"):
        s = open(p).read()
        x = [m for m in M if m[0].startswith("M19 ")][0]
        assert s.count(x[2]) == 1
        s = s.replace(x[2], x[3])
    else:
        s = open(p).read()
    if s.count(a) != 1:
        print(name, "ANCHOR", s.count(a), flush=True); continue
    s2 = s.replace(a, b)
    if name.startswith("M23"):
        i = s2.index("        (|| {\n            let mut state = self.state();\n            let result = state\n                .admit")
        j = s2.index("        })\n    }\n", i)
        s2 = s2[:j] + "        })()\n    }\n" + s2[j + len("        })\n    }\n"):]
    open(p, "w").write(s2)
    t = time.time()
    try:
        r = sh("cd %s/server && { cargo test --locked -j 4 -p nabedra-rt --features sim --lib --no-run > /tmp/build.log 2>&1 || { cat /tmp/build.log; exit 9; }; } && B=$(grep -oE '[^( ]*deps/nabedra_rt-[0-9a-f]+' /tmp/build.log | tail -1) && exec timeout -k 5 120 $B sim:: --test-threads=4" % W, env=env, timeout=1800)
        out = r.stdout + r.stderr
        failed = sorted(set(re.findall(r"^test (\S+) \.\.\. FAILED", out, re.M)))
        res = re.findall(r"test result:.*", out)
        err = re.findall(r"^error.*", out, re.M)[:2] if not res else ""
        print(name, "| CAUGHT" if failed or (not res and r.returncode in (124, 137)) else ("| SURVIVED" if res else "| NO VERDICT rc %s" % r.returncode), [f.replace("sim::", "") for f in failed][:5], len(failed), res[:1] and res[0][:60], err, f"{time.time()-t:.0f}s", flush=True)
    except subprocess.TimeoutExpired:
        print(name, "| TIMEOUT (hang)", flush=True)
    sh(f"cd {W} && git checkout -q -- server")
