from pathlib import Path
import hashlib,json,os,platform,subprocess,time
root=Path('/source'); out=Path('/evidence')
def digest():
 h=hashlib.sha256()
 for p in [root/'server/Cargo.toml',root/'server/Cargo.lock',*sorted((root/'server/src').rglob('*.rs')),*sorted((root/'orm/python/src').rglob('*.py'))]:
  h.update(str(p.relative_to(root)).encode());h.update(b'\0');h.update(p.read_bytes());h.update(b'\0')
 return h.hexdigest()
initial=digest()
meta={'checkpoint':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'source_tree_sha256_before':initial,'platform':platform.platform(),'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),'python':platform.python_version(),'cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text().strip(),'memory_max':Path('/sys/fs/cgroup/memory.max').read_text().strip(),'scope':'Linux container correctness checks; source is immutable checkpoint; concurrent host work uncontrolled','stages':[]}
commands=[('engine-matrix',['python','scripts/check_engine_features.py']),('oltp-no-s3-build',['cargo','build','--locked','--manifest-path','server/Cargo.toml','--no-default-features','--features','oltp','--bin','nabedra-db']),('process',['python','server/tests/oltp_process.py','--binary','/target/debug/nabedra-db','--evidence-dir','/evidence/process','--benchmark-condition','Linux Docker container, two CPU quota, six GiB memory limit; concurrent host compilation uncontrolled']),('python-unit',['python','-m','unittest','discover','-s','orm/python/tests','-v']),('harness-unit',['python','-m','unittest','discover','-s','scripts','-p','test_*.py','-v'])]
for name,cmd in commands:
 start=time.monotonic()
 with (out/(name+'.log')).open('w') as log:
  result=subprocess.run(cmd,cwd=root,stdout=log,stderr=subprocess.STDOUT)
 entry={'name':name,'command':cmd,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start};meta['stages'].append(entry)
 meta['source_tree_sha256_after']=digest(); meta['source_unchanged']=initial==meta['source_tree_sha256_after']
 (out/'validation-summary.json').write_text(json.dumps(meta,indent=2)+'\n')
 print(json.dumps(entry),flush=True)
 if result.returncode or not meta['source_unchanged']:
  print((out/(name+'.log')).read_text()[-6000:],flush=True)
  raise SystemExit(result.returncode or 1)
