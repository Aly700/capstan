from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from collections import Counter
import json, math, re, subprocess
D = json.loads(Path('docs/evidence/load-results.json').read_text())
A = D[:11]
B = [x for x in D if x.get('phase') in ('after','after-repeat')]
def trunc(x): return format(Decimal(str(x)).quantize(Decimal('0.0001'), rounding=ROUND_DOWN), '.4f')
def cells(line): return [x.strip() for x in line.strip().strip('|').split('|')]
def result(d):
 r=d['result']; return [str(r['concurrency']),str(r['runs']), d['command'].split()[-1],*[trunc(r[k]) for k in ('runs_per_second','activities_per_second','p50_ms','p99_ms')], str(r['errors'])]
def cpu(d):
 s=d['sampler'];return [str(s['samples']),trunc(s['postgres_cpu_percent']),trunc(s['server_cpu_percent']),trunc(sum(s['worker_cpu_percent']))]
text=Path('docs/evidence/load.md').read_text(); lines=text.splitlines()
results=[cells(l) for l in lines if re.match(r'^\| (10|25|50|100|200|800) \| \d+ \| [148] \|',l)]
assert results==[result(d) for d in A],(results,[result(d) for d in A])
print('PASS load original result table: all 88 numeric cells match downward truncation')
original_cpu=[cells(l) for l in lines if re.match(r'^\| (10|25|50|100|200|800) / [148] \| \d+ \|',l)]
want=[[f"{d['result']['concurrency']} / {d['command'].split()[-1]}",*cpu(d),str(d['sampler']['max_ready_tasks']),str(d['sampler']['max_waiting_locks'])] for d in A]
assert original_cpu==want,(original_cpu,want)
print('PASS load original CPU table: all 66 numeric cells match JSON')
final=[cells(l) for l in lines if re.match(r'^\| after(?:-repeat)? / \d+ \| \d+ \|',l)]
want=[[f"{d['phase']} / {i%11+1}",*result(d),'yes' if d['sampler']['other_active_databases'] else 'no'] for i,d in enumerate(B)]
assert final==want,(final,want)
print('PASS load final result table: all 176 numeric cells match JSON')
final_cpu=[cells(l) for l in lines if re.match(r'^\| after(?:-repeat)? / \d+ \| \d+ / \d+ \|',l)]
want=[[f"{d['phase']} / {i%11+1}",f"{d['result']['concurrency']} / {d['command'].split()[-1]}",*cpu(d),*[str(d['sampler'][k]) for k in ('max_active_or_in_transaction','max_ready_tasks','max_waiting_locks','deadlocks_delta')]] for i,d in enumerate(B)]
assert final_cpu==want,(final_cpu,want)
print('PASS load final CPU table: all 176 numeric cells match JSON')
selected=[(11,12),(18,19),(20,21),(33,34),(36,37)]
comparison=[cells(l)[1:] for l in lines if re.match(r'^\| (Original,|D30,|Targeted queue|Run/child)',l)]
want=[]
for ix,iy in selected:
 x,y=D[ix],D[iy];want.append([trunc(x['result']['runs_per_second']),trunc(y['result']['runs_per_second']),trunc(x['result']['p99_ms']),trunc(y['result']['p99_ms']),f"{x['sampler']['transactions_delta']:,} / {y['sampler']['transactions_delta']:,}"])
assert comparison==want,(comparison,want)
print('PASS load incremental comparison: all 30 numeric cells match JSON')
pools=[cells(l) for l in lines if re.match(r'^\| (10|20|40|60) \| [\d.]+',l) and len(cells(l))==3]
want=[[str(pool),trunc(D[ix]['result']['runs_per_second'])+(' *' if D[ix]['sampler']['other_active_databases'] else ''),trunc(D[iy]['result']['runs_per_second'])] for pool,ix,iy in [(10,24,25),(20,18,19),(40,20,21),(60,22,23)]]
assert pools==want,(pools,want)
print('PASS load pool comparison: 12 numeric cells match JSON')
def rng(xs):
 lo,hi=min(xs),max(xs);return trunc(lo) if lo==hi else f'{trunc(lo)}–{trunc(hi)}'
ranges=[cells(l) for l in lines if re.match(r'^\| (10|25|50|100|200|800) / [148] \|',l) and len(cells(l))==5]
want=[]
for c,w in [(10,4),(25,4),(50,4),(100,4),(200,4),(800,4),(50,1),(50,8)]:
 selected_groups=[[d for d in group if d['result']['concurrency']==c and int(d['command'].split()[-1])==w] for group in [A,B]]
 want.append([f'{c} / {w}',*[rng([d['result'][k] for d in group]) for k in ('runs_per_second','p99_ms') for group in selected_groups]])
assert ranges==want,(ranges,want)
print('PASS all before/after ranges computed from all retained entries')
for d in D:
 r=d['result'];a=d['audit'];assert r['runs']==r['completed']==a['runs']==a['completed'];assert r['activities']==a['activity_completions']==5*r['runs']
 assert r['errors']==a['other']==a['remaining_tasks']==a['task_failures']==0
 assert math.isclose(r['runs_per_second'],r['completed']/r['elapsed_seconds'],abs_tol=1e-8)
 assert math.isclose(r['activities_per_second'],r['activities']/r['elapsed_seconds'],abs_tol=1e-8)
print('PASS all 61 run/audit entries internally agree, zero errors, task failures or remaining tasks')
print('ORIGINAL',len(A),sum(d['result']['runs'] for d in A),sum(d['result']['activities'] for d in A))
print('AFTER',len(B),sum(d['result']['runs'] for d in B),sum(d['result']['activities'] for d in B),'samples',sum(d['sampler']['samples'] for d in B),'other_active',sum(bool(d['sampler']['other_active_databases']) for d in B))
print('FAIL raw directories absent',sum(not Path(d['raw_directory']).exists() for d in D),'of',len(D),'(latency percentile and CPU/lock sampling recomputation unavailable)')
for p in sorted(Path('docs/evidence').glob('*.cast')):
 records=[json.loads(x) for x in p.read_text().splitlines()];events=records[1:]
 assert records[0]['version']==2;assert all(a[0]<=b[0] for a,b in zip(events,events[1:]));text=''.join(x[2] for x in events if x[1]=='o');assert 'PASS:' in text
 print('PASS CAST',p.name,'events',len(events),'duration',events[-1][0],'env keys',sorted(records[0]['env']))
 if p.stem=='demo-cost':
  assert '|     0.010000 | 0.000034 |           14 |             4 |' in text
  assert Decimal(14)/1000000+Decimal(4)*5/1000000==Decimal('0.000034')
  print('PASS cost ledger cast and pricing arithmetic 14 / 4 -> 0.000034; provider provenance not independently observable')
fixtures=[json.loads(p.read_text()) for p in Path('conformance/fixtures').glob('*.json')]
shared=[f for f in fixtures if f.get('only')!=['ts']]
assert len(fixtures)==54 and len(shared)==53
assert sum('commands' in f['expect'] for f in shared)==39
assert sum('mismatch' in f['expect'] for f in shared)==14
exports=re.findall(r'^export (?:async )?function (\w+)',Path('conformance/workflows.ts').read_text(),re.M)
assert len(exports)==29
print('PASS fixtures 39 shared command cases, 14 mismatches, 1 TS-only, 29 workflow exports')
files=subprocess.check_output(['git','ls-files','docs/evidence'],text=True).splitlines()
patterns={'Anthropic':r'sk-ant-[A-Za-z0-9_-]{12,}','OpenAI':r'sk-[A-Za-z0-9_-]{20,}','AWS':r'(?:AKIA|ASIA)[A-Z0-9]{16}','DB URL':r'postgres(?:ql)?://[^\s"<>]+','private key':r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----','evidence-key':r'cap_evidence_[0-9a-f]{48}','Bearer':r'Bearer\s+[A-Za-z0-9_./+:-]{16,}'}
for label,regex in patterns.items():
 hits=[f'{f}:{n}' for f in files if Path(f).suffix not in ('.png','.gif') for n,line in enumerate(Path(f).read_text().splitlines(),1) if re.search(regex,line)]
 assert not hits,(label,hits)
 print('PASS evidence credential scan',label,'matches',len(hits))

# The new audit measurements include their per-run observations.
audit = Path(__file__).resolve().parent
for c in (50, 200):
    folder = audit / f'load-c{c}'
    samples = [json.loads(s) for s in (folder / 'runs.jsonl').read_text().splitlines()]
    emitted = json.loads((folder / 'result.json').read_text())
    persisted = json.loads((folder / 'audit.json').read_text())
    latencies = sorted(s['latency_ms'] for s in samples)
    assert len(samples) == emitted['completed'] == persisted['completed'] == 1000
    assert len(set(s['run_id'] for s in samples)) == 1000
    assert sum(s['activities'] for s in samples) == emitted['activities'] == persisted['activity_completions'] == 5000
    for percentile in (50, 99):
        assert latencies[math.ceil(len(samples) * percentile / 100) - 1] == emitted[f'p{percentile}_ms']
    print('PASS fresh audit load raw samples', c, 'runs=1000 activities=5000 percentile nearest-rank checked')
report = json.loads((audit / 'mutations-fixed/results.json').read_text())
assert len(report['results']) == 26
assert Counter(r['status'] for r in report['results']) == Counter(caught=25, equivalent=1)
for name in ('baseline', 'M003'):
    events = [json.loads(s) for s in (audit / f'mutations-fixed/{name}.jsonl').read_text().splitlines()]
    assert any(e.get('Action') == 'pass' and e.get('Test') == 'TestLab' for e in events)
    assert sum('mutation seed ' in e.get('Output', '') for e in events) == 2000
print('PASS fresh mutation raw data: baseline and M003 2000 seeds each; 25 caught, 1 equivalent')
assert 'steps=53835 duplicate_acks=7589 database_errors=250 server_crashes=250' in (audit / 'pg-campaign.log').read_text()
assert '--- PASS: TestPostgresCampaign' in (audit / 'pg-campaign.log').read_text()
print('PASS fresh PostgreSQL campaign reproduces all500 seed/count metrics')
rev = 'c5bba0856b40d7f56956d01c893cf12f0df31f60'
import hashlib
h = hashlib.sha256()
for path in subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', rev, 'internal/lab'], text=True).splitlines():
    if '/mutants/' in path:
        continue
    data = subprocess.check_output(['git', 'show', f'{rev}:{path}'])
    relative = path.removeprefix('internal/lab/')
    h.update(f'{relative}\0{len(data)}\0'.encode())
    h.update(data)
assert h.hexdigest() == '908de63ea5cbcf56b204ad507b00605d099a198487b2e9c38ef3324dfe8e6079'
print('PASS historical mutation snapshot SHA256 independently recomputed from the recorded commit')
expected = {}
for line in Path('docs/evidence/lab-mutation.md').read_text().splitlines():
    if re.match(r'^\| M\d{3} \|', line):
        row = cells(line)
        expected[row[0]] = (row[2], -1 if row[3] == '—' else int(row[3]))
assert len(expected) == 26
for result in report['results']:
    assert expected[result['mutant']['id']] == (result['status'], result['seed'])
print('PASS all26 mutation result/status and first-seed cells reproduce the historical table')
store_interface = Path('internal/store/store.go').read_text().split('type Tx interface {', 1)[1].split('\n}', 1)[0]
methods = set(re.findall(r'^\s*([A-Z]\w*)\(', store_interface, re.M))
wrappers = set()
for path in Path('internal/lab').glob('*.go'):
    if not path.name.endswith('_test.go'):
        wrappers.update(re.findall(r'^func \(\w+ \*faultTx\) (\w+)\(', path.read_text(), re.M))
assert len(methods) == 35 and not methods - wrappers
print('PASS fault wrapper covers all35 Tx methods; historical lab-l2 count34 is stale after D30')
