#!/usr/bin/env python3
"""Run the original evidence programs in audit-owned databases and ports."""
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[4]
os.chdir(root)
mode = sys.argv[1] if len(sys.argv) > 1 else ''
if mode not in ('crash', 'idle', 'load50', 'load200', 'ui'):
    raise SystemExit('usage: reproduce.py crash|idle|load50|load200|ui')
scratch = root / '.lane' / 'audit-scripts'
scratch.mkdir(parents=True, exist_ok=True)
source = (root / 'scripts/evidence-lib.mjs').read_text()
source = source.replace('dirname(dirname(fileURLToPath(import.meta.url)))', 'dirname(dirname(dirname(fileURLToPath(import.meta.url))))')
source = source.replace('port < 7300 || port > 7499', 'port < 7600 || port > 7699')
source = source.replace('databasePrefix = "capstan_evidence"', 'databasePrefix = "capstan_audit"')
source = source.replace('["capstan_evidence", "capstan_perf"]', '["capstan_audit"]')
(scratch / 'evidence-lib.mjs').write_text(source)
name, replacements, args = {
    'crash': ('demo-crash', [('7302', '7652')], []),
    'idle': ('demo-idle-wait', [('7304', '7654')], []),
    'load50': ('run-load', [('"capstan_evidence"', '"capstan_audit"'), ('|| 7301', '|| 7651')], ['50', '1000', '4']),
    'load200': ('run-load', [('"capstan_evidence"', '"capstan_audit"'), ('|| 7301', '|| 7651')], ['200', '1000', '4']),
    'ui': ('check-ui', [('"ui", 7300', '"ui", 7650'), ('"docs/evidence/ui-', '".lane/audit-ui-')], []),
}[mode]
source = (root / f'scripts/{name}.mjs').read_text()
for old, new in replacements:
    source = source.replace(old, new)
script = scratch / f'{name}.mjs'
script.write_text(source)
env = dict(os.environ, GOTOOLCHAIN='go1.26.4', CAPSTAN_LOAD_PHASE='audit', CAPSTAN_LOAD_PORT='7651', CAPSTAN_LOAD_DB_PREFIX='capstan_audit')
env['PATH'] = str(Path.home() / '.local/bin') + ':' + env['PATH']
command = ['node', str(script), *args]
if mode == 'ui':
    command = ['npx', '--yes', '--package', '@playwright/cli@0.1.21', '-c', 'node .lane/audit-scripts/check-ui.mjs']
raise SystemExit(subprocess.call(command, cwd=root, env=env))
