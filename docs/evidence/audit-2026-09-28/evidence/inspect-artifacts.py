#!/usr/bin/env python3
"""Read-only checks of committed image, recording, and lab-count artifacts."""
from decimal import Decimal
import json
from pathlib import Path
import re

from PIL import Image

root = Path(__file__).resolve().parents[4]
evidence = root / 'docs/evidence'
output = {
    'scope': 'Artifact arithmetic and structure; this does not authenticate historical recordings or campaign counts.',
    'images': {},
    'recordings': {},
}
for path in sorted(evidence.glob('ui-*.png')):
    with Image.open(path) as picture:
        output['images'][path.name] = {
            'width': picture.width,
            'height': picture.height,
            'metadata_keys': sorted(picture.info),
        }
with Image.open(evidence / 'demo-crash.gif') as picture:
    milliseconds = 0
    for frame in range(picture.n_frames):
        picture.seek(frame)
        milliseconds += picture.info.get('duration', 0)
    output['images']['demo-crash.gif'] = {
        'width': picture.width,
        'height': picture.height,
        'frames': picture.n_frames,
        'duration_seconds': str(Decimal(milliseconds) / 1000),
    }
assert output['images']['demo-crash.gif'] == {
    'width': 1200, 'height': 720, 'frames': 446, 'duration_seconds': '17.84',
}
for name, dimensions in {
    'ui-list.png': (1280, 1000),
    'ui-completed.png': (1280, 1410),
    'ui-blocked.png': (1280, 2435),
    'ui-mobile.png': (390, 3802),
}.items():
    assert (output['images'][name]['width'], output['images'][name]['height']) == dimensions
markers = ('Waiting 4', 'Waiting 5', '3 real seconds', '2 real seconds')
for path in sorted(evidence.glob('*.cast')):
    records = [json.loads(line, parse_float=Decimal) for line in path.read_text().splitlines()]
    header, events = records[0], records[1:]
    assert header['version'] == 2
    assert all(previous[0] <= current[0] for previous, current in zip(events, events[1:]))
    pauses = []
    for index, event in enumerate(events[:-1]):
        found = [marker for marker in markers if marker in event[2]]
        if found:
            following = events[index + 1]
            pauses.append({
                'marker': found[0],
                'at_seconds': str(event[0]),
                'next_event_seconds': str(following[0]),
                'gap_seconds': str(following[0] - event[0]),
            })
    output['recordings'][path.name] = {
        'events': len(events),
        'duration_seconds': str(events[-1][0]),
        'monotonic': True,
        'captured_environment_keys': sorted(header['env']),
        'announced_pauses': pauses,
    }

# Check derived historical arithmetic without treating its input seed count as proved.
delta = (evidence / 'lab-delta1-2026-09-28.md').read_text()
def number(pattern):
    return int(re.search(pattern, delta, re.M).group(1).replace(',', ''))
seeds = number(r'^- Runs: ([\d,]+)$')
roots = number(r'^- Root runs: ([\d,]+)$')
executions = number(r'Thus this campaign ran ([\d,]+) scenario executions')
all_roots = number(r'containing ([\d,]+) initial root runs')
probes = number(r'plus ([\d,]+)\s+protocol probes')
assert roots == executions == seeds * 2
assert all_roots == seeds * 4
assert probes == seeds
output['historical_delta_arithmetic'] = {
    'input_seed_count_unverified': seeds,
    'faulted_root_runs': roots,
    'baseline_plus_faulted_executions': executions,
    'baseline_plus_faulted_initial_roots': all_roots,
    'protocol_probes': probes,
    'arithmetic_matches': True,
}

catalogue = (root / 'internal/lab/scenario_catalog.go').read_text().split('func peerScenario()', 1)[0]
scenarios = re.findall(r'\{name: "([^"]+)"', catalogue)
harness = (root / 'internal/lab/harness.go').read_text()
fault_symbols = re.search(r'func AllFaults\(\) \[\]FaultKind \{\s*return \[\]FaultKind\{([^}]+)\}', harness).group(1)
fault_values = dict(re.findall(r'(\w+)\s+FaultKind = "([^"]+)"', harness))
faults = [fault_values[symbol.strip()] for symbol in fault_symbols.split(',')]
probe_source = (root / 'internal/lab/probes.go').read_text()
probe_array = re.search(r'var protocolProbeNames = \[\]string\{([^}]+)\}', probe_source).group(1)
probe_names = re.findall(r'"([^"]+)"', probe_array)
audit_report = (Path(__file__).parent / 'lab-campaign.md').read_text()
workers = int(re.search(r'^- Workers per seed: (\d+)$', audit_report, re.M).group(1))
assert (len(scenarios), len(faults), len(probe_names), workers) == (12, 9, 10, 3)
output['current_lab_catalogues'] = {
    'primary_scenarios': scenarios,
    'primary_scenario_count': len(scenarios),
    'fault_types': faults,
    'fault_type_count': len(faults),
    'protocol_probes': probe_names,
    'protocol_probe_count': len(probe_names),
    'workers_per_seed_in_audit_campaign': workers,
}
print(json.dumps(output, indent=2))
