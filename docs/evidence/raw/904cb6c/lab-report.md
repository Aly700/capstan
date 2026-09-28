# Capstan fault-lab campaign

Confirmed defects found: 0

- Runs: 200000
- Passed: 200000
- Failing seeds: 0
- Recognized known failures: 0
- Unrecognized failures: 0
- Started: 2026-09-28T20:23:07Z
- Elapsed: 23m31.169260583s
- Total steps: 65602948
- Root runs: 400000
- Store transaction steps: 13285961

## Campaign options

- Seed range: 0..199999
- Requested seed limit: 200000
- Time budget: unlimited
- Scheduling stopped: seed limit reached
- Parallelism: 8
- Workers per seed: 3
- Maximum steps per seed: 1000
- Scenario: selected by seed
- Fault selection: all

Repeat the requested campaign:

    go run ./cmd/capstan-lab -seeds 200000 -parallel 8 -workers 3 -max-steps 1000 -faults all -out /Users/affan/Projects/capstan--final/docs/evidence/raw/904cb6c/lab-report.md

## Injected faults

| Fault | Count |
| --- | ---: |
| crash-server | 196971 |
| database-failure | 196948 |
| duplicate-task | 78475 |
| duplicate-timer | 1838 |
| early-timer | 11196 |
| kill-activity | 32914 |
| kill-workflow | 20059 |
| late-ack | 77062 |
| signal-in-flight | 49665 |

## Gate responses

| Response | Count |
| --- | ---: |
| approved | 33560 |
| approved-after-deadline | 15990 |
| http-503 | 16667 |
| pending | 16667 |
| timeout | 16667 |

## Failing seeds

None.
