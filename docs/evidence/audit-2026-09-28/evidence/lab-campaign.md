# Capstan fault-lab campaign

Confirmed defects found: 0

- Runs: 2000
- Passed: 2000
- Failing seeds: 0
- Recognized known failures: 0
- Unrecognized failures: 0
- Started: 2026-09-28T17:43:25Z
- Elapsed: 18.185566041s
- Total steps: 657029
- Root runs: 4000
- Store transaction steps: 133884

## Campaign options

- Seed range: 0..1999
- Requested seed limit: 2000
- Time budget: unlimited
- Scheduling stopped: seed limit reached
- Parallelism: 4
- Workers per seed: 3
- Maximum steps per seed: 1000
- Scenario: selected by seed
- Fault selection: all

Repeat the requested campaign:

    go run ./cmd/capstan-lab -seeds 2000 -parallel 4 -workers 3 -max-steps 1000 -faults all -out .lane/audit-lab-campaign.md

## Injected faults

| Fault | Count |
| --- | ---: |
| crash-server | 1969 |
| database-failure | 1964 |
| duplicate-task | 770 |
| duplicate-timer | 18 |
| early-timer | 118 |
| kill-activity | 354 |
| kill-workflow | 210 |
| late-ack | 757 |
| signal-in-flight | 487 |

## Gate responses

| Response | Count |
| --- | ---: |
| approved | 335 |
| approved-after-deadline | 159 |
| http-503 | 167 |
| pending | 167 |
| timeout | 167 |

## Failing seeds

None.
