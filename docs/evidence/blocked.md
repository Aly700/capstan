# A changed workflow blocks, then resumes

## Historical recording

The original recording is retained. A new final-code rerun is linked below.

Verified 2026-09-28: [recording](demo-blocked.cast).

```sh
scripts/demo-blocked.sh
asciinema rec --headless --output-format asciicast-v2 --window-size 120x36 --capture-env TERM --return --command scripts/demo-blocked.sh docs/evidence/demo-blocked.cast
asciinema play docs/evidence/demo-blocked.cast
```

The script uses the real server on port 7303, a fresh PostgreSQL database and three
successive real SDK worker processes. V1 records `activity("step")`, completes it,
and waits for a signal. The script stops V1, starts V2 with
`activity("revisedStep")`, then signals the live run. Replay finds the mismatch at
history event 5, and the server records `RunBlocked`. The CLI prints the mismatch
and the history.

The third worker uses:

```ts
activity(patched("revised-step-v2") ? "revisedStep" : "step", ...)
```

After the explicit `capstan resume`, the old run follows its recorded branch and
returns `{value:1}` with one activity completion. The check compares its original
history to the final prefix. A new run on the same worker records the patch marker
and returns `{value:10}`. Both histories come from the real CLI.

The `.cast` retains real wall-clock timing, including the script’s announced
3-second/2-second pauses to read the displayed states. The script never edits history or run
status directly. Each worker is one Node process in its own process group. Cleanup
stops every owned process and drops only the script's database; logs stay in `.lane/`.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The real V2 worker blocked the old run at history event 5. An explicit
resume with the patched worker completed it with value 1 and one activity
completion; a new run recorded the patch marker and returned value 10.
[Full CLI histories and assertions](raw/904cb6c/demo-blocked.log). The original
recording was retained.
<!-- final-numbers:end -->
