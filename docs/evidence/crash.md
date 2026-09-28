# Kill the server mid-run

Verified 2026-09-28: [recording](demo-crash.cast), [GIF](demo-crash.gif).

Run and record on the local shared PostgreSQL, with an isolated database and port 7302:

```sh
scripts/demo-crash.sh
asciinema rec --headless --output-format asciicast-v2 --window-size 120x36 --capture-env TERM --return --command scripts/demo-crash.sh docs/evidence/demo-crash.cast
asciinema play docs/evidence/demo-crash.cast
```

The script builds the Go server with Go 1.26.4 and launches one real SDK worker as
`node --import tsx`, in its own process group. It starts `crashSurvivor`, waits for
the first `TimerStarted`, and sends SIGKILL to the actual **server** PID. The worker
remains alive. After four real seconds, it restarts the server against the same
database and waits for the workflow to complete. No state is reconstructed by the
script, and no event rows are edited.

The checks require result `[1,2,3,4]`, an unchanged pre-crash history prefix, four
activity completions and three distinct timer firings. The final describe and
history output come from the real `capstan` CLI. Full server/worker logs and before/
after histories remain under `.lane/crash-*`; all owned processes and the temporary
database are removed even when a check fails.

The recording uses asciinema 3.2.1 and retains wall-clock timing, without an idle-time
limit or speed edits. The recording is the evidence; the script's PASS line is backed
by assertions against real server responses. This shows recovery of this workflow;
it does not establish exactly-once execution of arbitrary external effects.

Render the same captured session as a GIF at speed 1 with `vhs scripts/demo-crash.tape`.
The GIF contains the recording’s original timing, plus the renderer’s final display pause.
