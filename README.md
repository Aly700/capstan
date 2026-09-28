# Capstan

A durable execution engine for agent work. A workflow is ordinary TypeScript that can call
tools, call models, sleep for days, and wait on a human decision; when a worker crashes or
the server restarts mid-run, it resumes at the step it reached and does not repeat a side
effect that already happened.

Status: under construction. Design: `docs/superpowers/specs/2026-09-28-capstan-design.md`.
