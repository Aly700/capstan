-- Capstan schema, version 1. Frozen contract for the build: pgstore implements it, and the
-- in-memory store mirrors its semantics. Protobuf values are stored as binary (bytea) so
-- they round-trip exactly; the server never parses user payloads.
--
-- Zero Go times map to NULL. Durations are stored in milliseconds.

create table run (
    run_id                text primary key check (run_id ~ '^[A-Za-z0-9._:~-]{1,200}$') -- '~' is reserved for continuations (D19),
    workflow_type         text        not null check (length(workflow_type) between 1 and 200),
    task_queue            text        not null check (length(task_queue) between 1 and 200),
    status                smallint    not null check (status between 1 and 7),
    input                 bytea,
    result                bytea,
    failure               bytea,
    task_timeout_ms       bigint      not null check (task_timeout_ms > 0),
    run_timeout_ms        bigint      not null default 0 check (run_timeout_ms >= 0),
    run_deadline          timestamptz,
    started_at            timestamptz not null,
    closed_at             timestamptz,
    last_event_id         bigint      not null default 0 check (last_event_id >= 0),
    workflow_task_id      bigint      not null default 0,
    in_flight             boolean     not null default false,
    cancel_requested      boolean     not null default false,
    continued_from_run_id text        not null default '',
    continued_to_run_id   text        not null default '',
    identity              text        not null default ''
);

create index run_status_idx   on run (status, run_id);
create index run_type_idx     on run (workflow_type, run_id);
-- open runs (running = 1, blocked = 6) with a deadline, for the run-timeout sweeper
create index run_deadline_idx on run (run_deadline) where run_deadline is not null and status in (1, 6);

-- The history. Append-only and gap-free: the trigger below forbids update and delete, and
-- pgstore rejects appends whose ids do not continue the sequence.
create table event (
    run_id   text        not null references run (run_id),
    event_id bigint      not null check (event_id > 0),
    type     smallint    not null,
    at       timestamptz not null,
    data     bytea       not null, -- a serialized capstan.v1.HistoryEvent
    primary key (run_id, event_id)
);

create function event_is_append_only() returns trigger language plpgsql as $$
begin
    raise exception 'capstan: history is append-only (% on event)', tg_op;
end $$;

create trigger event_append_only
    before update or delete on event
    for each row execute function event_is_append_only();

-- External events that arrived while a workflow task was in flight, flushed into history
-- (in id order) when the task completes, fails, or times out.
create table inbox (
    id          bigserial primary key,
    run_id      text        not null references run (run_id),
    data        bytea       not null, -- a serialized capstan.v1.HistoryEvent, event_id unset
    received_at timestamptz not null
);

create index inbox_run_idx on inbox (run_id, id);

-- Workflow tasks and activity attempts waiting for, or held by, a worker.
create table task (
    id                 bigserial primary key,
    kind               smallint    not null check (kind in (1, 2)), -- 1 workflow, 2 activity
    run_id             text        not null references run (run_id),
    task_queue         text        not null,
    scheduled_event_id bigint      not null,
    attempt            integer     not null check (attempt >= 1),
    visible_at         timestamptz not null,
    leased_until       timestamptz,
    worker_id          text        not null default '',
    started_at         timestamptz,
    scheduled_at       timestamptz not null,
    check_at           timestamptz,
    activity           bytea,      -- serialized ActivityScheduledAttributes (activities only)
    last_heartbeat_at  timestamptz,
    heartbeat_details  bytea,
    last_failure       bytea,
    cancel_requested   boolean     not null default false,
    started_event_id   bigint      not null default 0
);

-- the claim path: unleased tasks of one kind on one queue, oldest first
create index task_claim_idx on task (kind, task_queue, visible_at, id) where leased_until is null;
create index task_check_idx on task (check_at) where check_at is not null;
create index task_run_idx   on task (run_id);

create table timer (
    run_id           text        not null references run (run_id),
    seq              bigint      not null,
    started_event_id bigint      not null,
    due_at           timestamptz not null,
    primary key (run_id, seq)
);

create index timer_due_idx on timer (due_at);

create table approval (
    run_id             text        not null references run (run_id),
    approval_id        text        not null check (length(approval_id) between 1 and 200),
    seq                bigint      not null,
    requested_event_id bigint      not null,
    source             smallint    not null check (source in (1, 2)), -- 1 gate, 2 human
    gate_decision_id   text        not null default '',
    status             smallint    not null check (status between 1 and 4), -- pending, approved, denied, expired
    due_at             timestamptz,
    check_at           timestamptz,
    gate_polls         integer     not null default 0,
    requested_at       timestamptz not null,
    resolved_at        timestamptz,
    resolver           text        not null default '',
    choice             text        not null default '',
    note               text        not null default '',
    primary key (run_id, approval_id)
);

create index approval_check_idx on approval (check_at) where status = 1 and check_at is not null;

create table signal_request (
    run_id     text not null references run (run_id),
    request_id text not null,
    primary key (run_id, request_id)
);

-- The model-call ledger. Reserved rows count at their estimate until finished.
create table ai_call (
    id                 bigserial primary key,
    run_id             text          not null references run (run_id),
    activity_seq       bigint        not null,
    model              text          not null,
    status             smallint      not null check (status between 1 and 3), -- reserved, finished, failed
    estimate_usd       numeric(12, 6) not null check (estimate_usd >= 0),
    cost_usd           numeric(12, 6) not null default 0 check (cost_usd >= 0),
    input_tokens       bigint        not null default 0,
    output_tokens      bigint        not null default 0,
    cache_read_tokens  bigint        not null default 0,
    cache_write_tokens bigint        not null default 0,
    error_code         text          not null default '',
    at                 timestamptz   not null,
    finished_at        timestamptz
);

create index ai_call_at_idx  on ai_call (at);
create index ai_call_run_idx on ai_call (run_id);
