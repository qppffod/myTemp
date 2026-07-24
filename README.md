# myTemp

A small distributed workflow engine written in Go, built for **educational
purposes**. It takes inspiration from [Temporal](https://temporal.io) and
explores the core ideas behind durable workflow execution: persisted event
histories, task queues, and worker-driven activity execution.

> ⚠️ This is a learning project, not production software (despite the module
> name `github.com/qppffod/myTemp`). It exists to make the *replay model* of
> durable execution concrete and hackable.

## What is this?

In a durable workflow engine, a workflow is **not** a long-lived process. It is
a deterministic function that is re-executed ("replayed") from the top every
time something new happens, with a persisted **event history** standing in for
everything it has already done. Activities (the side-effecting steps) run on
workers and report their results back as events. If a worker crashes mid-flight,
another worker picks the work up and replays to exactly where it left off.

`myTemp` implements that loop end to end against Postgres, over gRPC, with a
tiny Go SDK for authoring workflows and activities.

## Architecture

```
                 gRPC :7233
  ┌──────────┐  ───────────►  ┌─────────────────────────┐
  │  worker  │   poll task    │         engine          │     ┌──────────┐
  │  (SDK)   │  ◄───────────  │  frontend → history →   │ ──► │ Postgres │
  │          │   history      │       persistence       │     │ (events, │
  │ replays  │  ───────────►  │  (source of truth)      │ ◄── │  tasks)  │
  │ workflow │  commands      └─────────────────────────┘     └──────────┘
  └──────────┘
  ┌──────────┐
  │   api    │  StartWorkflow ─────────────► engine
  │  (HTTP)  │
  └──────────┘
```

- **engine** (`cmd/engine`) — the source of truth, backed by Postgres. Layered:
  - `internal/frontend` — gRPC server bootstrap (listens on `:7233`), wiring in
    the logging + Prometheus interceptors and reflection.
  - `internal/frontend/grpc` — `EngineService` RPC handlers; a thin translation
    layer between protobuf and the internal types, including the request-logging
    interceptor and domain-error → gRPC status mapping.
  - `internal/metrics` — the engine's Prometheus domain counters, incremented by
    the history layer as transactions commit.
  - `internal/history` — the business logic of durable execution; owns all
    multi-statement transactions (start workflow, turn worker commands into
    events + tasks).
  - `internal/persistence` — raw `pgx` data access over the `events`, `tasks`,
    `timers`, and `workflow_executions` tables.
- **SDK** (`sdk/`) — `Client` (gRPC wrapper), `Worker` (poll loops + replay
  driver), and `sdk/workflow` (the workflow-author API: `Context`,
  `ExecuteActivity`, `ActivityFuture`, `Sleep`, `ReceiveSignal`).
- There is **no in-process scheduling** — all coordination happens through the
  `tasks` and `timers` tables, scanned by background loops in the engine.

### The replay loop

1. `StartWorkflow` writes a `WorkflowStarted` event + a `workflow` task.
2. A worker polls the task, receives the **full event history**, and re-runs the
   registered workflow function from the top.
3. `workflow.ExecuteActivity(ctx, name, input)` checks history: if this call
   (keyed by `ActivityName` + a per-call `ActivityIndex`) was already scheduled,
   it does nothing new; otherwise it appends a `ScheduleActivity` command.
4. `ActivityFuture.Get(&out)` looks for a matching `ActivityCompleted` event and
   JSON-decodes it; an `ActivityFailed` event surfaces as a Go `error`. If
   neither exists yet, it panics with `ErrPendingActivity` to unwind the
   function — the worker recovers, submits the commands so far, and the workflow
   stays paused.
5. The engine schedules the activity; a worker runs it and reports completion or
   failure, which appends an event and enqueues a **new** workflow task → replay
   happens again, now getting past that `Get`.

**Determinism is mandatory:** because the function is replayed, activities must
be scheduled in a stable order. Side effects (e.g. logging) re-run on every
replay — that's expected.

Typed data flows end to end: workflow input, activity input, and activity result
are all JSON-encoded, so one activity's result struct can be passed straight
into the next as input.

### Beyond activities

The same replay model powers the richer primitives:

- **Durable timers** — `workflow.Sleep(ctx, d)` appends a `StartTimer` command;
  the engine persists a row in the `timers` table (`fire_at = now() + d`) and a
  background `scanTimers` loop fires due timers (a `TimerFired` event + a new
  workflow task) so the workflow wakes up exactly where it slept.
- **Signals** — `workflow.ReceiveSignal(ctx, name, &out)` suspends the workflow
  until a signal arrives. `Client.SignalWorkflow` appends a `SignalReceived`
  event out-of-band and enqueues a workflow task; signals of the same name are
  consumed in order.
- **Activity retries** — a failed activity is automatically retried with
  exponential backoff + jitter before the failure is surfaced to the workflow
  (see *Features*).

## Quickstart

A full end-to-end run needs **three processes**: engine + worker + api.

```bash
# 1. Start Postgres + Prometheus + Grafana (schema is auto-migrated on engine boot)
docker compose up -d

# 2. Run the engine (gRPC on :7233, metrics on :2112; runs ./migrations first)
make grun            # == go run ./cmd/engine

# 3. Run the example worker (connects to localhost:7233)
go run ./examples/worker

# 4. Run the example API (HTTP on :3000)
go run ./examples/api

# 5. Kick off a workflow (starts an approval workflow that waits for a signal)
curl -XPOST localhost:3000/test -d '{"OrderID":1,"Items":["pizza"]}'

# 6. Unblock it by sending the "approval" signal
curl -XPOST localhost:3000/approve
```

`examples/worker/main.go` registers three example workflows:

- `TestWorkflow` — a toy order pipeline running three activities sequentially,
  passing typed results down the chain: `CheckStock → ChargeCard → Ship`.
  `ChargeCard` is deliberately flaky (fails twice) to demonstrate activity
  retries.
- `TestTimerWorkflow` — same pipeline with a `workflow.Sleep(c, 30s)` in the
  middle to show durable timers.
- `ApprovalWorkflow` — runs an activity, then blocks on a `"approval"` signal
  (delivered by `POST /approve`) before shipping. This is what the quickstart
  above triggers.

The engine reads `DATABASE_URL` (defaults to
`postgres://postgres:password@localhost:5432/myengine?sslmode=disable`, matching
`docker-compose.yml`). Set `DEBUG=1` to raise the log level from info to debug.

## Observability

`docker compose up -d` brings up a full metrics stack alongside Postgres:

| Service    | URL                              | Notes                                     |
| ---------- | -------------------------------- | ----------------------------------------- |
| engine     | `http://localhost:2112/metrics`  | Prometheus exposition (host process)      |
| Prometheus | `http://localhost:9090`          | scrapes the engine every 5s               |
| Grafana    | `http://localhost:3001`          | anonymous viewer; `admin`/`admin` to edit |

Because the engine typically runs on the host (`make grun`) while Prometheus
runs in Docker, the scrape config reaches it via `host.docker.internal:2112`
(mapped to the host gateway for Linux in `docker-compose.yml`).

Three families of metrics are exported on `:2112/metrics`:

- **Domain counters** (`myengine_*`, defined in `internal/metrics`) — workflows
  started / terminated, activities scheduled / finished, activity retries,
  timers started / fired, signals delivered, leases reclaimed, and task polls.
  These are incremented **after** the owning transaction commits, so they track
  durable outcomes rather than rolled-back attempts.
- **gRPC server metrics** — per-method request counts and handling-time
  histograms, pre-initialized to zero so every series exists before the first
  request.
- **Go runtime / process collectors** — standard `go_*` and `process_*` series.

Grafana auto-provisions the Prometheus datasource and the `myengine` dashboard
from `observability/`.

## Writing a workflow

```go
// Workflow: func(*workflow.Context, InputT) error
func TestWorkflow(c *workflow.Context, order PizzaOrder) error {
    var stock StockResult
    if err := workflow.ExecuteActivity(c, "CheckStock", order).Get(&stock); err != nil {
        return err
    }

    // A → B: CheckStock's typed result is the input to ChargeCard.
    var charge ChargeResult
    if err := workflow.ExecuteActivity(c, "ChargeCard", stock).Get(&charge); err != nil {
        return err
    }
    return nil
}

// Activity: func(context.Context, InputT) (ResultT, error)
func CheckStock(ctx context.Context, order PizzaOrder) (StockResult, error) {
    return StockResult{Available: true, Item: order.Items[0]}, nil
}
```

Register them on a worker by their **bare Go function name** (the registered
name must match the identifier, since it's used as the `workflow_type` /
`activity_name` over the wire):

```go
worker := sdk.NewWorker(client, "test")
worker.RegisterWorkflow(TestWorkflow)
worker.RegisterActivity(CheckStock)
worker.Run(ctx)
```

Workflows can also sleep and wait for signals — both are durable and replay-safe:

```go
func ApprovalWorkflow(c *workflow.Context, order PizzaOrder) error {
    // Durable timer: the workflow is suspended and resumed by the engine.
    workflow.Sleep(c, 30*time.Second)

    // Block until an "approval" signal arrives (sent via Client.SignalWorkflow).
    var decision Decision
    workflow.ReceiveSignal(c, "approval", &decision)
    if !decision.Approved {
        return fmt.Errorf("approval rejected")
    }
    return nil
}
```

## Features

### Implemented

- ✅ **Durable event-sourced execution** — workflows persisted as event
  histories in Postgres and replayed deterministically.
- ✅ **Sequential activity execution** with typed, JSON-encoded data flowing
  from one activity into the next.
- ✅ **gRPC engine API** — `StartWorkflow`, `PollWorkflowTask`,
  `RespondWorkflowTaskCompleted`, `PollActivityTask`,
  `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `SignalWorkflow`.
- ✅ **Durable timers** — `workflow.Sleep` persists a timer; a background
  `scanTimers` loop fires due timers and resumes the workflow via replay.
- ✅ **Signals** — `workflow.ReceiveSignal` suspends a workflow until a signal is
  delivered (`Client.SignalWorkflow`); same-named signals are consumed in order.
- ✅ **Automatic activity retries** — a failed activity is rescheduled with
  exponential backoff + jitter (`DefaultRetryPolicy`: 3 attempts, 1s initial,
  ×2.0, capped at 1m) using the task's `visibility_time`. The `ActivityFailed`
  event is only written once attempts are exhausted, and then surfaces as an
  `error` to the workflow on replay.
- ✅ **Concurrent task dispatch** — `FOR UPDATE SKIP LOCKED` + a partial index
  (`lease_owner IS NULL`) lets multiple workers poll the shared `tasks` table
  without handing out the same task twice.
- ✅ **Task leasing + crash recovery** — `PollTask` leases a task to the polling
  worker (`lease_owner` = worker UUID, `lease_expires_at = now() + 30s`) in the
  same transaction. A background loop in the engine reclaims leases that expire
  (worker crashed or stalled), making the task pollable again.
- ✅ **Graceful shutdown** — the engine runs the gRPC server and its background
  loops under a `signal.NotifyContext` (SIGINT/SIGTERM); the metrics HTTP server
  drains with a 5s timeout and the gRPC server does a `GracefulStop`.
- ✅ **Auto-migration** — the engine runs SQL migrations from `./migrations` on
  boot.
- ✅ **Prometheus metrics** — domain counters (`myengine_*`: workflows started /
  terminated, activities scheduled / finished, activity retries, timers, signals,
  leases reclaimed, task polls), plus gRPC server metrics (per-method request
  counts + handling-time histograms) and the standard Go runtime / process
  collectors, exposed on `:2112/metrics`. Domain counters increment only after
  the owning transaction commits, so they reflect durable truth.
- ✅ **Grafana dashboard** — a provisioned dashboard and Prometheus datasource
  ship under `observability/`, wired up by `docker-compose.yml`.
- ✅ **Structured JSON logging** — the engine logs via `slog` (JSON handler;
  `DEBUG=1` raises the level to debug). A gRPC interceptor tags every request
  with a request-scoped logger carrying the RPC name.
- ✅ **Domain-error → gRPC status mapping** — known sentinels become precise
  codes (`AlreadyExists`, `FailedPrecondition`, `NotFound`); anything else is
  `Internal` with a generic message so internal details aren't leaked to callers.
- ✅ **gRPC server reflection** — enabled so `grpcurl` and similar tools can
  introspect the service without the `.proto`.

### Not implemented / partial

- 🚧 **Parallel activities** — the replay model supports it (each call gets a
  distinct `ActivityIndex`), but it's only sketched, not exercised (see the
  commented block in `examples/worker/main.go`).
- ❌ **Queries, child workflows, continue-as-new** — the remaining richer
  Temporal primitives.
- ❌ **Workflow versioning / non-determinism detection.**
- ❌ **Per-activity retry policies, timeouts, heartbeats** — retries use a single
  hard-coded `DefaultRetryPolicy`.

## Development

```bash
go build ./...
go vet ./...
go test ./sdk/workflow/        # pure replay / typed-flow unit tests (no DB)
go test ./...                  # integration tests spin up Postgres via
                               # testcontainers — requires a running Docker daemon
go test -run TestName ./...    # single test

# Regenerate gRPC stubs after editing the proto
make proto                     # needs protoc on PATH; installs the plugins if missing
make proto-clean
```

## Data model (`migrations/000001_initial.up.sql`)

- `workflow_executions` — one row per run (`workflow_id` + `run_id`, status,
  timestamps).
- `events` — the append-only history; replay reads these in order.
- `tasks` — a single table backing both `workflow` and `activity` tasks
  (distinguished by `task_type`), plus the lease columns (`lease_owner`,
  `lease_expires_at`), retry counters (`attempt`, `max_attempts`), and
  `visibility_time` (used for retry backoff). A completed task is **deleted** —
  the history is the durable record, not the task row.
- `timers` — pending durable timers (`fire_at`, `fired`), scanned by the engine
  to fire `Sleep`s.
