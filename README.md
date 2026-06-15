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
  - `internal/frontend` — gRPC server bootstrap (listens on `:7233`).
  - `internal/frontend/grpc` — `EngineService` RPC handlers; a thin translation
    layer between protobuf and the internal types.
  - `internal/history` — the business logic of durable execution; owns all
    multi-statement transactions (start workflow, turn worker commands into
    events + tasks).
  - `internal/persistence` — raw `pgx` data access over the `events`, `tasks`,
    and `workflow_executions` tables.
- **SDK** (`sdk/`) — `Client` (gRPC wrapper), `Worker` (poll loops + replay
  driver), and `sdk/workflow` (the workflow-author API: `Context`,
  `ExecuteActivity`, `ActivityFuture`).
- There is **no in-process scheduling** — all coordination happens through the
  `tasks` table.

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

## Quickstart

A full end-to-end run needs **three processes**: engine + worker + api.

```bash
# 1. Start Postgres (schema is auto-migrated on engine boot)
docker compose up -d

# 2. Run the engine (gRPC on :7233; runs ./migrations first)
make grun            # == go run ./cmd/engine

# 3. Run the example worker (connects to localhost:7233)
go run ./examples/worker

# 4. Run the example API (HTTP on :3000)
go run ./examples/api

# 5. Kick off a workflow
curl -XPOST localhost:3000/test -d '{"OrderID":1,"Items":["pizza"]}'
```

The example is a toy order pipeline that runs three activities sequentially,
passing typed results down the chain:
`CheckStock → ChargeCard → Ship` (see `examples/worker/main.go`).

The engine reads `DATABASE_URL` (defaults to
`postgres://postgres:password@localhost:5432/myengine?sslmode=disable`, matching
`docker-compose.yml`).

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

## Features

### Implemented

- ✅ **Durable event-sourced execution** — workflows persisted as event
  histories in Postgres and replayed deterministically.
- ✅ **Sequential activity execution** with typed, JSON-encoded data flowing
  from one activity into the next.
- ✅ **gRPC engine API** — `StartWorkflow`, `PollWorkflowTask`,
  `RespondWorkflowTaskCompleted`, `PollActivityTask`,
  `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`.
- ✅ **Concurrent task dispatch** — `FOR UPDATE SKIP LOCKED` + a partial index
  (`lease_owner IS NULL`) lets multiple workers poll the shared `tasks` table
  without handing out the same task twice.
- ✅ **Activity failure** wired end to end — a panic, returned `error`, or
  marshal error reports `RespondActivityTaskFailed`, appends an `ActivityFailed`
  event, and surfaces as an `error` to the workflow on replay.
- ✅ **Task leasing + crash recovery** — `PollTask` leases a task to the polling
  worker (`lease_owner` = worker UUID, `lease_expires_at = now() + 30s`) in the
  same transaction. A background loop in the engine reclaims leases that expire
  (worker crashed or stalled), making the task pollable again.
- ✅ **Auto-migration** — the engine runs SQL migrations from `./migrations` on
  boot.

### Not implemented / partial

- ❌ **Activity retry** — a *failed* activity is not automatically rescheduled
  (distinct from lease reclamation, which only re-queues tasks abandoned by a
  dead worker).
- 🚧 **Parallel activities** — the replay model supports it (each call gets a
  distinct `ActivityIndex`), but it's only sketched, not exercised (see the
  commented block in `examples/worker/main.go`).
- ❌ **Timers / `workflow.Sleep`, signals, queries, child workflows,
  continue-as-new** — none of the richer Temporal primitives.
- ❌ **Workflow versioning / non-determinism detection.**
- ❌ **Configurable retry policies, timeouts, heartbeats.**
- ⚠️ Only `sdk/workflow` currently has unit tests (`go test ./sdk/workflow/`).

## Development

```bash
go build ./...
go vet ./...
go test ./...                 # sdk/workflow has the replay / typed-flow tests
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
  (distinguished by `task_type`), plus the lease columns. A completed task is
  **deleted** — the history is the durable record, not the task row.
