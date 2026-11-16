<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Temporal Workflows, Histories and Schedules in DuckDB

> **vgi-temporal** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-temporal/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-temporal/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Temporal](https://temporal.io) keeps every workflow execution, its full event history, its
in-flight activities, schedules and task queues behind a gRPC API. The Web UI and `temporal`
CLI show them one at a time. This worker turns them into SQL tables in DuckDB, so you can ask
"which workflow types failed most this week", "which activities needed more than three
attempts", or "which schedules are paused" with a `GROUP BY`.

It works with **self-hosted Temporal** and **Temporal Cloud** (API key or mTLS), and it is
**read-only**: nothing can be started, signalled, reset or terminated through it.

- `WHERE` filters on workflow executions run **server-side**: they are translated into
  Temporal's visibility query (List Filter), including custom search attributes, which appear
  as typed columns.
- `unified.runs` / `unified.task_runs` have the same columns as the Airflow, Dagster, Prefect,
  dbt, Databricks and GitHub workers, so Temporal runs `UNION ALL` with the rest of your
  orchestration.

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'temporal' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-temporal@v0.1.1');

-- Temporal Cloud with an API key
CREATE SECRET (TYPE temporal, HOST 'my-ns.a1b2c.tmprl.cloud:7233', NAMESPACE 'my-ns.a1b2c', API_KEY '...');

SELECT workflow_type, count(*) AS failures
FROM temporal.main.workflow_executions()
WHERE status IN ('Failed', 'TimedOut') AND start_time >= now() - INTERVAL 7 DAY
GROUP BY ALL ORDER BY failures DESC;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-temporal:0.1.1`, then
  `ATTACH 'temporal' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **From source**: `make build` (Go 1.26+ and a C toolchain, for cgo), then
  `ATTACH 'temporal' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-temporal-worker')`.
</details>

## Secrets and configuration

All settings live in one DuckDB secret of type `temporal`. Without a secret the worker talks to
a local `temporal server start-dev` at `127.0.0.1:7233`, namespace `default`.

| Field | Meaning |
|---|---|
| `HOST` | Frontend gRPC address `host:port`. Temporal Cloud: `<namespace>.<account>.tmprl.cloud:7233` (mTLS) or `<region>.<cloud>.api.temporal.io:7233` (API key). Default `127.0.0.1:7233`. |
| `NAMESPACE` | Namespace queried by default (most functions also take `namespace :=`). Default `default`. |
| `API_KEY` | Temporal Cloud API key, sent as `Authorization: Bearer`. Redacted. Turns TLS on. |
| `TLS` | `'true'` / `'false'`. Default `false`, or `true` when `API_KEY` or `CLIENT_CERT` is set. |
| `CA_CERT` | PEM CA bundle for the server certificate (default: system roots). |
| `CLIENT_CERT`, `CLIENT_KEY` | PEM client certificate and key for mTLS. The key is redacted. |
| `SERVER_NAME` | TLS server name to verify when it differs from `HOST`. |
| `UI_URL` | Web UI base for the `url` columns, e.g. `https://cloud.temporal.io`. Derived for Temporal Cloud and local dev servers, otherwise NULL. |

```sql
-- Self-hosted with mTLS
CREATE SECRET (TYPE temporal, HOST 'temporal.internal:7233', NAMESPACE 'prod',
    CA_CERT '-----BEGIN CERTIFICATE-----...', CLIENT_CERT '-----BEGIN CERTIFICATE-----...',
    CLIENT_KEY '-----BEGIN PRIVATE KEY-----...', UI_URL 'https://temporal-ui.internal');
```

The secret is read on every call and never stored in scan state, which travels back to DuckDB
in HTTP continuation tokens. Unavailable and rate-limited (`ResourceExhausted`) responses are
retried with exponential backoff; every RPC has a 30-second deadline.

## What's inside

### `main`: Temporal's own objects

| Object | Returns |
|---|---|
| `workflow_executions(namespace := NULL, query := NULL, page_size := NULL)` | One row per workflow run: ids, type, status, start/execution/close time, task queue, history length/size, parent/root ids, schedule id, memo and search attributes (JSON), UI url, plus **one typed column per custom search attribute** (Keyword/Text `VARCHAR`, Int `BIGINT`, Double `DOUBLE`, Bool `BOOLEAN`, Datetime `TIMESTAMPTZ`, KeywordList `VARCHAR[]`). |
| `count_workflow_executions(namespace := NULL, query := NULL, group_by_status := true)` | Server-side `CountWorkflowExecutions`, one row per status. |
| `workflow_history(workflow_id, run_id := NULL, namespace := NULL, page_size := NULL)` | One row per history event: id, type, time, hoisted activity / timer / child-workflow / signal / failure fields, and the full attributes as JSON with payloads decoded. |
| `pending_activities(workflow_id, run_id := NULL, namespace := NULL)` | In-flight activities of one run: state, attempt, backoff, next attempt time, last failure, heartbeat details. |
| `namespaces` (table) | Namespaces with retention, archival and replication settings. |
| `schedules(namespace := NULL)` | Schedules: spec (cron, intervals, calendars, time zone), recent actions, next action times; action / policy / counters via `DescribeSchedule` when selected. |
| `task_queues(task_queue, namespace := NULL)` | Workflow and activity side of one task queue: pollers, backlog count and age, add/dispatch rates. |
| `search_attributes(namespace := NULL)` | System and custom search attributes with their types. |
| `batch_operations(namespace := NULL)` | Batch jobs (terminate, cancel, signal, reset, ...) with counts, reason and query. List only. |
| `unified_run_status(status)` | Scalar: Temporal status to queued/running/success/failed/cancelled. |

Statuses in `main` use Temporal's spelling: `Running`, `Completed`, `Failed`, `Canceled`,
`Terminated`, `ContinuedAsNew`, `TimedOut`.

### `unified`: the cross-orchestrator shape

| Table | Columns |
|---|---|
| `unified.runs` | orchestrator, run_id, pipeline, status, raw_status, trigger, queued_at, started_at, ended_at, duration_seconds, url |
| `unified.task_runs` | orchestrator, run_id, task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url |

- `runs`: one row per workflow run. `pipeline` is the workflow type. `Completed` and
  `ContinuedAsNew` are `success`; `Failed`, `TimedOut` and `Terminated` are `failed`; `Canceled`
  is `cancelled`. `trigger` is `schedule` (started by a Temporal schedule), `other` (a child
  workflow, or a continue-as-new / retry / cron chain) or `api`. `queued_at` is Temporal's
  StartTime and `started_at` its ExecutionTime (later for delayed and cron starts).
- `task_runs`: one row per **activity execution**, derived from each run's event history
  (`ActivityTaskScheduled` / `Started` / `Completed` / `Failed` / `TimedOut` / `Canceled`), plus
  `DescribeWorkflowExecution` for activities of runs that are still open. Temporal retries
  inside one activity execution and records only the last attempt in history, so `attempt` is
  the final (or current) attempt number. `task_run_id` is `<run_id>:<scheduled event id>`.

**`task_runs` reads one history per workflow run.** Filter `run_id` (it pushes down), or join
from a filtered `runs` query, rather than scanning a whole namespace:

```sql
SELECT t.task_name, t.attempt, t.status
FROM temporal.unified.task_runs t
WHERE t.run_id IN (SELECT run_id FROM temporal.unified.runs
                   WHERE status = 'failed' AND queued_at >= now() - INTERVAL 1 DAY);
```

## What pushes down

DuckDB removes every predicate it pushes into a table function from its own plan, so the worker
re-applies all of them exactly (natively in Go for comparisons, `IN`, `IS [NOT] NULL` and
`AND`/`OR`; anything else through vgi-go's embedded DuckDB evaluator). The visibility query only
has to narrow what Temporal returns.

| Table | Columns sent to Temporal | Operators |
|---|---|---|
| `workflow_executions` | `workflow_id`, `run_id`, `workflow_type`, `status`, `task_queue`, `parent_workflow_id`, `parent_run_id`, `root_workflow_id`, `root_run_id`, `scheduled_by_id`, Keyword search attributes | `=`, `!=`, `IN`, `IS [NOT] NULL` (nullable columns) |
| | `start_time`, `execution_time`, `close_time`, Datetime search attributes | `<`, `<=`, `>`, `>=`, `=` (as a one-microsecond range), `IS [NOT] NULL` |
| | `history_length`, `history_size_bytes`, `state_transition_count`, Int/Double search attributes | all comparisons, `IN` |
| | Bool search attributes | `=` |
| `unified.runs` | `run_id`, `pipeline`, `status` (as the set of Temporal statuses), `raw_status`, `queued_at`, `started_at`, `ended_at` | as above |
| `unified.task_runs` | `run_id` | `=`, `IN` |

An `OR` is pushed only when every branch translates; an untranslatable conjunct is simply left
out of the `AND`. String literals are single-quoted with `'` doubled. Temporal's visibility
parser does not round-trip backslashes, so a literal containing one is filtered locally only.
Text and KeywordList attributes are never pushed (their `=` means full-text match / contains).
`query :=` adds raw List Filter syntax (for example `STARTS_WITH`) ANDed with the pushed filters.

Projection pushes down too: `workflow_history` renders the attributes JSON only when it is
selected, and `schedules` calls `DescribeSchedule` only when an action/policy/counter column is.

**Pagination** uses Temporal's `next_page_token`, kept in the scan state the SDK carries in HTTP
continuation tokens, so long scans survive the HTTP transport. `workflow_executions` asks for 100
executions on the first page (cheap `LIMIT` queries) and 1000 after; `page_size :=` overrides it.

## Examples

```sql
-- Status totals without listing anything
SELECT status, count FROM temporal.main.count_workflow_executions() ORDER BY count DESC;

-- The oldest open workflows on one task queue, with their custom tier
SELECT workflow_id, start_time, CustomerTier
FROM temporal.main.workflow_executions()
WHERE status = 'Running' AND task_queue = 'orders' ORDER BY start_time LIMIT 20;

-- Why is this workflow stuck?
SELECT activity_type, state, attempt, next_attempt_schedule_time, last_failure_message
FROM temporal.main.pending_activities('order-123');

-- Every activity of a run with its final attempt and outcome
SELECT s.activity_type, st.attempt, c.event_type AS outcome
FROM temporal.main.workflow_history('order-123') s
JOIN temporal.main.workflow_history('order-123') c
  ON c.scheduled_event_id = s.event_id AND c.event_type IN ('ActivityTaskCompleted', 'ActivityTaskFailed', 'ActivityTaskTimedOut')
LEFT JOIN temporal.main.workflow_history('order-123') st
  ON st.scheduled_event_id = s.event_id AND st.event_type = 'ActivityTaskStarted'
WHERE s.event_type = 'ActivityTaskScheduled' ORDER BY s.event_id;

-- Are workers polling, and how far behind are they?
SELECT task_queue_type, poller_count, approximate_backlog_count
FROM temporal.main.task_queues('orders');

-- Temporal next to Airflow in one report
SELECT orchestrator, pipeline, count(*) FILTER (WHERE status = 'failed') AS failures
FROM (SELECT * FROM temporal.unified.runs UNION ALL SELECT * FROM airflow.unified.runs)
GROUP BY ALL ORDER BY failures DESC;
```

## How it is tested

- **Unit tests** (`make test-unit`): visibility-query translation (escaping, ranges, status sets,
  OR/AND handling), payload and search-attribute decoding, secret parsing, unified status and
  trigger mapping, nested Arrow batches.
- **SQL tests** (`test/sql/temporal.test`, through the real `vgi` extension over stdio, launch
  and HTTP) run against a real **`temporal server start-dev`**: in-memory, headless, on a free
  port, about 170 MB. `ci/temporal-fixture.sh` starts it, `test/seeder` (a separate Go module
  using the Temporal Go SDK) seeds it with a completed, a failed, a retried (3 attempts), a
  running (retrying) workflow, a parent with a child, a workflow id that needs escaping, a
  batch-terminated workflow, a paused and triggered schedule, and the custom search attributes
  `CustomerTier` (Keyword) and `OrderCount` (Int). The server is stopped by PID afterwards.
- The Temporal CLI comes from `ci/fetch-temporal-cli.sh`: version pinned (1.10.1) and checked
  against pinned SHA-256 sums, for linux and macOS. CI downloads it the same way, so CI runs the
  same live-server suite (linux only; there is no Windows lane because the fixture script is
  POSIX-only).
- **Metadata** (`make lint-explain`): vgi-lint, 100/100 with zero findings at `info`. It runs
  static (`--no-execute`) because every table needs a reachable Temporal frontend.

`make check` runs all of it. One dev server is shared by the three transports.

## Known limitations

- **TIMESTAMPTZ filters and a non-UTC `TimeZone`**: in a session with ICU loaded and `TimeZone`
  set to anything but UTC, a pushed-down filter on a `TIMESTAMPTZ` column fails with
  "comparison operands have incompatible types". vgi-go 0.42.0 compares the column's
  `timestamp[us, UTC]` with the literal's session-zone type before the worker sees the filter.
  Run `SET TimeZone = 'UTC'` or compare `start_time::TIMESTAMP`.
- `workflow_executions` adds custom search attribute columns at bind time. If the cluster cannot
  be reached at bind, the query binds with the fixed columns only (and then fails with the
  connection error).
- Payloads encrypted or encoded by a client-side codec are returned as
  `{"encoding", "data_base64"}`; the worker has no access to your codec.
- `task_runs` covers activities only (not child workflows or Nexus operations), one row per
  activity execution rather than per attempt.
- `task_queues` needs the queue name; stats (backlog, rates) are NULL on servers that do not
  report them. Nexus task queues are not described.
- Temporal Cloud API keys scoped to one namespace cannot call `ListNamespaces`; `namespaces`
  then returns the secret's namespace via `DescribeNamespace`. Search attributes fall back to
  `GetSearchAttributes` where the OperatorService is not permitted.

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Temporal and Temporal Cloud are
trademarks of Temporal Technologies, Inc.; this worker is not affiliated with or endorsed by
them. It talks to Temporal through the published `go.temporal.io/api` protobuf definitions.
