// unified.runs / unified.task_runs: the cross-orchestrator shape shared by
// the airflow, dagster, prefect, temporal, databricks, dbt and github
// workers, so runs can be UNION ALLed across tools.
//
//   - runs: one row per workflow run of the secret's namespace. Filters on
//     run_id, pipeline, status, raw_status, queued_at, started_at and
//     ended_at push down into the visibility query.
//   - task_runs: one row per activity execution, derived from each run's
//     event history (ActivityTaskScheduled / Started / Completed / Failed /
//     TimedOut / Canceled) plus DescribeWorkflowExecution for runs still open.
//     That is one history read per workflow run, so filter run_id (pushed
//     down) or join from a filtered runs query.

package temporal

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

var unifiedRunsSchema = arrow.NewSchema([]arrow.Field{
	column("orchestrator", tString, "Always 'temporal'.", false),
	column("run_id", tString, "Temporal run id (unique per execution).", false),
	column("pipeline", tString, "Workflow type name.", true),
	column("status", tString, "queued, running, success, failed, cancelled or skipped (Completed and ContinuedAsNew are success; Failed, TimedOut and Terminated are failed).", false),
	column("raw_status", tString, "Temporal execution status: Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew or TimedOut.", true),
	column("trigger", tString, "schedule (started by a Temporal schedule), other (child workflow or continue-as-new/retry/cron chain), or api (started by a client).", true),
	column("queued_at", tTS, "When the execution was created (StartTime).", true),
	column("started_at", tTS, "When its first workflow task was scheduled (ExecutionTime; later than queued_at for delayed and cron starts).", true),
	column("ended_at", tTS, "When the execution closed; NULL while running.", true),
	column("duration_seconds", tFloat64, "Seconds from started_at to ended_at; NULL while running.", true),
	column("url", tString, "Link to the run in the Temporal Web UI (NULL when unknown; set UI_URL in the secret).", true),
}, nil)

var unifiedTaskRunsSchema = arrow.NewSchema([]arrow.Field{
	column("orchestrator", tString, "Always 'temporal'.", false),
	column("run_id", tString, "Run id of the workflow execution the activity belongs to.", false),
	column("task_run_id", tString, "'<run_id>:<event id of ActivityTaskScheduled>', unique per activity execution.", false),
	column("task_name", tString, "Activity type name.", true),
	column("status", tString, "queued, running, success, failed or cancelled.", false),
	column("raw_status", tString, "Completed, Failed, TimedOut or Canceled for finished activities; the pending state (Scheduled, Started, CancelRequested, Paused) for open ones; Unfinished when the workflow closed first.", true),
	column("attempt", tInt32, "Attempt number of the last (or current) try; Temporal retries inside one activity execution.", true),
	column("started_at", tTS, "When the last attempt started.", true),
	column("ended_at", tTS, "When the activity completed, failed, timed out or was canceled.", true),
	column("duration_seconds", tFloat64, "Seconds from started_at to ended_at (the last attempt).", true),
	column("url", tString, "Link to the workflow run in the Temporal Web UI.", true),
}, nil)

// unifiedStatus maps a Temporal status to the shared value set.
func unifiedStatus(s enumspb.WorkflowExecutionStatus) string {
	switch s {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "success"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "cancelled"
	}
	return "failed"
}

// unifiedStatusValues maps a unified status literal to Temporal statuses.
func unifiedStatusValues(s string) ([]string, bool) {
	switch s {
	case "running":
		return []string{"Running"}, true
	case "success":
		return []string{"Completed", "ContinuedAsNew"}, true
	case "failed":
		return []string{"Failed", "TimedOut", "Terminated"}, true
	case "cancelled":
		return []string{"Canceled"}, true
	}
	return nil, false
}

var unifiedRunsAttrs = map[string]visAttr{
	"run_id":     {Attr: "RunId", Kind: kindKeyword},
	"pipeline":   {Attr: "WorkflowType", Kind: kindKeyword},
	"status":     {Attr: "ExecutionStatus", Kind: kindKeyword, Values: unifiedStatusValues},
	"raw_status": {Attr: "ExecutionStatus", Kind: kindKeyword, Values: rawStatusValues},
	"queued_at":  {Attr: "StartTime", Kind: kindDatetime},
	"started_at": {Attr: "ExecutionTime", Kind: kindDatetime},
	"ended_at":   {Attr: "CloseTime", Kind: kindDatetime, Nullable: true},
}

// runTrigger classifies how a run was started.
func runTrigger(e *workflowpb.WorkflowExecutionInfo) string {
	if _, ok := e.GetSearchAttributes().GetIndexedFields()["TemporalScheduledById"]; ok {
		return "schedule"
	}
	if e.GetParentExecution() != nil || (e.GetFirstRunId() != "" && e.GetFirstRunId() != e.GetExecution().GetRunId()) {
		return "other"
	}
	return "api"
}

func unifiedRunRow(c *client, ns string, e *workflowpb.WorkflowExecutionInfo) []any {
	wid, rid := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
	started := ts(e.GetExecutionTime())
	if started == nil {
		started = ts(e.GetStartTime())
	}
	ended := ts(e.GetCloseTime())
	var dur any
	if ended != nil {
		if dur = secs(e.GetExecutionDuration()); dur == nil {
			dur = durationBetween(started, ended)
		}
	}
	return []any{"temporal", rid, nilIfEmpty(e.GetType().GetName()), unifiedStatus(e.GetStatus()), e.GetStatus().String(),
		runTrigger(e), ts(e.GetStartTime()), started, ended, dur, c.cfg.workflowURL(ns, wid, rid)}
}

var unifiedRunsDef = &tableDef{
	Name:   "runs",
	Schema: unifiedRunsSchema,
	Filter: true,
	Docs: fnDocs{
		Title: "Unified Runs", Category: "unified",
		Keywords:    "unified, runs, orchestrator, cross-tool, union all, temporal, workflow runs",
		Description: "Temporal workflow runs in the cross-orchestrator runs shape",
		DocLLM: "unified.runs has columns orchestrator ('temporal'), run_id, pipeline (workflow type), status " +
			"(queued/running/success/failed/cancelled/skipped), raw_status (Temporal status), trigger (schedule, other, " +
			"api), queued_at, started_at, ended_at, duration_seconds, url - the same columns as the airflow, dagster, " +
			"prefect and dbt workers' unified.runs, for UNION ALL. One row per workflow run of the secret's namespace. " +
			"Filters on run_id, pipeline, status, raw_status, queued_at, started_at and ended_at run server-side.",
		DocMD: "## unified.runs\n\nWorkflow runs in the shared orchestrator shape: `UNION ALL` it with another " +
			"orchestrator worker's `unified.runs`.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT pipeline, count(*) FILTER (WHERE status = 'failed') AS failures, count(*) AS runs FROM temporal.unified.runs " +
				"WHERE queued_at >= TIMESTAMPTZ '2026-01-01' GROUP BY pipeline ORDER BY failures DESC",
				Description: "Failure counts per workflow type in the shared shape"},
		},
	},
	Init: func(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
		st.Query = visibilityQuery(p.CurrentPushdownFilters, unifiedRunsAttrs, "")
		slog.Debug("temporal: visibility query", "table", "unified.runs", "query", st.Query)
		return nil
	},
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		r, err := c.wf.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: st.Namespace, PageSize: pageSize(st, 100, 1000), NextPageToken: st.Token, Query: st.Query})
		if err != nil {
			return nil, err
		}
		st.Step++
		st.Token = r.GetNextPageToken()
		st.Done = len(st.Token) == 0
		rows := make([][]any, 0, len(r.GetExecutions()))
		for _, e := range r.GetExecutions() {
			rows = append(rows, unifiedRunRow(c, st.Namespace, e))
		}
		return rows, nil
	},
}

// activityRun is one activity execution assembled from history events.
type activityRun struct {
	ScheduledID int64
	ActivityID  string
	Type        string
	Started     any
	Ended       any
	Attempt     any
	Raw         string
	Status      string
}

// activityRuns derives activity executions from a run's history.
func activityRuns(ctx context.Context, c *client, ns, workflowID, runID string, running bool) ([]*activityRun, error) {
	st := &ScanState{Namespace: ns, WorkflowID: workflowID, RunID: runID}
	byID := map[int64]*activityRun{}
	var order []*activityRun
	for {
		events, err := historyPage(ctx, c, st, 1000)
		if err != nil {
			return nil, err
		}
		for _, ev := range events {
			switch ev.GetEventType() {
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
				a := ev.GetActivityTaskScheduledEventAttributes()
				r := &activityRun{ScheduledID: ev.GetEventId(), ActivityID: a.GetActivityId(), Type: a.GetActivityType().GetName()}
				byID[ev.GetEventId()] = r
				order = append(order, r)
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
				a := ev.GetActivityTaskStartedEventAttributes()
				if r := byID[a.GetScheduledEventId()]; r != nil {
					r.Started, r.Attempt = ts(ev.GetEventTime()), int64(a.GetAttempt())
				}
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
				closeActivity(byID[ev.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()], ev.GetEventTime().AsTime(), "Completed", "success")
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
				closeActivity(byID[ev.GetActivityTaskFailedEventAttributes().GetScheduledEventId()], ev.GetEventTime().AsTime(), "Failed", "failed")
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
				closeActivity(byID[ev.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()], ev.GetEventTime().AsTime(), "TimedOut", "failed")
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
				closeActivity(byID[ev.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()], ev.GetEventTime().AsTime(), "Canceled", "cancelled")
			}
		}
		if len(st.Token) == 0 {
			break
		}
	}
	var open []*activityRun
	for _, r := range order {
		if r.Status == "" {
			open = append(open, r)
		}
	}
	if len(open) == 0 {
		return order, nil
	}
	pending := map[string]*workflowpb.PendingActivityInfo{}
	if running {
		d, err := c.wf.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns, Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID}})
		if err != nil {
			return nil, err
		}
		for _, p := range d.GetPendingActivities() {
			pending[p.GetActivityId()] = p
		}
	}
	for _, r := range open {
		p, ok := pending[r.ActivityID]
		if !ok {
			r.Raw, r.Status = "Unfinished", "cancelled"
			if running {
				r.Raw, r.Status = "Scheduled", "queued"
			}
			continue
		}
		r.Raw = p.GetState().String()
		r.Attempt = int64(p.GetAttempt())
		if v := ts(p.GetLastStartedTime()); v != nil {
			r.Started = v
		}
		switch p.GetState() {
		case enumspb.PENDING_ACTIVITY_STATE_STARTED, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
			r.Status = "running"
		default:
			r.Status = "queued"
		}
	}
	return order, nil
}

func closeActivity(r *activityRun, at time.Time, raw, status string) {
	if r == nil {
		return
	}
	r.Ended = at.UTC().Truncate(time.Microsecond)
	r.Raw, r.Status = raw, status
}

var unifiedTaskRunsDef = &tableDef{
	Name:   "task_runs",
	Schema: unifiedTaskRunsSchema,
	Filter: true,
	Docs: fnDocs{
		Title: "Unified Task Runs", Category: "unified",
		Keywords:    "unified, task runs, activities, orchestrator, cross-tool, union all, temporal, retries",
		Description: "Temporal activity executions in the cross-orchestrator task_runs shape, derived from event histories",
		DocLLM: "unified.task_runs has columns orchestrator ('temporal'), run_id, task_run_id, task_name (activity " +
			"type), status (queued/running/success/failed/cancelled), raw_status, attempt, started_at, ended_at, " +
			"duration_seconds, url - the same columns as the other orchestrator workers' unified.task_runs. Each row is " +
			"one activity execution; attempt is the final (or current) attempt, since Temporal retries inside one " +
			"execution. Rows are derived from each workflow run's event history - one history read per run - so " +
			"always filter run_id (pushed down) or join from a filtered unified.runs query; an unfiltered scan reads " +
			"every history in the namespace.",
		DocMD: "## unified.task_runs\n\nActivity executions in the shared orchestrator shape. Costs one history " +
			"read per workflow run: filter `run_id` first.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT t.task_name, t.attempt, t.status FROM temporal.unified.task_runs t " +
				"WHERE t.run_id IN (SELECT run_id FROM temporal.unified.runs WHERE status = 'failed' AND queued_at >= TIMESTAMPTZ '2026-01-01') ORDER BY t.attempt DESC",
				Description: "Activities of recent failed runs, most-retried first"},
		},
	},
	Init: func(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
		st.Query = visibilityQuery(p.CurrentPushdownFilters, map[string]visAttr{"run_id": {Attr: "RunId", Kind: kindKeyword}}, "")
		slog.Debug("temporal: visibility query", "table", "unified.task_runs", "query", st.Query)
		return nil
	},
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		if len(st.Runs) == 0 {
			if st.ListDone {
				st.Done = true
				return nil, nil
			}
			r, err := c.wf.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: st.Namespace, PageSize: 100, NextPageToken: st.Token, Query: st.Query})
			if err != nil {
				return nil, err
			}
			st.Token = r.GetNextPageToken()
			st.ListDone = len(st.Token) == 0
			for _, e := range r.GetExecutions() {
				st.Runs = append(st.Runs, RunRef{WorkflowID: e.GetExecution().GetWorkflowId(), RunID: e.GetExecution().GetRunId(),
					Running: e.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING})
			}
			if len(st.Runs) == 0 {
				st.Done = st.ListDone
				return nil, nil
			}
		}
		run := st.Runs[0]
		st.Runs = st.Runs[1:]
		acts, err := activityRuns(ctx, c, st.Namespace, run.WorkflowID, run.RunID, run.Running)
		if err != nil {
			return nil, err
		}
		url := c.cfg.workflowURL(st.Namespace, run.WorkflowID, run.RunID)
		rows := make([][]any, 0, len(acts))
		for _, a := range acts {
			rows = append(rows, []any{"temporal", run.RunID, run.RunID + ":" + strconv.FormatInt(a.ScheduledID, 10),
				nilIfEmpty(a.Type), a.Status, a.Raw, a.Attempt, a.Started, a.Ended, durationBetween(a.Started, a.Ended), url})
		}
		st.Done = st.ListDone && len(st.Runs) == 0
		return rows, nil
	},
}

// catalogTable registers a tableDef as a catalog table keyed by key, with
// optional foreign keys (column -> referenced unified table's primary key).
func catalogTable(schema string, def *tableDef, key string, fks map[string]string) vgi.CatalogTable {
	comments := map[string]string{}
	for _, f := range def.Schema.Fields() {
		c, _ := f.Metadata.GetValue("comment")
		comments[f.Name] = c
	}
	tags := objectTags(def.Docs)
	tags["domain"] = "workflow orchestration"
	cons := func(id string, kind vgi.ConstraintKind, col string, ref *vgi.ConstraintReference) []byte {
		rec, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{SchemaPath: []string{schema}, Table: def.Name, ID: id,
			Kind: kind, Columns: [][]string{{col}}, Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
			References: ref})
		if err != nil {
			panic(err)
		}
		return rec
	}
	constraints := [][]byte{cons(key+"_not_null", vgi.ConstraintNotNull, key, nil), cons("pk", vgi.ConstraintPrimaryKey, key, nil)}
	for col, table := range fks {
		// Every run a task_runs row names was just listed from the same
		// visibility store that unified.runs reads.
		constraints = append(constraints, cons(col+"_fk", vgi.ConstraintForeignKey, col,
			&vgi.ConstraintReference{SchemaPath: []string{schema}, Table: table, Columns: [][]string{{col}}, ConstraintID: "pk"}))
	}
	return vgi.CatalogTable{
		Constraints:    constraints,
		Name:           def.Name,
		Comment:        def.Docs.Description,
		Columns:        def.Schema,
		Function:       newTable(def),
		ColumnComments: comments,
		Tags:           tags,
	}
}
