// workflow_history and pending_activities: one workflow execution in detail.

package temporal

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var historySchema = arrow.NewSchema([]arrow.Field{
	column("workflow_id", tString, "Workflow id whose history this is.", false),
	column("run_id", tString, "Run id whose history this is (the latest run when run_id was not given).", false),
	column("event_id", tInt64, "Event id, 1-based and increasing through the history.", false),
	column("event_type", tString, "Event type without prefix, e.g. WorkflowExecutionStarted, ActivityTaskScheduled, TimerFired.", false),
	column("event_time", tTS, "When the event was recorded.", true),
	column("task_id", tInt64, "Internal task id of the event.", true),
	column("activity_id", tString, "Activity id (ActivityTaskScheduled and, via scheduled_event_id, its later events: only set on Scheduled events).", true),
	column("activity_type", tString, "Activity type name (ActivityTaskScheduled events).", true),
	column("scheduled_event_id", tInt64, "Event id of the matching *Scheduled / *Initiated event, for started/completed/failed/timed-out events.", true),
	column("started_event_id", tInt64, "Event id of the matching *Started event, for completed/failed/timed-out events.", true),
	column("attempt", tInt32, "Attempt number (ActivityTaskStarted, WorkflowTaskStarted, WorkflowExecutionStarted).", true),
	column("timer_id", tString, "Timer id (TimerStarted, TimerFired, TimerCanceled).", true),
	column("child_workflow_id", tString, "Child workflow id (StartChildWorkflowExecutionInitiated and ChildWorkflowExecution* events).", true),
	column("child_workflow_type", tString, "Child workflow type (child workflow events).", true),
	column("child_run_id", tString, "Child run id (ChildWorkflowExecutionStarted and later child events).", true),
	column("signal_name", tString, "Signal name (WorkflowExecutionSignaled, SignalExternalWorkflowExecutionInitiated).", true),
	column("task_queue", tString, "Task queue named by the event (workflow start, activity scheduling).", true),
	column("identity", tString, "Identity of the worker or client that caused the event.", true),
	column("failure_message", tString, "Failure message of a failed / timed-out event (activity, workflow task, child or workflow failure).", true),
	column("attributes", tJSON, "The event's full attributes as JSON (payloads decoded), as in Temporal's JSON history export.", true),
}, nil)

type historyArgs struct {
	WorkflowID string  `vgi:"pos=0,doc=Workflow id whose event history to read."`
	RunID      *string `vgi:"name=run_id,doc=Run id; default the latest run of the workflow id."`
	Namespace  *string `vgi:"name=namespace,doc=Namespace; default the secret's NAMESPACE."`
}

type historyPageArgs struct {
	WorkflowID string  `vgi:"pos=0,doc=Workflow id whose event history to read."`
	RunID      *string `vgi:"name=run_id,doc=Run id; default the latest run of the workflow id."`
	Namespace  *string `vgi:"name=namespace,doc=Namespace; default the secret's NAMESPACE."`
	PageSize   *int64  `vgi:"name=page_size,ge=1,le=1000,doc=Events requested per page (1 to 1000); default 1000."`
}

// resolveRun fills in the latest run id when none was given.
func resolveRun(ctx context.Context, c *client, st *ScanState) error {
	if st.RunID != "" {
		return nil
	}
	d, err := c.wf.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: st.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: st.WorkflowID}})
	if err != nil {
		return err
	}
	st.RunID = d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	return nil
}

func initExecutionArgs(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState) error {
	var args historyArgs
	if err := vgi.BindArgs(p.Args, &args); err != nil {
		return err
	}
	return startExecution(ctx, c, st, args.WorkflowID, args.RunID, args.Namespace)
}

func startExecution(ctx context.Context, c *client, st *ScanState, workflowID string, runID, ns *string) error {
	namespaceArg(ns, st)
	st.WorkflowID = workflowID
	if runID != nil {
		st.RunID = *runID
	}
	return resolveRun(ctx, c, st)
}

// field returns a message field by proto name, or an invalid Value.
func field(m protoreflect.Message, name string) (protoreflect.Value, bool) {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || !m.Has(fd) {
		return protoreflect.Value{}, false
	}
	return m.Get(fd), true
}

func fieldString(m protoreflect.Message, name string) any {
	v, ok := field(m, name)
	if !ok {
		return nil
	}
	return nilIfEmpty(v.String())
}

func fieldInt(m protoreflect.Message, name string) any {
	v, ok := field(m, name)
	if !ok {
		return nil
	}
	return v.Int()
}

// nestedString reads msg.<outer>.<inner> as a string.
func nestedString(m protoreflect.Message, outer, inner string) any {
	v, ok := field(m, outer)
	if !ok {
		return nil
	}
	return fieldString(v.Message(), inner)
}

// eventAttributes returns the populated attributes message of an event.
func eventAttributes(ev *historypb.HistoryEvent) protoreflect.Message {
	m := ev.ProtoReflect()
	od := m.Descriptor().Oneofs().ByName("attributes")
	if od == nil {
		return nil
	}
	fd := m.WhichOneof(od)
	if fd == nil {
		return nil
	}
	return m.Get(fd).Message()
}

// historyRow builds a full-width workflow_history row; the JSON attributes
// are rendered only when projected.
func historyRow(st *ScanState, ev *historypb.HistoryEvent, wantJSON bool) []any {
	et := ev.GetEventType().String()
	row := []any{st.WorkflowID, st.RunID, ev.GetEventId(), et, ts(ev.GetEventTime()), ev.GetTaskId(),
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
	a := eventAttributes(ev)
	if a == nil {
		return row
	}
	row[6] = fieldString(a, "activity_id")
	row[7] = nestedString(a, "activity_type", "name")
	row[8] = fieldInt(a, "scheduled_event_id")
	if row[8] == nil {
		row[8] = fieldInt(a, "initiated_event_id")
	}
	row[9] = fieldInt(a, "started_event_id")
	if v := fieldInt(a, "attempt"); v != nil {
		row[10] = v
	}
	row[11] = fieldString(a, "timer_id")
	if strings.Contains(et, "ChildWorkflow") {
		row[12] = fieldString(a, "workflow_id")
		if row[12] == nil {
			row[12] = nestedString(a, "workflow_execution", "workflow_id")
		}
		row[13] = nestedString(a, "workflow_type", "name")
		row[14] = nestedString(a, "workflow_execution", "run_id")
	}
	row[15] = fieldString(a, "signal_name")
	row[16] = nestedString(a, "task_queue", "name")
	row[17] = fieldString(a, "identity")
	row[18] = nestedString(a, "failure", "message")
	if wantJSON {
		row[19] = eventAttributesJSON(ev)
	}
	return row
}

func projected(p *vgi.ProcessParams, name string) bool {
	if p.OutputSchema == nil {
		return true
	}
	return len(p.OutputSchema.FieldIndices(name)) > 0
}

// historyPage reads one page of a run's history.
func historyPage(ctx context.Context, c *client, st *ScanState, size int32) ([]*historypb.HistoryEvent, error) {
	r, err := c.wf.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: st.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: st.WorkflowID, RunId: st.RunID},
		MaximumPageSize: size, NextPageToken: st.Token})
	if err != nil {
		return nil, err
	}
	st.Token = r.GetNextPageToken()
	return r.GetHistory().GetEvents(), nil
}

var historyDef = &tableDef{
	Name:   "workflow_history",
	Args:   historyPageArgs{},
	Schema: historySchema,
	Docs: fnDocs{
		Title: "Workflow Event History", Category: "workflows",
		Keywords:    "temporal, event history, workflow history, activities, timers, child workflows, signals, debugging",
		Description: "The event history of one workflow run, one row per event",
		DocLLM: "workflow_history(workflow_id, run_id := NULL, namespace := NULL) returns every history event of one " +
			"workflow run (the latest run when run_id is omitted) via GetWorkflowExecutionHistory, paged. Columns: " +
			"workflow_id, run_id, event_id, event_type (e.g. WorkflowExecutionStarted, ActivityTaskScheduled, " +
			"ActivityTaskStarted, ActivityTaskCompleted, ActivityTaskFailed, TimerFired, " +
			"ChildWorkflowExecutionCompleted, WorkflowExecutionSignaled), event_time, task_id, then hoisted fields: " +
			"activity_id and activity_type (on Scheduled events), scheduled_event_id / started_event_id (link " +
			"later activity/child events back to their Scheduled event), attempt (ActivityTaskStarted carries the " +
			"final attempt number), timer_id, child_workflow_id/type/run_id, signal_name, task_queue, identity, " +
			"failure_message, and attributes (full JSON with payloads decoded). Join completed/failed events to " +
			"their Scheduled event on scheduled_event_id = event_id to get the activity type.",
		DocMD: "## workflow_history(workflow_id, run_id := NULL, namespace := NULL)\n\nOne row per history event, " +
			"with activity, timer, child-workflow and failure fields hoisted into columns and the full attributes as JSON.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT event_id, event_type, event_time, failure_message FROM temporal.main.workflow_history('order-123') ORDER BY event_id",
				Description: "Walk the history of the latest run of one workflow"},
			{SQL: "SELECT s.activity_type, st.attempt, c.event_type AS outcome FROM temporal.main.workflow_history('order-123') s " +
				"JOIN temporal.main.workflow_history('order-123') c ON c.scheduled_event_id = s.event_id AND c.event_type LIKE 'ActivityTask%' AND c.event_type <> 'ActivityTaskStarted' " +
				"LEFT JOIN temporal.main.workflow_history('order-123') st ON st.scheduled_event_id = s.event_id AND st.event_type = 'ActivityTaskStarted' " +
				"WHERE s.event_type = 'ActivityTaskScheduled' ORDER BY s.event_id",
				Description: "Each activity of a run with its final attempt and outcome"},
		},
	},
	Init: func(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState) error {
		var args historyPageArgs
		if err := vgi.BindArgs(p.Args, &args); err != nil {
			return err
		}
		if args.PageSize != nil {
			st.PageSize = int32(*args.PageSize)
		}
		return startExecution(ctx, c, st, args.WorkflowID, args.RunID, args.Namespace)
	},
	Page: func(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		events, err := historyPage(ctx, c, st, pageSize(st, 1000, 1000))
		if err != nil {
			return nil, err
		}
		st.Step++
		st.Done = len(st.Token) == 0
		wantJSON := projected(p, "attributes")
		rows := make([][]any, 0, len(events))
		for _, ev := range events {
			rows = append(rows, historyRow(st, ev, wantJSON))
		}
		return rows, nil
	},
}

var pendingSchema = arrow.NewSchema([]arrow.Field{
	column("workflow_id", tString, "Workflow id.", false),
	column("run_id", tString, "Run id (the latest run when run_id was not given).", false),
	column("activity_id", tString, "Activity id.", false),
	column("activity_type", tString, "Activity type name.", true),
	column("state", tString, "Scheduled (waiting for a worker or a retry), Started, CancelRequested, Paused or PauseRequested.", false),
	column("attempt", tInt32, "Current attempt number (1 = first try).", true),
	column("maximum_attempts", tInt32, "Retry policy maximum attempts; 0 means unlimited.", true),
	column("scheduled_time", tTS, "When the current attempt was scheduled.", true),
	column("last_started_time", tTS, "When the last attempt started.", true),
	column("last_heartbeat_time", tTS, "Last heartbeat from the worker.", true),
	column("expiration_time", tTS, "When the activity gives up (schedule-to-close timeout).", true),
	column("next_attempt_schedule_time", tTS, "When the next retry is scheduled, while waiting between attempts.", true),
	column("current_retry_interval_seconds", tFloat64, "Current backoff between attempts, in seconds.", true),
	column("last_failure_message", tString, "Message of the last failed attempt.", true),
	column("last_failure_type", tString, "Application error type of the last failure (e.g. CardDeclined), when set.", true),
	column("last_worker_identity", tString, "Identity of the worker that ran the last attempt.", true),
	column("paused", tBool, "Whether the activity is paused.", true),
	column("heartbeat_details", tJSON, "Last heartbeat details, decoded.", true),
}, nil)

var pendingDef = &tableDef{
	Name:   "pending_activities",
	Args:   historyArgs{},
	Schema: pendingSchema,
	Docs: fnDocs{
		Title: "Pending Activities", Category: "workflows",
		Keywords:    "temporal, pending activities, stuck workflow, retries, heartbeat, last failure, describe workflow",
		Description: "In-flight activities of one workflow run, with attempt, backoff and last failure",
		DocLLM: "pending_activities(workflow_id, run_id := NULL, namespace := NULL) calls DescribeWorkflowExecution " +
			"and returns one row per pending (scheduled, started or retrying) activity of the run: activity_id, " +
			"activity_type, state, attempt, maximum_attempts, scheduled/started/heartbeat/expiration times, " +
			"next_attempt_schedule_time, current_retry_interval_seconds, last_failure_message, last_failure_type, " +
			"last_worker_identity, paused and heartbeat_details. Use it to see why a Running workflow is stuck: an " +
			"activity with a high attempt and a last_failure_message is retrying. Empty for closed workflows.",
		DocMD: "## pending_activities(workflow_id, run_id := NULL, namespace := NULL)\n\nWhat a running workflow is waiting on.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT activity_type, state, attempt, last_failure_message FROM temporal.main.pending_activities('order-123') ORDER BY attempt DESC",
				Description: "Why is this workflow stuck?"},
		},
	},
	Init: initExecutionArgs,
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		st.Done = true
		d, err := c.wf.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: st.Namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: st.WorkflowID, RunId: st.RunID}})
		if err != nil {
			return nil, err
		}
		var rows [][]any
		for _, a := range d.GetPendingActivities() {
			var detail any
			if v := decodePayloads(a.GetHeartbeatDetails()); v != nil {
				detail = v
			}
			rows = append(rows, []any{st.WorkflowID, st.RunID, a.GetActivityId(), nilIfEmpty(a.GetActivityType().GetName()),
				a.GetState().String(), int64(a.GetAttempt()), int64(a.GetMaximumAttempts()),
				ts(a.GetScheduledTime()), ts(a.GetLastStartedTime()), ts(a.GetLastHeartbeatTime()), ts(a.GetExpirationTime()),
				ts(a.GetNextAttemptScheduleTime()), secs(a.GetCurrentRetryInterval()),
				nilIfEmpty(a.GetLastFailure().GetMessage()), nilIfEmpty(a.GetLastFailure().GetApplicationFailureInfo().GetType()),
				nilIfEmpty(a.GetLastWorkerIdentity()), a.GetPaused(), detail})
		}
		return rows, nil
	},
}
