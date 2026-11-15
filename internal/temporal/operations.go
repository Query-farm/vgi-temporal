// Namespace-level operations tables: namespaces, schedules, task_queues,
// search_attributes and batch_operations.

package temporal

import (
	"context"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

type namespaceOnlyArgs struct {
	Namespace *string `vgi:"name=namespace,doc=Namespace; default the secret's NAMESPACE."`
}

func initNamespaceOnly(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
	var args namespaceOnlyArgs
	if err := vgi.BindArgs(p.Args, &args); err != nil {
		return err
	}
	namespaceArg(args.Namespace, st)
	return nil
}

// ---------------------------------------------------------------- namespaces

var namespacesSchema = arrow.NewSchema([]arrow.Field{
	column("name", tString, "Namespace name.", false),
	column("id", tString, "Namespace id (UUID).", true),
	column("state", tString, "Registered, Deprecated or Deleted.", true),
	column("description", tString, "Namespace description.", true),
	column("owner_email", tString, "Contact email of the team that owns the namespace.", true),
	column("retention_days", tFloat64, "Workflow execution retention after close, in days.", true),
	column("history_archival_state", tString, "Enabled or Disabled.", true),
	column("visibility_archival_state", tString, "Enabled or Disabled.", true),
	column("is_global", tBool, "Whether the namespace is replicated across clusters.", true),
	column("active_cluster", tString, "Active cluster of a replicated namespace.", true),
	column("supports_schedules", tBool, "Whether the namespace supports schedules.", true),
	column("data", tJSON, "Free-form namespace data (key/value map).", true),
}, nil)

func namespaceRow(info *namespacepb.NamespaceInfo, cfg *namespacepb.NamespaceConfig, global bool, active string) []any {
	var retention any
	if r := cfg.GetWorkflowExecutionRetentionTtl(); r != nil {
		retention = r.AsDuration().Hours() / 24
	}
	data := map[string]any{}
	for k, v := range info.GetData() {
		data[k] = v
	}
	return []any{info.GetName(), nilIfEmpty(info.GetId()), info.GetState().String(), nilIfEmpty(info.GetDescription()),
		nilIfEmpty(info.GetOwnerEmail()), retention, cfg.GetHistoryArchivalState().String(), cfg.GetVisibilityArchivalState().String(),
		global, nilIfEmpty(active), info.GetSupportsSchedules(), jsonOrNil(data)}
}

var namespacesDef = &tableDef{
	Name:   "namespaces",
	Schema: namespacesSchema,
	Docs: fnDocs{
		Title: "Temporal Namespaces", Category: "operations",
		Keywords:    "temporal, namespaces, retention, archival, multi-cluster, temporal cloud",
		Description: "Namespaces of the cluster with retention, archival and replication settings",
		DocLLM: "The namespaces table lists the cluster's namespaces via ListNamespaces: name, id, state, description, " +
			"owner_email, retention_days, history/visibility archival state, is_global, active_cluster, " +
			"supports_schedules and data (JSON). On Temporal Cloud, where an API key is scoped to one namespace and " +
			"ListNamespaces is not allowed, it returns the secret's namespace via DescribeNamespace.",
		DocMD: "## namespaces\n\nNamespaces with their retention and archival settings.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT name, retention_days, state FROM temporal.main.namespaces ORDER BY name",
				Description: "Every namespace with its retention"},
		},
	},
	Init: func(context.Context, *client, *vgi.ProcessParams, *ScanState) error { return nil },
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		r, err := c.wf.ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{PageSize: 100, NextPageToken: st.Token})
		if err != nil {
			if !isUnimplemented(err) || st.Step > 0 {
				return nil, err
			}
			st.Done = true
			d, derr := c.wf.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: st.Namespace})
			if derr != nil {
				return nil, err
			}
			return [][]any{namespaceRow(d.GetNamespaceInfo(), d.GetConfig(), d.GetIsGlobalNamespace(), d.GetReplicationConfig().GetActiveClusterName())}, nil
		}
		st.Step++
		st.Token = r.GetNextPageToken()
		st.Done = len(st.Token) == 0
		var rows [][]any
		for _, n := range r.GetNamespaces() {
			rows = append(rows, namespaceRow(n.GetNamespaceInfo(), n.GetConfig(), n.GetIsGlobalNamespace(), n.GetReplicationConfig().GetActiveClusterName()))
		}
		return rows, nil
	},
}

// ---------------------------------------------------------------- schedules

var (
	tInterval = arrow.StructOf(
		arrow.Field{Name: "every_seconds", Type: tFloat64, Nullable: true},
		arrow.Field{Name: "offset_seconds", Type: tFloat64, Nullable: true})
	tAction = arrow.StructOf(
		arrow.Field{Name: "schedule_time", Type: tTS, Nullable: true},
		arrow.Field{Name: "actual_time", Type: tTS, Nullable: true},
		arrow.Field{Name: "workflow_id", Type: tString, Nullable: true},
		arrow.Field{Name: "run_id", Type: tString, Nullable: true},
		arrow.Field{Name: "status", Type: tString, Nullable: true})
)

var schedulesSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace of the schedule.", false),
	column("schedule_id", tString, "Schedule id.", false),
	column("workflow_type", tString, "Workflow type the schedule starts.", true),
	column("paused", tBool, "Whether the schedule is paused.", true),
	column("notes", tString, "Notes recorded with the last pause/unpause.", true),
	column("cron_expressions", tStrList, "Cron strings of the spec as written, when the server keeps them (most servers store them as calendars).", true),
	column("intervals", arrow.ListOf(tInterval), "Interval specs: every_seconds and offset_seconds.", true),
	column("calendars", tJSON, "Structured calendar specs (seconds, minutes, hours, days of month/week, months, years as ranges), including those compiled from cron strings.", true),
	column("timezone", tString, "IANA time zone the calendars are evaluated in; NULL for UTC.", true),
	column("spec", tJSON, "The full schedule spec as JSON.", true),
	column("recent_actions", arrow.ListOf(tAction), "Most recent actions: schedule_time, actual_time, started workflow_id/run_id and its status.", true),
	column("next_action_times", tTSList, "Upcoming action times.", true),
	column("action_workflow_id", tString, "Workflow id prefix of started workflows (the action's workflow id; the start time is appended).", true),
	column("action_task_queue", tString, "Task queue of started workflows.", true),
	column("overlap_policy", tString, "What happens when a run is still open at the next action (Skip, BufferOne, AllowAll, ...).", true),
	column("catchup_window_seconds", tFloat64, "How far back missed actions are caught up, in seconds.", true),
	column("action_count", tInt64, "Number of actions taken so far.", true),
	column("missed_catchup_window", tInt64, "Actions skipped because they were outside the catchup window.", true),
	column("overlap_skipped", tInt64, "Actions skipped by the overlap policy.", true),
	column("running_workflow_ids", tStrList, "Workflow ids started by the schedule that are still running.", true),
	column("create_time", tTS, "When the schedule was created.", true),
	column("update_time", tTS, "When the schedule was last updated.", true),
	column("memo", tJSON, "Schedule memo, decoded.", true),
	column("search_attributes", tJSON, "Schedule search attributes, decoded.", true),
}, nil)

// describeColumns are filled from DescribeSchedule (one call per schedule).
var describeColumns = []string{"action_workflow_id", "action_task_queue", "overlap_policy", "catchup_window_seconds", "action_count",
	"missed_catchup_window", "overlap_skipped", "running_workflow_ids", "create_time", "update_time", "memo"}

func specColumns(spec *schedulepb.ScheduleSpec) (cron, intervals, calendars, tz, full any) {
	if spec == nil {
		return nil, nil, nil, nil, nil
	}
	var cr []any
	for _, s := range spec.GetCronString() {
		cr = append(cr, s)
	}
	cron = cr
	if cr == nil {
		cron = []any{}
	}
	iv := []any{}
	for _, i := range spec.GetInterval() {
		iv = append(iv, structVal{secs(i.GetInterval()), secs(i.GetPhase())})
	}
	intervals = iv
	if len(spec.GetStructuredCalendar()) > 0 {
		calendars = protoToJSON(&schedulepb.ScheduleSpec{StructuredCalendar: spec.GetStructuredCalendar()})
		if m, ok := calendars.(jsonText); ok {
			calendars = jsonText(trimWrapper(string(m), "structuredCalendar"))
		}
	}
	return cron, intervals, calendars, nilIfEmpty(spec.GetTimezoneName()), protoToJSON(spec)
}

// trimWrapper unwraps {"key": X} to X (falls back to the input).
func trimWrapper(s, key string) string {
	prefix := `{"` + key + `":`
	if len(s) > len(prefix)+1 && s[:len(prefix)] == prefix && s[len(s)-1] == '}' {
		return s[len(prefix) : len(s)-1]
	}
	return s
}

func actionsList(as []*schedulepb.ScheduleActionResult) any {
	out := []any{}
	for _, a := range as {
		var status any
		if a.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			status = a.GetStartWorkflowStatus().String()
		}
		out = append(out, structVal{ts(a.GetScheduleTime()), ts(a.GetActualTime()),
			nilIfEmpty(a.GetStartWorkflowResult().GetWorkflowId()), nilIfEmpty(a.GetStartWorkflowResult().GetRunId()), status})
	}
	return out
}

var schedulesDef = &tableDef{
	Name:   "schedules",
	Args:   namespaceOnlyArgs{},
	Schema: schedulesSchema,
	Docs: fnDocs{
		Title: "Schedules", Category: "operations",
		Keywords:    "temporal, schedules, cron, intervals, recent actions, next run, paused schedules",
		Description: "Schedules of a namespace with their spec, recent actions, upcoming times and policies",
		DocLLM: "schedules(namespace := NULL) lists schedules via ListSchedules: schedule_id, workflow_type, paused, " +
			"notes, cron_expressions, intervals (every_seconds, offset_seconds), calendars and spec (JSON), timezone, " +
			"recent_actions (list of structs with schedule_time, actual_time, workflow_id, run_id, status) and " +
			"next_action_times. The action/policy/info columns (action_workflow_id, action_task_queue, " +
			"overlap_policy, catchup_window_seconds, action_count, missed_catchup_window, overlap_skipped, " +
			"running_workflow_ids, create_time, update_time, memo) need one DescribeSchedule call per schedule and are " +
			"fetched only when selected. Use UNNEST(recent_actions) to get one row per action.",
		DocMD: "## schedules(namespace := NULL)\n\nSchedules with spec, recent actions and upcoming action times.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT schedule_id, workflow_type, paused, next_action_times[1] AS next_run FROM temporal.main.schedules() ORDER BY schedule_id",
				Description: "Every schedule with its next run"},
			{SQL: "SELECT schedule_id, a.workflow_id, a.status FROM temporal.main.schedules(), UNNEST(recent_actions) AS r(a) ORDER BY a.actual_time DESC",
				Description: "Recent runs started by schedules"},
		},
	},
	Init: initNamespaceOnly,
	Page: func(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		r, err := c.wf.ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: st.Namespace, MaximumPageSize: 100, NextPageToken: st.Token})
		if err != nil {
			return nil, err
		}
		st.Step++
		st.Token = r.GetNextPageToken()
		st.Done = len(st.Token) == 0
		describe := false
		for _, n := range describeColumns {
			describe = describe || projected(p, n)
		}
		var rows [][]any
		for _, e := range r.GetSchedules() {
			info := e.GetInfo()
			spec := info.GetSpec()
			var d *workflowservice.DescribeScheduleResponse
			if describe {
				d, err = c.wf.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{Namespace: st.Namespace, ScheduleId: e.GetScheduleId()})
				if err != nil {
					return nil, err
				}
				if s := d.GetSchedule().GetSpec(); s != nil {
					spec = s
				}
			}
			cron, intervals, cals, tz, specJSON := specColumns(spec)
			next := []any{}
			for _, t := range info.GetFutureActionTimes() {
				next = append(next, ts(t))
			}
			row := []any{st.Namespace, e.GetScheduleId(), nilIfEmpty(info.GetWorkflowType().GetName()), info.GetPaused(), nilIfEmpty(info.GetNotes()),
				cron, intervals, cals, tz, specJSON, actionsList(info.GetRecentActions()), next,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				jsonOrNil(payloadMap(e.GetSearchAttributes().GetIndexedFields()))}
			if d != nil {
				sw := d.GetSchedule().GetAction().GetStartWorkflow()
				pol := d.GetSchedule().GetPolicies()
				di := d.GetInfo()
				running := []any{}
				for _, w := range di.GetRunningWorkflows() {
					running = append(running, w.GetWorkflowId())
				}
				var overlap any
				if pol.GetOverlapPolicy() != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
					overlap = pol.GetOverlapPolicy().String()
				}
				copy(row[12:23], []any{nilIfEmpty(sw.GetWorkflowId()), nilIfEmpty(sw.GetTaskQueue().GetName()), overlap,
					secs(pol.GetCatchupWindow()), di.GetActionCount(), di.GetMissedCatchupWindow(), di.GetOverlapSkipped(),
					running, ts(di.GetCreateTime()), ts(di.GetUpdateTime()), jsonOrNil(payloadMap(d.GetMemo().GetFields()))})
				if len(di.GetRecentActions()) > 0 {
					row[10] = actionsList(di.GetRecentActions())
				}
				if row[3] == false && d.GetSchedule().GetState().GetPaused() {
					row[3] = true
				}
				if notes := d.GetSchedule().GetState().GetNotes(); notes != "" {
					row[4] = notes
				}
			}
			rows = append(rows, row)
		}
		return rows, nil
	},
}

// ---------------------------------------------------------------- task_queues

var tPoller = arrow.StructOf(
	arrow.Field{Name: "identity", Type: tString, Nullable: true},
	arrow.Field{Name: "last_access_time", Type: tTS, Nullable: true},
	arrow.Field{Name: "rate_per_second", Type: tFloat64, Nullable: true})

var taskQueuesSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace of the task queue.", false),
	column("task_queue", tString, "Task queue name.", false),
	column("task_queue_type", tString, "Workflow, Activity or Nexus.", false),
	column("poller_count", tInt32, "Number of workers that polled recently.", false),
	column("pollers", arrow.ListOf(tPoller), "Recent pollers: worker identity, last access time and rate limit.", true),
	column("approximate_backlog_count", tInt64, "Approximate number of tasks waiting (NULL when the server does not report stats).", true),
	column("approximate_backlog_age_seconds", tFloat64, "Age of the oldest waiting task, in seconds (NULL when not reported).", true),
	column("tasks_add_rate", tFloat64, "Tasks added per second (NULL when not reported).", true),
	column("tasks_dispatch_rate", tFloat64, "Tasks dispatched per second (NULL when not reported).", true),
}, nil)

type taskQueueArgs struct {
	TaskQueue string  `vgi:"pos=0,doc=Task queue name, e.g. the TaskQueue of a workflow execution."`
	Namespace *string `vgi:"name=namespace,doc=Namespace; default the secret's NAMESPACE."`
}

var taskQueueTypes = []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY}

var taskQueuesDef = &tableDef{
	Name:   "task_queues",
	Args:   taskQueueArgs{},
	Schema: taskQueuesSchema,
	Docs: fnDocs{
		Title: "Task Queue Pollers and Backlog", Category: "operations",
		Keywords:    "temporal, task queue, pollers, workers, backlog, dispatch rate, worker health",
		Description: "Pollers and backlog of one task queue, one row for its workflow and one for its activity side",
		DocLLM: "task_queues(task_queue, namespace := NULL) calls DescribeTaskQueue for the workflow and the activity " +
			"side of one task queue and returns task_queue_type, poller_count, pollers (identity, last_access_time, " +
			"rate_per_second), approximate_backlog_count, approximate_backlog_age_seconds, tasks_add_rate and " +
			"tasks_dispatch_rate. Stats are NULL on servers that do not report them. poller_count = 0 means no worker " +
			"is serving that side of the queue. Task queue names come from workflow_executions.task_queue.",
		DocMD: "## task_queues(task_queue, namespace := NULL)\n\nAre workers polling, and how far behind are they?",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT task_queue_type, poller_count, approximate_backlog_count FROM temporal.main.task_queues('orders') ORDER BY task_queue_type",
				Description: "Workers and backlog of one task queue"},
		},
	},
	Init: func(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
		var args taskQueueArgs
		if err := vgi.BindArgs(p.Args, &args); err != nil {
			return err
		}
		namespaceArg(args.Namespace, st)
		st.TaskQueue = args.TaskQueue
		return nil
	},
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		st.Done = true
		var rows [][]any
		for _, typ := range taskQueueTypes {
			r, err := c.wf.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{Namespace: st.Namespace,
				TaskQueue:     &taskqueuepb.TaskQueue{Name: st.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				TaskQueueType: typ, ReportStats: true})
			if err != nil {
				return nil, err
			}
			pollers := []any{}
			for _, pl := range r.GetPollers() {
				pollers = append(pollers, structVal{nilIfEmpty(pl.GetIdentity()), ts(pl.GetLastAccessTime()), pl.GetRatePerSecond()})
			}
			var backlog, age, addRate, dispatch any
			if s := r.GetStats(); s != nil {
				backlog, age = s.GetApproximateBacklogCount(), secs(s.GetApproximateBacklogAge())
				addRate, dispatch = float64(s.GetTasksAddRate()), float64(s.GetTasksDispatchRate())
			}
			rows = append(rows, []any{st.Namespace, st.TaskQueue, typ.String(), int64(len(r.GetPollers())), pollers, backlog, age, addRate, dispatch})
		}
		return rows, nil
	},
}

// ---------------------------------------------------------------- search_attributes

var searchAttributesSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace the attributes are listed for.", false),
	column("name", tString, "Search attribute name, as used in visibility queries.", false),
	column("type", tString, "Keyword, Text, Int, Double, Bool, Datetime or KeywordList.", false),
	column("is_custom", tBool, "true for attributes registered by users; false for Temporal's system attributes.", false),
}, nil)

var searchAttributesDef = &tableDef{
	Name:   "search_attributes",
	Args:   namespaceOnlyArgs{},
	Schema: searchAttributesSchema,
	Docs: fnDocs{
		Title: "Search Attributes", Category: "operations",
		Keywords:    "temporal, search attributes, custom search attributes, visibility, indexed fields",
		Description: "System and custom search attributes of a namespace with their types",
		DocLLM: "search_attributes(namespace := NULL) lists every search attribute (name, type, is_custom) via the " +
			"OperatorService (falling back to GetSearchAttributes on servers that restrict it). Custom attributes also " +
			"appear as typed columns of workflow_executions, where filters on them push down.",
		DocMD: "## search_attributes(namespace := NULL)\n\nThe attributes usable in visibility queries.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT name, type FROM temporal.main.search_attributes() WHERE is_custom ORDER BY name",
				Description: "Custom search attributes of the namespace"},
		},
	},
	Init: initNamespaceOnly,
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		st.Done = true
		all := map[string]string{}
		custom := map[string]bool{}
		r, err := c.op.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: st.Namespace})
		if err == nil {
			for k, v := range r.GetSystemAttributes() {
				all[k] = saTypeName(v)
			}
			for k, v := range r.GetCustomAttributes() {
				all[k], custom[k] = saTypeName(v), true
			}
		} else {
			if !isUnimplemented(err) {
				return nil, err
			}
			g, gerr := c.wf.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
			if gerr != nil {
				return nil, err
			}
			for k, v := range g.GetKeys() {
				all[k], custom[k] = saTypeName(v), !systemSearchAttribute(k)
			}
		}
		names := make([]string, 0, len(all))
		for k := range all {
			names = append(names, k)
		}
		sort.Strings(names)
		rows := make([][]any, 0, len(names))
		for _, n := range names {
			rows = append(rows, []any{st.Namespace, n, all[n], custom[n]})
		}
		return rows, nil
	},
}

// ---------------------------------------------------------------- batch_operations

var batchSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace of the batch operation.", false),
	column("job_id", tString, "Batch job id.", false),
	column("operation_type", tString, "Terminate, Cancel, Signal, Delete, Reset, UpdateExecutionOptions, ...", true),
	column("state", tString, "Running, Completed or Failed.", true),
	column("start_time", tTS, "When the batch started.", true),
	column("close_time", tTS, "When the batch finished.", true),
	column("total_operation_count", tInt64, "Executions the batch targets.", true),
	column("complete_operation_count", tInt64, "Executions processed successfully.", true),
	column("failure_operation_count", tInt64, "Executions that failed.", true),
	column("identity", tString, "Who started the batch.", true),
	column("reason", tString, "Reason given when the batch was started.", true),
	column("query", tString, "Visibility query that selected the executions.", true),
}, nil)

var batchDef = &tableDef{
	Name:   "batch_operations",
	Args:   namespaceOnlyArgs{},
	Schema: batchSchema,
	Docs: fnDocs{
		Title: "Batch Operations", Category: "operations",
		Keywords:    "temporal, batch operations, bulk terminate, bulk cancel, bulk signal, audit",
		Description: "Batch operations (bulk terminate, cancel, signal, reset ...) of a namespace, read-only",
		DocLLM: "batch_operations(namespace := NULL) lists batch jobs via ListBatchOperations and describes each " +
			"(DescribeBatchOperation): job_id, operation_type, state, start_time, close_time, total / complete / " +
			"failure operation counts, identity, reason and the visibility query it targeted. Read-only: the worker " +
			"cannot start or stop batches.",
		DocMD: "## batch_operations(namespace := NULL)\n\nAn audit trail of bulk actions on workflows.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT job_id, operation_type, state, complete_operation_count, reason FROM temporal.main.batch_operations() ORDER BY start_time DESC",
				Description: "Recent bulk actions and why they ran"},
		},
	},
	Init: initNamespaceOnly,
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		r, err := c.wf.ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{Namespace: st.Namespace, PageSize: 100, NextPageToken: st.Token})
		if err != nil {
			return nil, err
		}
		st.Step++
		st.Token = r.GetNextPageToken()
		st.Done = len(st.Token) == 0
		var rows [][]any
		for _, b := range r.GetOperationInfo() {
			d, err := c.wf.DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{Namespace: st.Namespace, JobId: b.GetJobId()})
			if err != nil {
				return nil, err
			}
			rows = append(rows, []any{st.Namespace, b.GetJobId(), d.GetOperationType().String(), b.GetState().String(),
				ts(b.GetStartTime()), ts(b.GetCloseTime()), d.GetTotalOperationCount(), d.GetCompleteOperationCount(),
				d.GetFailureOperationCount(), nilIfEmpty(d.GetIdentity()), nilIfEmpty(d.GetReason()), nilIfEmpty(d.GetQuery())})
		}
		return rows, nil
	},
}
