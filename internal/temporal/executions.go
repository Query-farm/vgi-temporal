// workflow_executions and count_workflow_executions: the visibility store.

package temporal

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// saMetaKey marks a column that holds a custom search attribute; its value is
// the attribute type (Keyword, Int, ...).
const saMetaKey = "temporal_search_attribute"

var executionColumns = []arrow.Field{
	column("namespace", tString, "Namespace the execution belongs to.", false),
	column("workflow_id", tString, "Workflow id (unique per namespace among open workflows). Pushed down as WorkflowId.", false),
	column("run_id", tString, "Run id of this execution. Pushed down as RunId.", false),
	column("workflow_type", tString, "Workflow type name. Pushed down as WorkflowType.", true),
	column("status", tString, "Execution status: Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew or TimedOut. Pushed down as ExecutionStatus.", false),
	column("start_time", tTS, "When the execution was started. Range filters push down as StartTime.", true),
	column("execution_time", tTS, "When the first workflow task was scheduled (later than start_time for delayed and cron starts). Pushed down as ExecutionTime.", true),
	column("close_time", tTS, "When the execution closed; NULL while running. Pushed down as CloseTime.", true),
	column("execution_duration_seconds", tFloat64, "Seconds from execution_time to close_time; NULL while running.", true),
	column("task_queue", tString, "Task queue the workflow runs on. Pushed down as TaskQueue.", true),
	column("history_length", tInt64, "Number of events in the history. Pushed down as HistoryLength.", true),
	column("history_size_bytes", tInt64, "History size in bytes. Pushed down as HistorySizeBytes.", true),
	column("state_transition_count", tInt64, "Number of state transitions. Pushed down as StateTransitionCount.", true),
	column("parent_workflow_id", tString, "Parent workflow id for a child workflow, else NULL. Pushed down as ParentWorkflowId.", true),
	column("parent_run_id", tString, "Parent run id for a child workflow, else NULL. Pushed down as ParentRunId.", true),
	column("root_workflow_id", tString, "Workflow id at the root of the child-workflow tree. Pushed down as RootWorkflowId.", true),
	column("root_run_id", tString, "Run id at the root of the child-workflow tree. Pushed down as RootRunId.", true),
	column("first_run_id", tString, "First run id of the workflow chain (continue-as-new, retries, cron).", true),
	column("scheduled_by_id", tString, "Schedule id that started this run (TemporalScheduledById), else NULL. Pushed down.", true),
	column("memo", tJSON, "Memo fields with payloads decoded to JSON.", true),
	column("search_attributes", tJSON, "All search attributes (system and custom) with decoded values.", true),
	column("url", tString, "Link to the run in the Temporal Web UI (NULL when the UI URL is unknown; set UI_URL in the secret).", true),
}

var executionsSchema = arrow.NewSchema(executionColumns, nil)

// executionAttrs maps the fixed columns to visibility attributes.
var executionAttrs = map[string]visAttr{
	"workflow_id":            {Attr: "WorkflowId", Kind: kindKeyword},
	"run_id":                 {Attr: "RunId", Kind: kindKeyword},
	"workflow_type":          {Attr: "WorkflowType", Kind: kindKeyword},
	"status":                 {Attr: "ExecutionStatus", Kind: kindKeyword, Values: rawStatusValues},
	"start_time":             {Attr: "StartTime", Kind: kindDatetime},
	"execution_time":         {Attr: "ExecutionTime", Kind: kindDatetime},
	"close_time":             {Attr: "CloseTime", Kind: kindDatetime, Nullable: true},
	"task_queue":             {Attr: "TaskQueue", Kind: kindKeyword},
	"history_length":         {Attr: "HistoryLength", Kind: kindInt},
	"history_size_bytes":     {Attr: "HistorySizeBytes", Kind: kindInt},
	"state_transition_count": {Attr: "StateTransitionCount", Kind: kindInt},
	"parent_workflow_id":     {Attr: "ParentWorkflowId", Kind: kindKeyword, Nullable: true},
	"parent_run_id":          {Attr: "ParentRunId", Kind: kindKeyword, Nullable: true},
	"root_workflow_id":       {Attr: "RootWorkflowId", Kind: kindKeyword, Nullable: true},
	"root_run_id":            {Attr: "RootRunId", Kind: kindKeyword, Nullable: true},
	"scheduled_by_id":        {Attr: "TemporalScheduledById", Kind: kindKeyword, Nullable: true},
}

// saArrowType maps a search attribute type to a column type.
func saArrowType(t string) (arrow.DataType, attrKind, bool) {
	switch t {
	case "Keyword":
		return tString, kindKeyword, true
	case "Text":
		return tString, kindKeyword, false
	case "Int":
		return tInt64, kindInt, true
	case "Double":
		return tFloat64, kindDouble, true
	case "Bool":
		return tBool, kindBool, true
	case "Datetime":
		return tTS, kindDatetime, true
	case "KeywordList":
		return tStrList, kindKeyword, false
	}
	return nil, 0, false
}

// saTypeName turns IndexedValueType into "Keyword", "Int", ...
func saTypeName(t enumspb.IndexedValueType) string {
	return strings.TrimPrefix(t.String(), "IndexedValueType")
}

// customSearchAttributes returns the namespace's custom search attributes.
func customSearchAttributes(ctx context.Context, c *client, ns string) (map[string]string, error) {
	r, err := c.op.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: ns})
	if err == nil {
		out := map[string]string{}
		for k, v := range r.GetCustomAttributes() {
			out[k] = saTypeName(v)
		}
		return out, nil
	}
	if !isUnimplemented(err) {
		return nil, err
	}
	// Temporal Cloud without operator access: GetSearchAttributes lists every
	// attribute; treat the non-Temporal-prefixed, non-system ones as custom.
	g, gerr := c.wf.GetSearchAttributes(ctx, &workflowservice.GetSearchAttributesRequest{})
	if gerr != nil {
		return nil, err
	}
	out := map[string]string{}
	for k, v := range g.GetKeys() {
		if !systemSearchAttribute(k) {
			out[k] = saTypeName(v)
		}
	}
	return out, nil
}

// systemSearchAttributes are Temporal's built-in attributes.
var systemSearchAttributes = map[string]bool{
	"WorkflowId": true, "RunId": true, "WorkflowType": true, "StartTime": true, "ExecutionTime": true,
	"CloseTime": true, "ExecutionStatus": true, "TaskQueue": true, "HistoryLength": true, "HistorySizeBytes": true,
	"ExecutionDuration": true, "StateTransitionCount": true, "ParentWorkflowId": true, "ParentRunId": true,
	"RootWorkflowId": true, "RootRunId": true, "BinaryChecksums": true, "BatcherUser": true, "BatcherNamespace": true,
	"BuildIds": true, "NamespaceId": true,
}

func systemSearchAttribute(name string) bool {
	return systemSearchAttributes[name] || strings.HasPrefix(name, "Temporal")
}

type executionsArgs struct {
	Namespace *string `vgi:"name=namespace,doc=Namespace to list; default the secret's NAMESPACE."`
	Query     *string `vgi:"name=query,doc=Extra Temporal visibility query ANDed with the pushed-down WHERE filters, e.g. 'CustomKeywordField STARTS_WITH ''ab'''."`
	PageSize  *int64  `vgi:"name=page_size,ge=1,le=1000,doc=Executions requested per page (1 to 1000); default 100 for the first page and 1000 after."`
}

// bindExecutions adds one typed column per custom search attribute. A cluster
// that cannot be reached at bind time yields the fixed columns only.
func bindExecutions(p *vgi.BindParams) (*arrow.Schema, error) {
	var args executionsArgs
	if err := vgi.BindArgs(p.Args, &args); err != nil {
		return nil, err
	}
	c, err := connect(p.Secrets)
	if err != nil {
		return nil, err
	}
	ns := c.cfg.Namespace
	if args.Namespace != nil && *args.Namespace != "" {
		ns = *args.Namespace
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sas, err := customSearchAttributes(ctx, c, ns)
	if err != nil {
		slog.Warn("temporal: custom search attributes unavailable at bind", "err", err)
		return executionsSchema, nil
	}
	return withSearchAttributeColumns(executionColumns, sas), nil
}

// withSearchAttributeColumns appends sorted custom search attribute columns
// whose names do not collide with fixed columns.
func withSearchAttributeColumns(base []arrow.Field, sas map[string]string) *arrow.Schema {
	taken := map[string]bool{}
	for _, f := range base {
		taken[strings.ToLower(f.Name)] = true
	}
	names := make([]string, 0, len(sas))
	for n := range sas {
		names = append(names, n)
	}
	sort.Strings(names)
	fields := append([]arrow.Field{}, base...)
	for _, n := range names {
		typ, _, _ := saArrowType(sas[n])
		if typ == nil || taken[strings.ToLower(n)] {
			continue
		}
		taken[strings.ToLower(n)] = true
		fields = append(fields, arrow.Field{Name: n, Type: typ, Nullable: true, Metadata: arrow.NewMetadata(
			[]string{"comment", saMetaKey},
			[]string{"Custom search attribute " + n + " (" + sas[n] + ").", sas[n]})})
	}
	return arrow.NewSchema(fields, nil)
}

// attrsFor adds the custom search attribute columns of a bound schema.
func attrsFor(full *arrow.Schema) map[string]visAttr {
	out := make(map[string]visAttr, len(executionAttrs)+4)
	for k, v := range executionAttrs {
		out[k] = v
	}
	for _, f := range full.Fields() {
		t, ok := f.Metadata.GetValue(saMetaKey)
		if !ok {
			continue
		}
		if _, kind, push := saArrowType(t); push {
			out[f.Name] = visAttr{Attr: f.Name, Kind: kind, Nullable: true}
		}
	}
	return out
}

// saValue decodes one search attribute payload for a typed column.
func saValue(p *commonpb.Payload, typ string) any {
	v := decodePayload(p)
	if v == nil {
		return nil
	}
	switch typ {
	case "Keyword", "Text":
		if s, ok := v.(string); ok {
			return s
		}
		b, _ := json.Marshal(v)
		return string(b)
	case "Int":
		if f, ok := v.(float64); ok {
			return int64(f)
		}
	case "Double":
		if f, ok := v.(float64); ok {
			return f
		}
	case "Bool":
		if b, ok := v.(bool); ok {
			return b
		}
	case "Datetime":
		if s, ok := v.(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return t.UTC().Truncate(time.Microsecond)
			}
		}
	case "KeywordList":
		switch x := v.(type) {
		case []any:
			out := make([]any, 0, len(x))
			for _, e := range x {
				if s, ok := e.(string); ok {
					out = append(out, s)
				}
			}
			return out
		case string:
			return []any{x}
		}
	}
	return nil
}

// executionRow builds a full-width row for workflow_executions.
func executionRow(c *client, ns string, e *workflowpb.WorkflowExecutionInfo, full *arrow.Schema) []any {
	sa := e.GetSearchAttributes().GetIndexedFields()
	wid, rid := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
	var scheduledBy any
	if p, ok := sa["TemporalScheduledById"]; ok {
		if s, ok := decodePayload(p).(string); ok {
			scheduledBy = s
		}
	}
	var dur any
	if e.GetCloseTime() != nil {
		dur = secs(e.GetExecutionDuration())
	}
	row := []any{
		ns, wid, rid, nilIfEmpty(e.GetType().GetName()), e.GetStatus().String(),
		ts(e.GetStartTime()), ts(e.GetExecutionTime()), ts(e.GetCloseTime()), dur,
		nilIfEmpty(e.GetTaskQueue()), e.GetHistoryLength(), e.GetHistorySizeBytes(), e.GetStateTransitionCount(),
		nilIfEmpty(e.GetParentExecution().GetWorkflowId()), nilIfEmpty(e.GetParentExecution().GetRunId()),
		nilIfEmpty(e.GetRootExecution().GetWorkflowId()), nilIfEmpty(e.GetRootExecution().GetRunId()),
		nilIfEmpty(e.GetFirstRunId()), scheduledBy,
		jsonOrNil(payloadMap(e.GetMemo().GetFields())), jsonOrNil(payloadMap(sa)),
		c.cfg.workflowURL(ns, wid, rid),
	}
	for _, f := range full.Fields()[len(row):] {
		t, _ := f.Metadata.GetValue(saMetaKey)
		row = append(row, saValue(sa[f.Name], t))
	}
	return row
}

var executionsDef = &tableDef{
	Name:   "workflow_executions",
	Args:   executionsArgs{},
	Schema: executionsSchema,
	Bind:   bindExecutions,
	Filter: true,
	Docs: fnDocs{
		Title: "Workflow Executions", Category: "workflows",
		Keywords:    "temporal, workflow executions, list workflows, visibility query, list filter, search attributes, failed workflows",
		Description: "Workflow executions from the Temporal visibility store, with WHERE filters translated into a visibility query",
		DocLLM: "workflow_executions(namespace := NULL, query := NULL) lists workflow executions (one row per run) of a " +
			"namespace via ListWorkflowExecutions. Columns: namespace, workflow_id, run_id, workflow_type, status " +
			"(Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut - capitalized as Temporal " +
			"spells them), start_time, execution_time, close_time, execution_duration_seconds, task_queue, " +
			"history_length, history_size_bytes, state_transition_count, parent/root workflow and run ids, " +
			"first_run_id, scheduled_by_id, memo JSON, search_attributes JSON, url, then one typed column per custom " +
			"search attribute named exactly like the attribute (Keyword/Text `VARCHAR`, Int `BIGINT`, Double `DOUBLE`, " +
			"Bool `BOOLEAN`, Datetime `TIMESTAMPTZ`, KeywordList `VARCHAR[]`). WHERE filters on workflow_id, run_id, " +
			"workflow_type, status, task_queue, the time columns, the counters, parent/root ids, scheduled_by_id " +
			"and Keyword/Int/Double/Bool/Datetime search attribute columns are sent to Temporal as a visibility " +
			"query (=, !=, IN, ranges, IS NULL), so filter here rather than scanning everything. query := adds raw " +
			"Temporal List Filter syntax (e.g. STARTS_WITH). Results stream page by page.",
		DocMD: "## workflow_executions(namespace := NULL, query := NULL, page_size := NULL)\n\nOne row per workflow run. `WHERE` filters on " +
			"the documented columns become a Temporal visibility query; custom search attributes appear as typed columns.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT workflow_type, count(*) AS failures FROM temporal.main.workflow_executions() " +
				"WHERE status IN ('Failed', 'TimedOut') AND start_time >= TIMESTAMPTZ '2026-01-01' GROUP BY workflow_type ORDER BY failures DESC",
				Description: "Failed or timed-out runs per workflow type since January (filters run server-side)"},
			{SQL: "SELECT workflow_id, start_time FROM temporal.main.workflow_executions() WHERE status = 'Running' AND task_queue = 'orders' ORDER BY start_time LIMIT 20",
				Description: "The oldest open workflows on one task queue"},
		},
	},
	Init: func(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
		var args executionsArgs
		if err := vgi.BindArgs(p.Args, &args); err != nil {
			return err
		}
		namespaceArg(args.Namespace, st)
		if args.PageSize != nil {
			st.PageSize = int32(*args.PageSize)
		}
		user := ""
		if args.Query != nil {
			user = *args.Query
		}
		full := p.BindOutputSchema
		if full == nil {
			full = executionsSchema
		}
		st.Query = visibilityQuery(p.CurrentPushdownFilters, attrsFor(full), user)
		slog.Debug("temporal: visibility query", "namespace", st.Namespace, "query", st.Query)
		return nil
	},
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, full *arrow.Schema) ([][]any, error) {
		r, err := c.wf.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: st.Namespace, PageSize: pageSize(st, 100, 1000), NextPageToken: st.Token, Query: st.Query})
		if err != nil {
			return nil, err
		}
		rows := make([][]any, 0, len(r.GetExecutions()))
		for _, e := range r.GetExecutions() {
			rows = append(rows, executionRow(c, st.Namespace, e, full))
		}
		st.Step++
		st.Token = r.GetNextPageToken()
		st.Done = len(st.Token) == 0
		return rows, nil
	},
}

var countSchema = arrow.NewSchema([]arrow.Field{
	column("namespace", tString, "Namespace counted.", false),
	column("status", tString, "Execution status of the group (Running, Completed, ...); NULL when group_by_status is false.", true),
	column("count", tInt64, "Number of executions matching the query in this group.", false),
}, nil)

type countArgs struct {
	Namespace     *string `vgi:"name=namespace,doc=Namespace to count; default the secret's NAMESPACE."`
	Query         *string `vgi:"name=query,doc=Temporal visibility query selecting the executions to count, e.g. 'WorkflowType = ''Order'''; default all."`
	GroupByStatus bool    `vgi:"name=group_by_status,default=true,doc=true (default) for one row per execution status; false for one total row."`
}

var countDef = &tableDef{
	Name:   "count_workflow_executions",
	Args:   countArgs{},
	Schema: countSchema,
	Docs: fnDocs{
		Title: "Count Workflow Executions", Category: "workflows",
		Keywords:    "temporal, count workflows, workflow status, group by status, visibility, health",
		Description: "Server-side count of workflow executions, grouped by execution status",
		DocLLM: "count_workflow_executions(namespace := NULL, query := NULL, group_by_status := true) calls " +
			"CountWorkflowExecutions, which counts in the visibility store without listing executions - the cheap way " +
			"to get status totals. Returns (namespace, status, count): one row per status, or one row with status NULL " +
			"when group_by_status is false. query takes Temporal List Filter syntax, e.g. " +
			"'WorkflowType = ''OrderWorkflow'' AND StartTime > ''2026-01-01T00:00:00Z'''.",
		DocMD: "## count_workflow_executions(namespace := NULL, query := NULL, group_by_status := true)\n\n" +
			"Status totals computed by Temporal (`GROUP BY ExecutionStatus`).",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT status, count FROM temporal.main.count_workflow_executions() ORDER BY count DESC",
				Description: "How many executions are in each status"},
			{SQL: "SELECT count FROM temporal.main.count_workflow_executions(query := 'WorkflowType = ''OrderWorkflow''', group_by_status := false)",
				Description: "Total runs of one workflow type"},
		},
	},
	Init: func(_ context.Context, _ *client, p *vgi.ProcessParams, st *ScanState) error {
		var args countArgs
		if err := vgi.BindArgs(p.Args, &args); err != nil {
			return err
		}
		namespaceArg(args.Namespace, st)
		if args.Query != nil {
			st.Query = strings.TrimSpace(*args.Query)
		}
		st.GroupByStatus = args.GroupByStatus
		return nil
	},
	Page: func(ctx context.Context, c *client, _ *vgi.ProcessParams, st *ScanState, _ *arrow.Schema) ([][]any, error) {
		q := st.Query
		if st.GroupByStatus {
			q = strings.TrimSpace(q + " GROUP BY ExecutionStatus")
		}
		r, err := c.wf.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: st.Namespace, Query: q})
		st.Done = true
		if err != nil {
			return nil, err
		}
		if !st.GroupByStatus {
			return [][]any{{st.Namespace, nil, r.GetCount()}}, nil
		}
		var rows [][]any
		for _, g := range r.GetGroups() {
			var status any
			if vals := g.GetGroupValues(); len(vals) > 0 {
				if s, ok := decodePayload(vals[0]).(string); ok {
					status = s
				}
			}
			rows = append(rows, []any{st.Namespace, status, g.GetCount()})
		}
		return rows, nil
	},
}
