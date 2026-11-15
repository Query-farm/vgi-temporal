// unified_run_status(status): Temporal execution status to the shared
// orchestrator vocabulary. Needs no connection, so it carries the worker's
// executable examples, and is useful on its own when comparing raw statuses
// from workflow_executions with other orchestrators.

package temporal

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	enumspb "go.temporal.io/api/enums/v1"
)

type unifiedStatusArgs struct {
	Status *array.String `vgi:"pos=0,const=false,doc=Temporal execution status such as 'Failed' or 'ContinuedAsNew' (case-insensitive; the WORKFLOW_EXECUTION_STATUS_ prefix is accepted)."`
}

// unifiedStatusFn is unified_run_status(status).
type unifiedStatusFn struct{}

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

var unifiedStatusDocs = fnDocs{
	Title: "Normalize Temporal Run Status", Category: "workflows",
	Keywords:    "temporal, execution status, normalize, unified status, failed, cancelled, cross orchestrator",
	Description: "Map a Temporal execution status (e.g. 'TimedOut') to queued/running/success/failed/cancelled",
	DocLLM: "unified_run_status(status) maps a Temporal workflow execution status to the vocabulary of the unified " +
		"schema: Running is 'running'; Completed and ContinuedAsNew are 'success'; Failed, TimedOut and Terminated " +
		"are 'failed'; Canceled is 'cancelled'. Case-insensitive, and the WORKFLOW_EXECUTION_STATUS_ enum prefix " +
		"(with or without underscores) is accepted. Unknown values and NULL give NULL. It is the mapping " +
		"temporal.unified.runs uses and needs no Temporal connection.",
	DocMD: "## unified_run_status(status)\n\nTemporal execution status to queued / running / success / failed / cancelled.\n\n" +
		"Unknown values and NULL give NULL.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT workflow_type, temporal.main.unified_run_status(status) AS status, count(*) AS runs " +
			"FROM temporal.main.workflow_executions() GROUP BY ALL ORDER BY workflow_type, status",
			Description: "Run counts per workflow type in the cross-orchestrator status vocabulary"},
	},
}

var unifiedStatusExecutable = []executableExample{
	{Description: "Normalize a few Temporal statuses",
		SQL: "SELECT s, temporal.main.unified_run_status(s) AS status " +
			"FROM (VALUES ('TimedOut'), ('ContinuedAsNew'), ('Canceled'), ('running'), ('WORKFLOW_EXECUTION_STATUS_TERMINATED'), ('bogus')) t(s) ORDER BY s",
		Expected: [][]any{{"Canceled", "cancelled"}, {"ContinuedAsNew", "success"}, {"TimedOut", "failed"},
			{"WORKFLOW_EXECUTION_STATUS_TERMINATED", "failed"}, {"bogus", nil}, {"running", "running"}}},
}

// statusByName indexes Temporal statuses by their normalized name.
var statusByName = func() map[string]enumspb.WorkflowExecutionStatus {
	m := map[string]enumspb.WorkflowExecutionStatus{}
	for v := range enumspb.WorkflowExecutionStatus_name {
		s := enumspb.WorkflowExecutionStatus(v)
		if s != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
			m[normalizeStatus(s.String())] = s
		}
	}
	return m
}()

func normalizeStatus(s string) string {
	s = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(s), "_", ""))
	return strings.TrimPrefix(s, "workflowexecutionstatus")
}

// unifiedStatusOf maps a status name ("" when unknown).
func unifiedStatusOf(s string) string {
	st, ok := statusByName[normalizeStatus(s)]
	if !ok {
		return ""
	}
	return unifiedStatus(st)
}

func (*unifiedStatusFn) Name() string { return "unified_run_status" }

func (*unifiedStatusFn) Metadata() vgi.FunctionMetadata {
	tags := objectTags(unifiedStatusDocs)
	tags["vgi.executable_examples"] = mustJSON(unifiedStatusExecutable)
	return vgi.FunctionMetadata{
		Description: unifiedStatusDocs.Description,
		Stability:   vgi.StabilityConsistent,
		Tags:        tags,
		Examples:    unifiedStatusDocs.Examples,
	}
}

func (*unifiedStatusFn) OnBindTyped(*unifiedStatusArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (*unifiedStatusFn) ProcessTyped(_ context.Context, _ *unifiedStatusArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		if u := unifiedStatusOf(vgi.GetStringValue(col, i)); u != "" {
			b.Append(u)
		} else {
			b.AppendNull()
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}
