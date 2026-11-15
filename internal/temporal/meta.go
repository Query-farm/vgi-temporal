// Discovery metadata: catalog and schema descriptions, categories, and the
// helpers every table uses to publish its docs.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.

package temporal

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "temporal"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-temporal"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "workflows", Title: "Workflow Executions", Description: "Workflow executions from the visibility store, counts by status, event histories and pending activities."},
	{Name: "operations", Title: "Namespace Operations", Description: "Namespaces, schedules, task queues, search attributes and batch operations."},
	{Name: "unified", Title: "Unified Orchestrator Shape", Description: "runs / task_runs with the columns shared by every orchestrator worker."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// fnDocs is the discovery text of one function or table.
type fnDocs struct {
	Title       string
	Category    string
	Keywords    string
	Description string
	DocLLM      string
	DocMD       string
	Examples    []vgi.CatalogExample
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d fnDocs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return withExamples(map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}, d.Examples)
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// duckType names an Arrow type the way DuckDB shows it.
func duckType(t arrow.DataType) string {
	switch x := t.(type) {
	case *arrow.StringType:
		return "VARCHAR"
	case *arrow.Int64Type:
		return "BIGINT"
	case *arrow.Int32Type:
		return "INTEGER"
	case *arrow.Float64Type:
		return "DOUBLE"
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.TimestampType:
		return "TIMESTAMP WITH TIME ZONE"
	case *arrow.ListType:
		return duckType(x.Elem()) + "[]"
	case *arrow.StructType:
		parts := make([]string, x.NumFields())
		for i, f := range x.Fields() {
			parts[i] = f.Name + " " + duckType(f.Type)
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	case arrow.ExtensionType:
		return "JSON"
	}
	return t.String()
}

// schemaColumns lists a schema's columns for vgi.result_columns_schema.
func schemaColumns(s *arrow.Schema) []resultColumn {
	out := make([]resultColumn, s.NumFields())
	for i, f := range s.Fields() {
		c, _ := f.Metadata.GetValue("comment")
		out[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: c}
	}
	return out
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

// CatalogComment is the catalog's one-line description in directories.
const CatalogComment = "Temporal workflow executions, event histories, pending activities, schedules, task queues and " +
	"batch operations as SQL, from self-hosted Temporal or Temporal Cloud. Read-only."

const catalogDocLLM = "Read-only SQL over a Temporal cluster (self-hosted or Temporal Cloud) through its gRPC " +
	"WorkflowService. Use it to answer questions about workflow executions: which runs failed or are stuck, how " +
	"many are in each status, what happened inside one run (its event history and in-flight activities), and how " +
	"schedules, task queues and batch jobs are doing. Connection settings come from a DuckDB secret of type " +
	"temporal (HOST host:port, NAMESPACE, API_KEY for Temporal Cloud, or TLS/CA_CERT/CLIENT_CERT/CLIENT_KEY for " +
	"mTLS); with no secret it uses a local dev server at 127.0.0.1:7233, namespace default. Key concepts: a " +
	"workflow id names a workflow and each execution of it has a run id; statuses in schema main are Temporal's " +
	"own (Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut); WHERE filters on the " +
	"execution listing are translated into Temporal's visibility query so they run server-side, and custom search " +
	"attributes appear as typed columns. Schema unified holds runs and activity task runs in the column layout " +
	"shared by the Query.Farm orchestrator workers (Airflow, Dagster, Prefect, dbt, ...) for UNION ALL."

// CatalogDocMD is the catalog's long human-facing description.
const CatalogDocMD = "# Temporal\n\n" +
	"Query a [Temporal](https://temporal.io) cluster from DuckDB: workflow executions with server-side " +
	"visibility filtering, counts by status, full event histories, pending activities, schedules, task queue " +
	"pollers and backlog, search attributes and batch operations. Works with self-hosted Temporal and Temporal " +
	"Cloud (API key or mTLS). Read-only: nothing can be started, signalled or terminated.\n\n" +
	"Connect with a `temporal` secret holding `HOST`, `NAMESPACE` and either `API_KEY` (Temporal Cloud) or " +
	"`CLIENT_CERT`/`CLIENT_KEY` (mTLS). Without a secret the worker talks to a local `temporal server start-dev`.\n\n" +
	"The `unified` schema exposes runs and task runs in the shape shared by the Query.Farm orchestrator workers."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml.
var agentTestTasks = []agentTask{
	{Name: "failed_workflows", Prompt: "Using the Temporal worker, which workflow ids in the default namespace failed? List them alphabetically."},
	{Name: "status_counts", Prompt: "How many Temporal workflow executions are there in each execution status?"},
	{Name: "retried_activity", Prompt: "Which activity type in workflow order-retry-1 needed more than one attempt, and how many attempts did it take?"},
	{Name: "gold_customers", Prompt: "How many workflow executions have the custom search attribute CustomerTier set to 'gold'?"},
	{Name: "stuck_activity", Prompt: "Which activity is still pending in the running workflow order-running-1, and what was its last failure message?"},
	{Name: "unified_failures", Prompt: "In the cross-orchestrator unified shape, how many Temporal runs failed and how many succeeded?"},
	{Name: "retried_tasks", Prompt: "In the cross-orchestrator unified task runs, which Temporal activity task names took more than one attempt?"},
	{Name: "namespace_retention", Prompt: "What is the workflow retention, in days, of the Temporal namespace named default?"},
	{Name: "paused_schedules", Prompt: "Which Temporal schedules are paused, and which workflow type does each one start?"},
	{Name: "custom_search_attributes", Prompt: "Which custom search attributes are registered in the Temporal namespace, and what is the type of each?"},
	{Name: "orders_pollers", Prompt: "For the Temporal task queue named orders, list the activity and workflow sides with their approximate backlog counts."},
	{Name: "normalized_failures", Prompt: "Map each Temporal workflow execution's status to the cross-orchestrator status vocabulary and count the executions per normalized status."},
	{Name: "batch_jobs", Prompt: "Which Temporal batch operations have run, what kind of operation was each, and how many executions did each complete?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "Temporal Workflows",
	"vgi.subtitle":           "Temporal workflow executions, histories, activities, schedules and task queues as SQL.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             CatalogDocMD,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("temporal, temporal cloud, workflow, workflow orchestration, durable execution, activities, schedules, observability"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	"vgi.agent_test_tasks":   mustJSON(agentTestTasks),
}

// schemaInfo is the per-schema discovery text.
type schemaInfo struct {
	Title, Domain, Keywords, DocLLM, DocMD string
	Categories                             []string
}

var mainSchema = schemaInfo{
	Title: "Temporal", Domain: "workflow orchestration",
	Keywords: "temporal, workflow executions, visibility query, event history, activities, schedules, task queues",
	DocLLM: "Schema main reads Temporal's own objects for one namespace at a time (the secret's NAMESPACE, or a " +
		"namespace := argument). Start from the execution listing and filter it with WHERE on workflow id, run id, " +
		"workflow type, status, task queue, start/close/execution time, history length or a custom search " +
		"attribute column: those filters become a Temporal visibility query and run server-side, so a narrow " +
		"WHERE is cheap even in a namespace with millions of runs. Status values are Temporal's spelling (Running, " +
		"Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut). To explain one execution, read its " +
		"event history (one row per event, with activity, timer, child workflow and failure fields hoisted) or its " +
		"pending activities (attempt, backoff and last failure of whatever it is waiting on). Counting by status is " +
		"done server-side without listing. The remaining objects describe the namespace: schedules, task queue " +
		"pollers and backlog, search attributes and batch operations.",
	DocMD: "## main\n\nTemporal's own objects for one namespace: workflow executions (with visibility-query " +
		"pushdown and typed custom search attribute columns), server-side counts by status, event histories and " +
		"pending activities.\n\nNamespace-level operations live here too: namespaces, schedules with their spec " +
		"and recent actions, task queue pollers and backlog, search attributes and batch operations.\n\n" +
		"Statuses use Temporal's spelling; `unified_run_status` maps them to the cross-orchestrator vocabulary.",
	Categories: []string{"workflows", "operations"},
}

var unifiedSchema = schemaInfo{
	Title: "Unified Orchestrator Runs", Domain: "workflow orchestration",
	Keywords: "unified, runs, task runs, orchestrator, cross-tool, union all, temporal",
	DocLLM: "Schema unified has two tables with the exact columns the airflow, dagster, prefect, dbt, databricks " +
		"and github workers also expose, for UNION ALL across orchestrators. runs: one row per Temporal workflow " +
		"run (pipeline = workflow type). task_runs: one row per activity execution, derived from each run's event " +
		"history (one history read per workflow run), so filter run_id or join from a filtered runs query first.",
	DocMD: "## unified\n\n`runs` and `task_runs` in the column layout shared by every Query.Farm orchestrator " +
		"worker, so Temporal workflows can be reported next to Airflow DAG runs, Dagster runs or Prefect flow runs " +
		"with a plain `UNION ALL`.\n\nStatuses are normalized to queued, running, success, failed, cancelled and " +
		"skipped; the Temporal status is kept in `raw_status`. Task runs are Temporal activity executions, read " +
		"from each run's event history, so filter `run_id` before scanning them.",
	Categories: []string{"unified"},
}

// MainSchemaComment and UnifiedSchemaComment are the schema comments.
const (
	MainSchemaComment    = "Temporal workflow executions, histories, activities, schedules, task queues and batch operations."
	UnifiedSchemaComment = "runs and task_runs in the shape shared by every orchestrator worker."
)

// schemaTags builds a schema's tags.
func schemaTags(si schemaInfo, examples []exampleQuery) map[string]string {
	var cats []category
	for _, name := range si.Categories {
		for _, c := range SchemaCategories {
			if c.Name == name {
				cats = append(cats, c)
			}
		}
	}
	return map[string]string{
		"vgi.title":           si.Title,
		"domain":              si.Domain,
		"vgi.keywords":        keywordsArray(si.Keywords),
		"vgi.doc_llm":         si.DocLLM,
		"vgi.doc_md":          si.DocMD,
		"vgi.categories":      mustJSON(cats),
		"vgi.example_queries": mustJSON(examples),
	}
}
