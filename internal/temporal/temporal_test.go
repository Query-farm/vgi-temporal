package temporal

import (
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
)

func cmp(col string, op vgi.ComparisonOp, v any) pred {
	return pred{Kind: "cmp", Col: col, Op: op, Vals: []any{v}}
}

func TestVisibilityQueryTranslation(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	attrs := attrsFor(withSearchAttributeColumns(executionColumns, map[string]string{
		"CustomerTier": "Keyword", "OrderCount": "Int", "Notes": "Text", "Tags": "KeywordList", "Weird Name": "Keyword"}))
	cases := []struct {
		name  string
		preds []pred
		user  string
		want  string
	}{
		{"keyword eq", []pred{cmp("workflow_id", vgi.OpEQ, "order-1")}, "", "WorkflowId = 'order-1'"},
		{"quote doubled", []pred{cmp("workflow_id", vgi.OpEQ, "o'brien")}, "", "WorkflowId = 'o''brien'"},
		{"backslash not pushed", []pred{cmp("workflow_id", vgi.OpEQ, `a\b`)}, "", ""},
		{"status raw", []pred{cmp("status", vgi.OpEQ, "Failed")}, "", "ExecutionStatus = 'Failed'"},
		{"status unknown not pushed", []pred{cmp("status", vgi.OpEQ, "failed")}, "", ""},
		{"in", []pred{{Kind: "in", Col: "workflow_type", Vals: []any{"A", nil, "B"}}}, "", "WorkflowType IN ('A', 'B')"},
		{"range gt", []pred{cmp("start_time", vgi.OpGE, ts)}, "", "StartTime >= '2026-01-02T03:04:05.123456Z'"},
		{"range le widened", []pred{cmp("close_time", vgi.OpLE, ts)}, "", "CloseTime < '2026-01-02T03:04:05.123457Z'"},
		{"timestamp eq", []pred{cmp("execution_time", vgi.OpEQ, ts)}, "",
			"(ExecutionTime >= '2026-01-02T03:04:05.123456Z' AND ExecutionTime < '2026-01-02T03:04:05.123457Z')"},
		{"timestamp ne not pushed", []pred{cmp("start_time", vgi.OpNE, ts)}, "", ""},
		{"int", []pred{cmp("history_length", vgi.OpGT, int64(10))}, "", "HistoryLength > 10"},
		{"int ne", []pred{cmp("OrderCount", vgi.OpNE, int64(2))}, "", "OrderCount != 2"},
		{"custom keyword", []pred{cmp("CustomerTier", vgi.OpEQ, "gold")}, "", "CustomerTier = 'gold'"},
		{"quoted attr", []pred{cmp("Weird Name", vgi.OpEQ, "x")}, "", "`Weird Name` = 'x'"},
		{"text not pushed", []pred{cmp("Notes", vgi.OpEQ, "x")}, "", ""},
		{"keyword list not pushed", []pred{cmp("Tags", vgi.OpEQ, "x")}, "", ""},
		{"is null", []pred{{Kind: "isnull", Col: "close_time"}}, "", "CloseTime IS NULL"},
		{"is null on required col", []pred{{Kind: "isnull", Col: "workflow_id"}}, "", ""},
		{"unknown column", []pred{cmp("memo", vgi.OpEQ, "x")}, "", ""},
		{"and drops untranslatable", []pred{{Kind: "and", Kids: []pred{cmp("workflow_id", vgi.OpEQ, "a"), {Kind: "opaque"}}}}, "", "(WorkflowId = 'a')"},
		{"or needs all", []pred{{Kind: "or", Kids: []pred{cmp("workflow_id", vgi.OpEQ, "a"), {Kind: "opaque"}}}}, "", ""},
		{"or", []pred{{Kind: "or", Kids: []pred{cmp("workflow_id", vgi.OpEQ, "a"), cmp("status", vgi.OpEQ, "Running")}}}, "",
			"(WorkflowId = 'a' OR ExecutionStatus = 'Running')"},
		{"user query and filters", []pred{cmp("task_queue", vgi.OpEQ, "q")}, "WorkflowId STARTS_WITH 'x'",
			"(WorkflowId STARTS_WITH 'x') AND TaskQueue = 'q'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildQuery(tc.preds, attrs, tc.user); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestUnifiedStatusPushdown(t *testing.T) {
	got := buildQuery([]pred{cmp("status", vgi.OpEQ, "failed")}, unifiedRunsAttrs, "")
	if got != "ExecutionStatus IN ('Failed', 'TimedOut', 'Terminated')" {
		t.Fatal(got)
	}
	got = buildQuery([]pred{cmp("status", vgi.OpNE, "success")}, unifiedRunsAttrs, "")
	if got != "ExecutionStatus NOT IN ('Completed', 'ContinuedAsNew')" {
		t.Fatal(got)
	}
	if got := buildQuery([]pred{cmp("status", vgi.OpEQ, "queued")}, unifiedRunsAttrs, ""); got != "" {
		t.Fatal(got)
	}
	for s, want := range map[enumspb.WorkflowExecutionStatus]string{
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:          "running",
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:        "success",
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW: "success",
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:           "failed",
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:        "failed",
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:       "failed",
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:         "cancelled",
	} {
		if got := unifiedStatus(s); got != want {
			t.Errorf("%v: got %s want %s", s, got, want)
		}
	}
}

func TestRunTrigger(t *testing.T) {
	sched := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"nightly"`)}
	cases := map[string]*workflowpb.WorkflowExecutionInfo{
		"schedule": {SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"TemporalScheduledById": sched}}},
		"other":    {ParentExecution: &commonpb.WorkflowExecution{WorkflowId: "p"}},
		"api":      {Execution: &commonpb.WorkflowExecution{RunId: "r1"}, FirstRunId: "r1"},
	}
	for want, e := range cases {
		if got := runTrigger(e); got != want {
			t.Errorf("got %s want %s", got, want)
		}
	}
}

func TestPayloadDecoding(t *testing.T) {
	js := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`{"a":1}`)}
	if m, ok := decodePayload(js).(map[string]any); !ok || m["a"] != 1.0 {
		t.Fatalf("json/plain: %#v", decodePayload(js))
	}
	if decodePayload(&commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/null")}}) != nil {
		t.Fatal("binary/null should be nil")
	}
	enc := decodePayload(&commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/encrypted")}, Data: []byte{1, 2}})
	if m, ok := enc.(map[string]any); !ok || m["encoding"] != "binary/encrypted" || m["data_base64"] != "AQI=" {
		t.Fatalf("encrypted: %#v", enc)
	}
	ev := &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{js}}, ScheduledEventId: 5}}}
	got := string(eventAttributesJSON(ev).(jsonText))
	if !strings.Contains(got, `"result":[{"a":1}]`) || !strings.Contains(got, `"scheduledEventId":"5"`) {
		t.Fatalf("attributes JSON: %s", got)
	}
}

func TestSearchAttributeValues(t *testing.T) {
	p := func(s string) *commonpb.Payload {
		return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(s)}
	}
	if v := saValue(p(`5`), "Int"); v != int64(5) {
		t.Fatalf("Int: %#v", v)
	}
	if v := saValue(p(`"2026-01-02T03:04:05.1234567Z"`), "Datetime"); !v.(time.Time).Equal(time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)) {
		t.Fatalf("Datetime: %#v", v)
	}
	if v := saValue(p(`["a","b"]`), "KeywordList").([]any); len(v) != 2 {
		t.Fatalf("KeywordList: %#v", v)
	}
	if saValue(nil, "Keyword") != nil {
		t.Fatal("missing attribute should be NULL")
	}
	s := withSearchAttributeColumns(executionColumns, map[string]string{"WORKFLOW_ID": "Keyword", "B": "Bool", "A": "Double"})
	names := []string{}
	for _, f := range s.Fields()[len(executionColumns):] {
		names = append(names, f.Name)
	}
	if strings.Join(names, ",") != "A,B" {
		t.Fatalf("custom columns: %v (collisions must be skipped, order sorted)", names)
	}
}

func TestConfigFromSecret(t *testing.T) {
	c, err := configFrom(vgi.Secrets{})
	if err != nil || c.Host != DefaultHost || c.Namespace != "default" || c.TLS || c.UIURL != "http://localhost:8233" {
		t.Fatalf("defaults: %+v %v", c, err)
	}
	sec := func(kv ...string) vgi.Secrets {
		m := map[string]any{"type": SecretType}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return vgi.Secrets{"t": m}
	}
	c, err = configFrom(sec("host", "ns.acct.tmprl.cloud:7233", "namespace", "ns.acct", "api_key", "k"))
	if err != nil || !c.TLS || c.UIURL != "https://cloud.temporal.io" || c.Namespace != "ns.acct" {
		t.Fatalf("cloud: %+v %v", c, err)
	}
	if u := c.workflowURL("ns.acct", "a/b", "r"); u != "https://cloud.temporal.io/namespaces/ns.acct/workflows/a%2Fb/r/history" {
		t.Fatalf("url: %v", u)
	}
	if _, err := configFrom(sec("host", "nohost")); err == nil {
		t.Fatal("host without port must fail")
	}
	if _, err := configFrom(sec("client_cert", "x")); err == nil {
		t.Fatal("cert without key must fail")
	}
	if _, err := configFrom(sec("tls", "maybe")); err == nil {
		t.Fatal("bad TLS value must fail")
	}
	c, _ = configFrom(sec("host", "temporal.internal:7233", "api_key", "k", "tls", "false"))
	if c.TLS || c.UIURL != "" {
		t.Fatalf("explicit tls=false: %+v", c)
	}
}

func TestBuildBatchNested(t *testing.T) {
	s := arrow.NewSchema([]arrow.Field{
		{Name: "l", Type: arrow.ListOf(tInterval), Nullable: true},
		{Name: "j", Type: tJSON, Nullable: true},
		{Name: "t", Type: tTS, Nullable: true},
	}, nil)
	rows := [][]any{
		{[]any{structVal{1.0, nil}}, map[string]any{"x": 1}, time.Unix(1, 0)},
		{nil, nil, nil},
	}
	b, err := buildBatch(s, s, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if b.NumRows() != 2 || b.Column(0).IsValid(1) {
		t.Fatalf("batch: %v", b)
	}
	if _, err := buildBatch(s, s, [][]any{{"not a list", nil, nil}}); err == nil {
		t.Fatal("type mismatch must fail")
	}
}

func TestDuckTypeAndTrim(t *testing.T) {
	if got := duckType(arrow.ListOf(tAction)); !strings.HasPrefix(got, "STRUCT(schedule_time TIMESTAMP WITH TIME ZONE") || !strings.HasSuffix(got, "[]") {
		t.Fatal(got)
	}
	if trimWrapper(`{"structuredCalendar":[1]}`, "structuredCalendar") != "[1]" {
		t.Fatal("trimWrapper")
	}
}
