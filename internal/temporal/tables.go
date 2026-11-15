// The table-function framework every Temporal table uses.
//
// A tableDef names its arguments, columns and docs, and supplies Init (once
// per scan) and Page (one upstream page per call). The wrapper connects with
// the session's temporal secret, re-applies pushed filters exactly, projects
// the requested columns and streams one page per batch. The cursor lives in
// ScanState, whose exported fields the SDK carries in HTTP continuation
// tokens, so a long scan survives the HTTP transport.

package temporal

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// RunRef is one workflow run queued for a per-run read (unified.task_runs).
type RunRef struct {
	WorkflowID string
	RunID      string
	Running    bool
}

// ScanState is the resumable cursor shared by every table. It never holds
// credentials: the secret is re-read on every call.
type ScanState struct {
	Started       bool
	Done          bool
	Namespace     string
	Query         string
	Token         []byte
	WorkflowID    string
	RunID         string
	TaskQueue     string
	GroupByStatus bool
	Step          int
	Runs          []RunRef
	ListDone      bool
	PageSize      int32
}

// tableDef defines one table function.
type tableDef struct {
	Name string
	Docs fnDocs
	// Args is a struct with `vgi:"..."` tags (nil for no arguments).
	Args any
	// Schema is the output schema; Bind (optional) computes it per call.
	Schema *arrow.Schema
	Bind   func(p *vgi.BindParams) (*arrow.Schema, error)
	// Filter enables filter pushdown (filters are always re-applied exactly).
	Filter bool
	Init   func(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState) error
	Page   func(ctx context.Context, c *client, p *vgi.ProcessParams, st *ScanState, full *arrow.Schema) ([][]any, error)
}

type tableFunc struct{ def *tableDef }

var _ vgi.TypedTableFunc[ScanState] = (*tableFunc)(nil)

func (t *tableFunc) Name() string { return t.def.Name }

func (t *tableFunc) Metadata() vgi.FunctionMetadata {
	d := t.def.Docs
	tags := objectTags(d)
	tags["vgi.result_columns_schema"] = mustJSON(schemaColumns(t.def.Schema))
	return vgi.FunctionMetadata{
		Description:        d.Description,
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		FilterPushdown:     t.def.Filter,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: SecretType}},
		Tags:               tags,
		Examples:           d.Examples,
	}
}

func (t *tableFunc) ArgumentSpecs() []vgi.ArgSpec {
	if t.def.Args == nil {
		return nil
	}
	return vgi.DeriveArgSpecs(t.def.Args)
}

func (t *tableFunc) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	if t.def.Bind != nil {
		s, err := t.def.Bind(p)
		if err != nil {
			return nil, err
		}
		return vgi.BindSchema(s)
	}
	return vgi.BindSchema(t.def.Schema)
}

func (t *tableFunc) NewState(*vgi.ProcessParams) (*ScanState, error) {
	return &ScanState{}, nil
}

func (t *tableFunc) Process(ctx context.Context, p *vgi.ProcessParams, st *ScanState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := connect(p.Secrets)
	if err != nil {
		return err
	}
	if !st.Started {
		st.Namespace = c.cfg.Namespace
		if err := t.def.Init(ctx, c, p, st); err != nil {
			return err
		}
		st.Started = true
	}
	full := p.BindOutputSchema
	if full == nil {
		full = t.def.Schema
	}
	for {
		rows, err := t.def.Page(ctx, c, p, st, full)
		if err != nil {
			return err
		}
		rows, err = applyFilters(ctx, p.CurrentPushdownFilters, full, rows)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			batch, err := buildBatch(p.OutputSchema, full, rows)
			if err != nil {
				return err
			}
			return out.Emit(batch)
		}
		if st.Done {
			return out.Finish()
		}
	}
}

// newTable wraps a definition for registration.
func newTable(def *tableDef) vgi.TableFunction {
	return vgi.AsTableFunction[ScanState](&tableFunc{def: def})
}

// namespaceArg resolves an optional namespace := argument against the secret.
func namespaceArg(ns *string, st *ScanState) {
	if ns != nil && *ns != "" {
		st.Namespace = *ns
	}
}

// pageSize grows from a small first page (cheap LIMIT queries) to the maximum.
// An explicit page_size := argument (st.PageSize) wins.
func pageSize(st *ScanState, first, maxSize int32) int32 {
	if st.PageSize > 0 {
		return st.PageSize
	}
	if st.Step == 0 {
		return first
	}
	return maxSize
}
