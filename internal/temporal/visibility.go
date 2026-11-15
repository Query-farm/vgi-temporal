// Translating pushed-down SQL filters into a Temporal visibility query (the
// SQL-like List Filter of ListWorkflowExecutions / CountWorkflowExecutions).
//
// The translation only has to keep a superset of the matching rows: the
// worker re-applies every pushed filter exactly (filters.go). So a predicate
// that cannot be expressed is simply left out of an AND, and an OR is pushed
// only when every branch translates.
//
// What translates:
//
//   - VARCHAR keyword columns (workflow_id, run_id, workflow_type, task_queue,
//     parent/root ids, Keyword search attributes): =, !=, IN.
//   - TIMESTAMPTZ columns (start_time, close_time, execution_time, Datetime
//     search attributes): <, <=, >, >=, = (as a one-microsecond range).
//   - BIGINT/DOUBLE columns (history_length, history_size_bytes,
//     state_transition_count, Int/Double search attributes): every comparison
//     and IN.
//   - BOOLEAN search attributes: =.
//   - status: =, !=, IN over Temporal status names (Running, Completed, ...).
//   - IS NULL / IS NOT NULL on nullable columns.
//
// String literals are single-quoted with ' doubled. Temporal's visibility
// parser does not round-trip backslashes, so a literal containing one is not
// pushed (the filter still applies locally).

package temporal

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// attrKind is how a column's values are compared in a visibility query.
type attrKind int

const (
	kindKeyword attrKind = iota
	kindDatetime
	kindInt
	kindDouble
	kindBool
)

// visAttr maps a SQL column to a visibility attribute.
type visAttr struct {
	Attr     string
	Kind     attrKind
	Nullable bool
	// Values maps a SQL literal to the attribute values it stands for; nil
	// means the literal is used as is. Returning ok=false makes the predicate
	// untranslatable.
	Values func(string) ([]string, bool)
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// executionStatuses are the ExecutionStatus values Temporal accepts.
var executionStatuses = []string{"Running", "Completed", "Failed", "Canceled", "Terminated", "ContinuedAsNew", "TimedOut"}

// rawStatusValues accepts only real status names (an unknown one is an error
// on the server, so it is left to the local filter, which returns no rows).
func rawStatusValues(s string) ([]string, bool) {
	for _, v := range executionStatuses {
		if v == s {
			return []string{s}, true
		}
	}
	return nil, false
}

// quoteString renders a string literal; ok is false when it cannot be pushed.
func quoteString(s string) (string, bool) {
	if strings.ContainsAny(s, "\\\x00") {
		return "", false
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'", true
}

func attrName(a string) string {
	if identRE.MatchString(a) {
		return a
	}
	return "`" + a + "`"
}

// pred is a pushed filter reduced to the shapes the translator understands.
type pred struct {
	Kind string // "and", "or", "isnull", "notnull", "in", "cmp"
	Col  string
	Op   vgi.ComparisonOp
	Vals []any // literal(s) as Go values; nil entries are SQL NULLs
	Kids []pred
}

// toPred converts an SDK filter; ok is false for shapes it does not model
// (they are never pushed, only applied locally).
func toPred(f vgi.Filter) (pred, bool) {
	switch ft := f.(type) {
	case *vgi.AndFilter, *vgi.OrFilter:
		kind, kids := "and", []vgi.Filter(nil)
		if a, ok := ft.(*vgi.AndFilter); ok {
			kids = a.Children
		} else {
			kind, kids = "or", ft.(*vgi.OrFilter).Children
		}
		p := pred{Kind: kind}
		for _, c := range kids {
			cp, ok := toPred(c)
			if !ok {
				cp = pred{Kind: "opaque"}
			}
			p.Kids = append(p.Kids, cp)
		}
		return p, true
	case *vgi.IsNullFilter:
		return pred{Kind: "isnull", Col: ft.ColumnName()}, true
	case *vgi.IsNotNullFilter:
		return pred{Kind: "notnull", Col: ft.ColumnName()}, true
	case *vgi.InFilter:
		if ft.Values == nil {
			return pred{}, false
		}
		p := pred{Kind: "in", Col: ft.ColumnName()}
		for i := 0; i < ft.Values.Len(); i++ {
			s, err := scalar.GetScalar(ft.Values, i)
			if err != nil {
				return pred{}, false
			}
			v, ok := scalarGo(s)
			if !ok {
				return pred{}, false
			}
			p.Vals = append(p.Vals, v)
		}
		return p, true
	case *vgi.ConstantFilter:
		v, ok := scalarGo(ft.Value)
		if !ok {
			return pred{}, false
		}
		return pred{Kind: "cmp", Col: ft.ColumnName(), Op: ft.Op, Vals: []any{v}}, true
	}
	return pred{}, false
}

// visibilityQuery builds the List Filter for the pushed filters (AND of the
// top-level filters), plus an optional user query ANDed in.
func visibilityQuery(pf *vgi.PushdownFilters, attrs map[string]visAttr, userQuery string) string {
	var preds []pred
	if pf != nil {
		for _, f := range pf.Filters {
			if p, ok := toPred(f); ok {
				preds = append(preds, p)
			}
		}
	}
	return buildQuery(preds, attrs, userQuery)
}

// buildQuery ANDs the translatable predicates and the user query.
func buildQuery(preds []pred, attrs map[string]visAttr, userQuery string) string {
	var parts []string
	if q := strings.TrimSpace(userQuery); q != "" {
		parts = append(parts, "("+q+")")
	}
	for _, p := range preds {
		if s, ok := translatePred(p, attrs, true); ok {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " AND ")
}

// translatePred renders one predicate. In an AND (lenient=true)
// untranslatable children are dropped; under an OR they make the whole
// predicate untranslatable.
func translatePred(p pred, attrs map[string]visAttr, lenient bool) (string, bool) {
	switch p.Kind {
	case "and":
		var parts []string
		for _, c := range p.Kids {
			s, ok := translatePred(c, attrs, lenient)
			if ok {
				parts = append(parts, s)
			} else if !lenient {
				return "", false
			}
		}
		if len(parts) == 0 {
			return "", false
		}
		return "(" + strings.Join(parts, " AND ") + ")", true
	case "or":
		var parts []string
		for _, c := range p.Kids {
			s, ok := translatePred(c, attrs, false)
			if !ok {
				return "", false
			}
			parts = append(parts, s)
		}
		if len(parts) == 0 {
			return "", false
		}
		return "(" + strings.Join(parts, " OR ") + ")", true
	case "isnull", "notnull":
		a, ok := attrs[p.Col]
		if !ok || !a.Nullable {
			return "", false
		}
		if p.Kind == "isnull" {
			return attrName(a.Attr) + " IS NULL", true
		}
		return attrName(a.Attr) + " IS NOT NULL", true
	case "in":
		a, ok := attrs[p.Col]
		if !ok || a.Kind == kindDatetime || a.Kind == kindBool {
			return "", false
		}
		var lits []string
		for _, v := range p.Vals {
			if v == nil {
				continue // NULL never matches IN
			}
			ls, ok := literals(a, v)
			if !ok {
				return "", false
			}
			lits = append(lits, ls...)
		}
		if len(lits) == 0 {
			return "", false
		}
		return attrName(a.Attr) + " IN (" + strings.Join(lits, ", ") + ")", true
	case "cmp":
		a, ok := attrs[p.Col]
		if !ok || len(p.Vals) != 1 || p.Vals[0] == nil {
			return "", false
		}
		return translateComparison(a, p.Op, p.Vals[0])
	}
	return "", false
}

// literals renders one SQL value as one or more attribute literals.
func literals(a visAttr, v any) ([]string, bool) {
	switch a.Kind {
	case kindKeyword:
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		vals := []string{s}
		if a.Values != nil {
			if vals, ok = a.Values(s); !ok {
				return nil, false
			}
		}
		out := make([]string, 0, len(vals))
		for _, x := range vals {
			q, ok := quoteString(x)
			if !ok {
				return nil, false
			}
			out = append(out, q)
		}
		return out, len(out) > 0
	case kindInt:
		switch x := v.(type) {
		case int64:
			return []string{strconv.FormatInt(x, 10)}, true
		}
	case kindDouble:
		switch x := v.(type) {
		case int64:
			return []string{strconv.FormatInt(x, 10)}, true
		case float64:
			return []string{strconv.FormatFloat(x, 'g', -1, 64)}, true
		}
	case kindBool:
		if b, ok := v.(bool); ok {
			return []string{strconv.FormatBool(b)}, true
		}
	case kindDatetime:
		if t, ok := v.(time.Time); ok {
			return []string{"'" + t.UTC().Format(time.RFC3339Nano) + "'"}, true
		}
	}
	return nil, false
}

func translateComparison(a visAttr, op vgi.ComparisonOp, v any) (string, bool) {
	name := attrName(a.Attr)
	switch a.Kind {
	case kindKeyword:
		lits, ok := literals(a, v)
		if !ok {
			return "", false
		}
		switch op {
		case vgi.OpEQ:
			if len(lits) == 1 {
				return name + " = " + lits[0], true
			}
			return name + " IN (" + strings.Join(lits, ", ") + ")", true
		case vgi.OpNE:
			if len(lits) == 1 {
				return name + " != " + lits[0], true
			}
			return name + " NOT IN (" + strings.Join(lits, ", ") + ")", true
		}
		return "", false
	case kindBool:
		if op != vgi.OpEQ {
			return "", false
		}
		lits, ok := literals(a, v)
		if !ok {
			return "", false
		}
		return name + " = " + lits[0], true
	case kindDatetime:
		t, ok := v.(time.Time)
		if !ok {
			return "", false
		}
		// Temporal keeps nanoseconds while the column shows microseconds, so
		// an upper bound is widened to the end of the microsecond.
		next := "'" + t.Add(time.Microsecond).UTC().Format(time.RFC3339Nano) + "'"
		lit := "'" + t.UTC().Format(time.RFC3339Nano) + "'"
		switch op {
		case vgi.OpGT, vgi.OpGE:
			return name + " " + op.Symbol() + " " + lit, true
		case vgi.OpLT:
			return name + " < " + lit, true
		case vgi.OpLE:
			return name + " < " + next, true
		case vgi.OpEQ:
			return fmt.Sprintf("(%s >= %s AND %s < %s)", name, lit, name, next), true
		}
		return "", false
	case kindInt, kindDouble:
		lits, ok := literals(a, v)
		if !ok {
			return "", false
		}
		switch op {
		case vgi.OpEQ, vgi.OpNE, vgi.OpLT, vgi.OpLE, vgi.OpGT, vgi.OpGE:
			sym := op.Symbol()
			if op == vgi.OpNE {
				sym = "!="
			}
			return name + " " + sym + " " + lits[0], true
		}
	}
	return "", false
}
