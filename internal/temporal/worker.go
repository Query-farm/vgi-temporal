// Package temporal exposes a Temporal cluster (self-hosted or Temporal Cloud)
// to DuckDB as read-only SQL: catalog temporal, schemas main and unified.
//
// Run it:
//
//	vgi-temporal-worker                      # stdio: what DuckDB launches by default
//	vgi-temporal-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//
// Attach it from DuckDB:
//
//	CREATE SECRET (TYPE temporal, HOST '127.0.0.1:7233', NAMESPACE 'default');
//	ATTACH 'temporal' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-temporal-worker');
package temporal

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// mainTables are the table functions of schema main.
var mainTables = []*tableDef{executionsDef, countDef, historyDef, pendingDef, schedulesDef, taskQueuesDef, searchAttributesDef, batchDef}

// unifiedTables are the catalog tables of schema unified.
var unifiedTables = []*tableDef{unifiedRunsDef, unifiedTaskRunsDef}

func examplesOf(defs []*tableDef, extra ...[]vgi.CatalogExample) []exampleQuery {
	lists := make([][]vgi.CatalogExample, 0, len(defs)+len(extra))
	for _, d := range defs {
		lists = append(lists, d.Docs.Examples)
	}
	return exampleQueries(append(lists, extra...)...)
}

// NewWorker builds the worker. Extra options (logging, storage) are appended
// after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogComment),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": MainSchemaComment, "unified": UnifiedSchemaComment}),
		vgi.WithSchemaTags(map[string]map[string]string{
			"main":    schemaTags(mainSchema, examplesOf(append(mainTables, namespacesDef), unifiedStatusDocs.Examples)),
			"unified": schemaTags(unifiedSchema, examplesOf(unifiedTables)),
		}),
		vgi.WithSecretTypes(TemporalSecretType),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	for _, d := range mainTables {
		w.RegisterTable(newTable(d))
	}
	w.RegisterCatalogTable("main", catalogTable("main", namespacesDef, "name", nil))
	w.RegisterScalar(vgi.AsScalarFunction[unifiedStatusArgs](&unifiedStatusFn{}))
	w.RegisterCatalogTable("unified", catalogTable("unified", unifiedRunsDef, "run_id", nil))
	w.RegisterCatalogTable("unified", catalogTable("unified", unifiedTaskRunsDef, "task_run_id", map[string]string{"run_id": "runs"}))
	return w
}
