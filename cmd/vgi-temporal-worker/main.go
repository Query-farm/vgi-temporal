// Command vgi-temporal-worker serves the temporal catalog to DuckDB.
//
//	vgi-temporal-worker                                  # stdio (what DuckDB launches)
//	vgi-temporal-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-temporal-worker --unix /tmp/temporal.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'temporal' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-temporal-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-temporal/internal/serve"
	"github.com/Query-farm/vgi-temporal/internal/temporal"
)

func main() {
	serve.Main("vgi-temporal-worker", os.Args[1:], func() *vgi.Worker { return temporal.NewWorker() })
}
