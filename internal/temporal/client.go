// Connection to a Temporal frontend (self-hosted or Temporal Cloud).
//
// Credentials come from a DuckDB secret of type `temporal`:
//
//	CREATE SECRET (TYPE temporal, HOST 'my-ns.a1b2c.tmprl.cloud:7233',
//	    NAMESPACE 'my-ns.a1b2c', API_KEY '...');
//	CREATE SECRET (TYPE temporal, HOST 'temporal.internal:7233', NAMESPACE 'prod',
//	    TLS 'true', CA_CERT '-----BEGIN ...', CLIENT_CERT '...', CLIENT_KEY '...');
//
// With no secret the worker talks to a local dev server at 127.0.0.1:7233.
// The secret is read on every call and never stored in scan state (which
// travels to DuckDB in HTTP continuation tokens).

package temporal

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// SecretType is the DuckDB secret type holding the connection settings.
const SecretType = "temporal"

// DefaultHost is used when no secret is defined: a local `temporal server start-dev`.
const DefaultHost = "127.0.0.1:7233"

// userAgent identifies the worker to the server.
const userAgent = "vgi-temporal-worker/" + Version

// callTimeout bounds every RPC attempt.
const callTimeout = 30 * time.Second

// maxAttempts bounds retries of Unavailable / ResourceExhausted responses.
const maxAttempts = 4

func secretField(name, doc string, redact bool) arrow.Field {
	keys, vals := []string{"vgi_doc"}, []string{doc}
	if redact {
		keys, vals = append(keys, "redact"), append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata(keys, vals)}
}

// TemporalSecretType registers CREATE SECRET (TYPE temporal, ...).
var TemporalSecretType = vgi.SecretTypeSpec{
	Name: SecretType,
	Description: "Temporal frontend address, namespace and credentials: an API key (Temporal Cloud), " +
		"mTLS client certificates, or none for a local dev server.",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("host", "Frontend gRPC address host:port, e.g. my-ns.a1b2c.tmprl.cloud:7233 or "+
			"<region>.<cloud>.api.temporal.io:7233 (Temporal Cloud) or temporal.internal:7233. Default 127.0.0.1:7233.", false),
		secretField("namespace", "Namespace queried by default, e.g. my-ns.a1b2c on Temporal Cloud. Default 'default'.", false),
		secretField("api_key", "Temporal Cloud API key, sent as 'Authorization: Bearer <key>'. Turns TLS on unless TLS is 'false'.", true),
		secretField("tls", "'true' to connect with TLS (default 'false', or 'true' when API_KEY or CLIENT_CERT is set).", false),
		secretField("ca_cert", "PEM CA bundle that signed the server certificate; default the system roots.", false),
		secretField("client_cert", "PEM client certificate for mTLS (Temporal Cloud mTLS namespaces, self-hosted mTLS).", false),
		secretField("client_key", "PEM private key of CLIENT_CERT.", true),
		secretField("server_name", "TLS server name to verify, when it differs from the HOST name.", false),
		secretField("ui_url", "Base URL of the Temporal Web UI used for the url columns, e.g. https://cloud.temporal.io or "+
			"http://localhost:8233. Default: derived from HOST for Temporal Cloud and local dev servers, otherwise none.", false),
	}, &secretSchemaMetadata),
}

var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{"https://github.com/Query-farm/vgi-temporal#secrets-and-configuration"})

// config is a resolved temporal secret. Never stored in scan state.
type config struct {
	Host       string
	Namespace  string
	APIKey     string
	TLS        bool
	CACert     string
	ClientCert string
	ClientKey  string
	ServerName string
	UIURL      string
}

// configFrom reads the temporal secret (or the local dev-server defaults).
func configFrom(secrets vgi.Secrets) (*config, error) {
	c := &config{Host: DefaultHost, Namespace: "default"}
	s, ok := secrets.Lookup(SecretType)
	if !ok {
		c.UIURL = defaultUIURL(c.Host)
		return c, nil
	}
	get := func(k string) string { return strings.TrimSpace(vgi.RenderSecretValue(s[k])) }
	if h := get("host"); h != "" {
		c.Host = h
	}
	if n := get("namespace"); n != "" {
		c.Namespace = n
	}
	c.APIKey = get("api_key")
	c.CACert, c.ClientCert, c.ClientKey = get("ca_cert"), get("client_cert"), get("client_key")
	c.ServerName = get("server_name")
	switch strings.ToLower(get("tls")) {
	case "true", "1", "yes", "on":
		c.TLS = true
	case "false", "0", "no", "off":
		c.TLS = false
	case "":
		c.TLS = c.APIKey != "" || c.ClientCert != ""
	default:
		return nil, fmt.Errorf("temporal secret: TLS must be 'true' or 'false', got %q", get("tls"))
	}
	if (c.ClientCert == "") != (c.ClientKey == "") {
		return nil, errors.New("temporal secret: CLIENT_CERT and CLIENT_KEY must be set together")
	}
	if _, _, err := net.SplitHostPort(c.Host); err != nil {
		return nil, fmt.Errorf("temporal secret: HOST must be host:port, got %q", c.Host)
	}
	c.UIURL = strings.TrimRight(get("ui_url"), "/")
	if c.UIURL == "" {
		c.UIURL = defaultUIURL(c.Host)
	}
	return c, nil
}

// defaultUIURL guesses the Web UI for Temporal Cloud and local dev servers.
func defaultUIURL(host string) string {
	h, port, _ := net.SplitHostPort(host)
	switch {
	case strings.HasSuffix(h, ".tmprl.cloud") || strings.HasSuffix(h, ".api.temporal.io"):
		return "https://cloud.temporal.io"
	case (h == "127.0.0.1" || h == "localhost") && port == "7233":
		return "http://localhost:8233"
	}
	return ""
}

// workflowURL links to a run's history in the Web UI ("" when unknown).
func (c *config) workflowURL(ns, workflowID, runID string) any {
	if c.UIURL == "" || workflowID == "" {
		return nil
	}
	u := c.UIURL + "/namespaces/" + url.PathEscape(ns) + "/workflows/" + url.PathEscape(workflowID)
	if runID != "" {
		u += "/" + url.PathEscape(runID)
	}
	return u + "/history"
}

func (c *config) key() string {
	h := sha256.Sum256([]byte(strings.Join([]string{c.Host, c.APIKey, fmt.Sprint(c.TLS), c.CACert, c.ClientCert, c.ClientKey, c.ServerName}, "\x00")))
	return hex.EncodeToString(h[:])
}

// client bundles the gRPC services of one connection.
type client struct {
	cfg *config
	wf  workflowservice.WorkflowServiceClient
	op  operatorservice.OperatorServiceClient
}

var (
	connMu sync.Mutex
	conns  = map[string]*grpc.ClientConn{}
)

// connect returns a client for the secret, reusing connections per config.
func connect(secrets vgi.Secrets) (*client, error) {
	cfg, err := configFrom(secrets)
	if err != nil {
		return nil, err
	}
	connMu.Lock()
	defer connMu.Unlock()
	k := cfg.key()
	cc, ok := conns[k]
	if !ok {
		cc, err = dial(cfg)
		if err != nil {
			return nil, err
		}
		conns[k] = cc
	}
	return &client{cfg: cfg, wf: workflowservice.NewWorkflowServiceClient(cc), op: operatorservice.NewOperatorServiceClient(cc)}, nil
}

func dial(cfg *config) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithUserAgent(userAgent),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128 << 20)),
		grpc.WithChainUnaryInterceptor(headerInterceptor(cfg.APIKey), retryInterceptor),
	}
	if cfg.TLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
		if cfg.CACert != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(cfg.CACert)) {
				return nil, errors.New("temporal secret: CA_CERT holds no PEM certificate")
			}
			tc.RootCAs = pool
		}
		if cfg.ClientCert != "" {
			pair, err := tls.X509KeyPair([]byte(cfg.ClientCert), []byte(cfg.ClientKey))
			if err != nil {
				return nil, fmt.Errorf("temporal secret: CLIENT_CERT/CLIENT_KEY: %w", err)
			}
			tc.Certificates = []tls.Certificate{pair}
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cc, err := grpc.NewClient(cfg.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to Temporal at %s: %w", cfg.Host, err)
	}
	return cc, nil
}

// namespaced is any request that names its namespace.
type namespaced interface{ GetNamespace() string }

// headerInterceptor adds the API key and the temporal-namespace routing header
// Temporal Cloud needs with API-key auth.
func headerInterceptor(apiKey string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if apiKey != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
		}
		if n, ok := req.(namespaced); ok && n.GetNamespace() != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "temporal-namespace", n.GetNamespace())
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryInterceptor bounds each attempt and retries Unavailable and
// ResourceExhausted (rate limiting) with exponential backoff.
func retryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, callTimeout)
		err := invoker(actx, method, req, reply, cc, opts...)
		cancel()
		if err == nil {
			return nil
		}
		code := status.Code(err)
		if (code != codes.Unavailable && code != codes.ResourceExhausted) || attempt >= maxAttempts || ctx.Err() != nil {
			return describeError(method, err)
		}
		sleep(ctx, time.Duration(250<<attempt)*time.Millisecond)
	}
}

// describeError turns a gRPC error into a message naming the call.
func describeError(method string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	name := method[strings.LastIndex(method, "/")+1:]
	return fmt.Errorf("temporal %s failed (%s): %s", name, st.Code(), st.Message())
}

// isUnimplemented reports whether an RPC is not available on this server
// (or not permitted, as on Temporal Cloud for some operator calls).
func isUnimplemented(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if s, ok := status.FromError(e); ok {
			switch s.Code() {
			case codes.Unimplemented, codes.PermissionDenied:
				return true
			}
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "(Unimplemented)") || strings.Contains(msg, "(PermissionDenied)")
}
