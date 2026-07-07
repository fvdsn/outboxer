// Package harness drives cloud integration tests against a deployed Outboxer
// stack. It is environment-agnostic: each deploy target (Cloud Run, GKE, ECS,
// EKS) provides an Env from its Terraform outputs and a thin test package
// that picks scenarios; the harness owns event production, consumption,
// metrics sampling, and performance reporting so every environment runs
// identical assertions.
package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Env describes a deployed stack, loaded from `terraform output -json`.
type Env struct {
	ProjectID              string `tf:"project_id"`
	Region                 string `tf:"region"`
	ServiceURL             string `tf:"service_url"`
	CloudSQLConnectionName string `tf:"cloudsql_connection_name"`
	DBName                 string `tf:"db_name"`
	DBUser                 string `tf:"db_user"`
	DBPassword             string `tf:"db_password"`
	Topic                  string `tf:"topic"`
	Subscription           string `tf:"subscription"`
	DLQTable               string `tf:"dlq_table"`
}

// LoadEnv parses a `terraform output -json` file. Tests skip when the file is
// absent: the stack has not been brought up.
func LoadEnv(t *testing.T, path string) Env {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("no terraform outputs at %s (run the cloud up recipe first): %v", path, err)
	}

	var outputs map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(content, &outputs); err != nil {
		t.Fatalf("parse terraform outputs: %v", err)
	}
	value := func(name string) string {
		raw, ok := outputs[name]
		if !ok {
			t.Fatalf("terraform output %q missing from %s", name, path)
		}
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			t.Fatalf("terraform output %q is not a string: %v", name, err)
		}
		return s
	}

	return Env{
		ProjectID:              value("project_id"),
		Region:                 value("region"),
		ServiceURL:             value("service_url"),
		CloudSQLConnectionName: value("cloudsql_connection_name"),
		DBName:                 value("db_name"),
		DBUser:                 value("db_user"),
		DBPassword:             value("db_password"),
		Topic:                  value("topic"),
		Subscription:           value("subscription"),
		DLQTable:               value("dlq_table"),
	}
}

// StartCloudSQLProxy launches cloud-sql-proxy for the stack's instance and
// returns a Postgres connection once it is reachable. Requires the
// cloud-sql-proxy binary on PATH and ADC credentials with cloudsql.client.
func StartCloudSQLProxy(ctx context.Context, t *testing.T, env Env) *pgx.Conn {
	t.Helper()

	port := freePort(t)
	proxyCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(proxyCtx, "cloud-sql-proxy", "--port", fmt.Sprint(port), env.CloudSQLConnectionName)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start cloud-sql-proxy (is it installed?): %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cloud-sql-proxy did not become ready: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable",
		env.DBUser, env.DBPassword, port, env.DBName)
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect through cloud-sql-proxy: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

// Event is one row the harness produces into the outbox table.
type Event struct {
	Payload     string
	Target      string
	Destination string
	OrderingKey string
}

// InsertEvents bulk-loads events with the COPY protocol; hundreds of
// thousands of rows load in seconds through the proxy.
func InsertEvents(ctx context.Context, db *pgx.Conn, events []Event) error {
	rows := make([][]any, len(events))
	now := time.Now().UTC()
	for i, evt := range events {
		var options any
		if evt.OrderingKey != "" {
			payload, err := json.Marshal(map[string]any{
				"pubsub": map[string]any{"orderingKey": evt.OrderingKey},
			})
			if err != nil {
				return err
			}
			options = string(payload)
		}
		rows[i] = []any{now, evt.Payload, nullable(evt.Target), nullable(evt.Destination), options}
	}

	_, err := db.CopyFrom(ctx,
		pgx.Identifier{"events"},
		[]string{"timestamp", "payload", "target", "destination", "options"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// CountRows returns the row count of a table.
func CountRows(ctx context.Context, db *pgx.Conn, table string) (int, error) {
	var count int
	err := db.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %q", table)).Scan(&count)
	return count, err
}

// WaitForEmptyTable polls until the outbox drains or the deadline passes.
func WaitForEmptyTable(ctx context.Context, t *testing.T, db *pgx.Conn, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		count, err := CountRows(ctx, db, table)
		if err == nil && count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("table %s did not drain within %s (last count %d, err %v)", table, timeout, count, err)
		}
		time.Sleep(time.Second)
	}
}

// Metrics fetches and parses the relay's /metrics endpoint, authenticating
// with the operator's identity token (via gcloud, which supports user ADC).
type Metrics struct {
	env   Env
	token string
}

// NewMetrics mints an identity token for the operator and returns a fetcher
// for the relay's HTTP endpoints.
func NewMetrics(t *testing.T, env Env) *Metrics {
	t.Helper()
	token, err := exec.Command("gcloud", "auth", "print-identity-token").Output()
	if err != nil {
		t.Fatalf("mint identity token via gcloud: %v", err)
	}
	return &Metrics{env: env, token: strings.TrimSpace(string(token))}
}

// Fetch returns every single-sample series by name.
func (m *Metrics) Fetch(ctx context.Context) (map[string]float64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.env.ServiceURL+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+m.token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return nil, fmt.Errorf("GET /metrics: status %d: %s", response.StatusCode, body)
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, err
	}
	values := map[string]float64{}
	for name, family := range families {
		if metrics := family.GetMetric(); len(metrics) == 1 {
			values[name] = metrics[0].GetCounter().GetValue() + metrics[0].GetGauge().GetValue()
		}
	}
	return values, nil
}

// Healthz returns the status code of the relay's /healthz endpoint.
func (m *Metrics) Healthz(ctx context.Context) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.env.ServiceURL+"/healthz", nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+m.token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, nil
}
