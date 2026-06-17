//go:build perf

package perf_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	perfProjectID      = "outboxer-test"
	perfPubSubEndpoint = "localhost:8085"
	perfSQSEndpoint    = "http://localhost:9324"
	perfPGDSN          = "postgres://outboxer:outboxer@localhost:54329/outboxer?sslmode=disable"
	perfAWSRegion      = "us-east-1"
)

func TestLocalCollectBatchTargetPerf(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping local performance test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), getenvDuration("OUTBOXER_PERF_TIMEOUT", 45*time.Minute))
	defer cancel()

	root := repoRoot(t)
	binary := buildOutboxer(t, root)
	db := openPerfDB(t, ctx)
	defer db.Close()

	records := getenvInt("OUTBOXER_PERF_RECORDS", 1_000_000)
	routes := getenvInt("OUTBOXER_PERF_ROUTES", 100)
	targets := getenvIntList("OUTBOXER_PERF_BATCH_TARGETS", []int{100, 250, 500, 2500, 5000})

	t.Logf("perf setup records=%d routes=%d batch_targets=%v", records, routes, targets)

	if !getenvBool("OUTBOXER_PERF_SKIP_PUBSUB", false) {
		t.Run("pubsub", func(t *testing.T) {
			topicIDs := createPerfPubSubTopics(t, ctx, routes)
			runBackendTargetSweep(t, ctx, binary, db, "pubsub", topicIDs, records, targets)
		})
	}

	if !getenvBool("OUTBOXER_PERF_SKIP_SQS", false) {
		t.Run("sqs", func(t *testing.T) {
			for _, target := range targets {
				queueURLs := createPerfSQSQueues(t, ctx, routes, target)
				result := runBackendOnce(t, ctx, binary, db, "sqs", queueURLs, records, target)
				t.Log(result.String())
				deletePerfSQSQueues(t, ctx, queueURLs)
			}
		})
	}
}

type perfResult struct {
	backend string
	records int
	routes  int
	target  int
	elapsed time.Duration
	rate    float64
}

func (r perfResult) String() string {
	return fmt.Sprintf("PERF backend=%s records=%d routes=%d collect_batch_target=%d elapsed=%s rate=%.0f events/s",
		r.backend, r.records, r.routes, r.target, r.elapsed.Round(time.Millisecond), r.rate)
}

func runBackendTargetSweep(t *testing.T, ctx context.Context, binary string, db *sql.DB, backend string, destinations []string, records int, targets []int) {
	t.Helper()
	for _, target := range targets {
		result := runBackendOnce(t, ctx, binary, db, backend, destinations, records, target)
		t.Log(result.String())
	}
}

func runBackendOnce(t *testing.T, ctx context.Context, binary string, db *sql.DB, backend string, destinations []string, records int, target int) perfResult {
	t.Helper()
	table := "outboxer_perf_" + backend + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	createPerfEventsTable(t, ctx, db, table)
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(table)))
	}()

	insertPerfEvents(t, ctx, db, table, destinations, records)
	createPerfRouteIndex(t, ctx, db, table, destinations[0])

	process := startOutboxer(t, ctx, binary, table, backend, destinations[0], target)
	started := time.Now()
	waitForTableCount(t, ctx, db, table, 0)
	elapsed := time.Since(started)
	stopOutboxer(t, process)

	return perfResult{
		backend: backend,
		records: records,
		routes:  len(destinations),
		target:  target,
		elapsed: elapsed,
		rate:    float64(records) / elapsed.Seconds(),
	}
}

func createPerfEventsTable(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id text PRIMARY KEY,
			timestamp timestamptz,
			payload text NOT NULL,
			target text,
			destination text,
			ordering_key text,
			attributes jsonb
		)
	`, ident(table)))
	if err != nil {
		t.Fatalf("create perf table: %v", err)
	}
}

func insertPerfEvents(t *testing.T, ctx context.Context, db *sql.DB, table string, destinations []string, records int) {
	t.Helper()
	destTable := table + "_destinations"
	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (route int PRIMARY KEY, destination text NOT NULL)`, ident(destTable)))
	if err != nil {
		t.Fatalf("create destinations table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", ident(destTable)))
	}()
	for route, destination := range destinations {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (route, destination) VALUES ($1, $2)`, ident(destTable)), route, destination)
		if err != nil {
			t.Fatalf("insert destination %d: %v", route, err)
		}
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, timestamp, payload, destination)
		SELECT format('event-%%07s', gs), now(), 'payload', destinations.destination
		FROM generate_series(1, $1) AS gs
		JOIN %s AS destinations ON destinations.route = ((gs - 1) %% $2)
	`, ident(table), ident(destTable)), records, len(destinations))
	if err != nil {
		t.Fatalf("insert perf events: %v", err)
	}
}

func createPerfRouteIndex(t *testing.T, ctx context.Context, db *sql.DB, table string, defaultDestination string) {
	t.Helper()
	index := table + "_route_idx"
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX %s ON %s ((COALESCE(NULLIF(destination, ''), %s)), id)`,
		ident(index),
		ident(table),
		sqlStringLiteral(defaultDestination),
	))
	if err != nil {
		t.Fatalf("create perf route index: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`ANALYZE %s`, ident(table))); err != nil {
		t.Fatalf("analyze perf table: %v", err)
	}
}

func startOutboxer(t *testing.T, ctx context.Context, binary string, table string, backend string, defaultDestination string, target int) *runningProcess {
	t.Helper()
	var output bytes.Buffer
	args := []string{
		"--event-target=",
		"--event-table=" + table,
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	env := map[string]string{
		"EVENT_TABLE":               table,
		"EVENT_DESTINATION":         "destination",
		"EVENT_ORDERING_KEY":        "ordering_key",
		"EVENT_ATTRIBUTES":          "attributes",
		"EVENT_PAYLOAD":             "payload",
		"EVENT_ID":                  "id",
		"EVENT_TIMESTAMP":           "timestamp",
		"COLLECTION_MODE":           "per_route_ordered",
		"COLLECT_GLOBAL_LIMIT":      "100",
		"COLLECT_BATCH_TARGET":      strconv.Itoa(target),
		"ORDERED_GROUP_BATCH_CAP":   "1000",
		"SQS_SEND_CONCURRENCY":      getenv("OUTBOXER_PERF_SQS_SEND_CONCURRENCY", "64"),
		"POLL_INTERVAL_MS":          "0",
		"ERROR_COOLDOWN_MS":         "1000",
		"PUBLISH_TIMEOUT_MS":        "30000",
		"PUBLISH_RESULT_GRACE_MS":   "5000",
		"WATCHDOG_INTERVAL_MS":      "1800000",
		"HEALTH_PORT":               "0",
		"LOG_LEVEL":                 getenv("OUTBOXER_PERF_LOG_LEVEL", "error"),
		"PG_HOST":                   getenv("OUTBOXER_PERF_PG_HOST", "localhost"),
		"PG_PORT":                   getenv("OUTBOXER_PERF_PG_PORT", "54329"),
		"PG_USER":                   getenv("OUTBOXER_PERF_PG_USER", "outboxer"),
		"PG_PASSWORD":               getenv("OUTBOXER_PERF_PG_PASSWORD", "outboxer"),
		"PG_DATABASE":               getenv("OUTBOXER_PERF_PG_DATABASE", "outboxer"),
		"PG_SSL":                    "false",
		"PUBSUB_PROJECT_ID":         perfProjectID,
		"PUBSUB_EMULATOR_HOST":      getenv("OUTBOXER_PERF_PUBSUB_ENDPOINT", perfPubSubEndpoint),
		"SQS_API_ENDPOINT":          getenv("OUTBOXER_PERF_SQS_ENDPOINT", perfSQSEndpoint),
		"AWS_REGION":                perfAWSRegion,
		"AWS_ACCESS_KEY_ID":         "test",
		"AWS_SECRET_ACCESS_KEY":     "test",
		"AWS_WEB_IDENTITY_PROVIDER": "",
	}
	switch backend {
	case "pubsub":
		env["PUBSUB_ENABLED"] = "true"
		env["SQS_ENABLED"] = "false"
		env["DEFAULT_PUBSUB_TOPIC"] = defaultDestination
		env["DEFAULT_SQS_QUEUE_URL"] = ""
	case "sqs":
		env["PUBSUB_ENABLED"] = "false"
		env["SQS_ENABLED"] = "true"
		env["DEFAULT_PUBSUB_TOPIC"] = ""
		env["DEFAULT_SQS_QUEUE_URL"] = defaultDestination
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	cmd.Env = mergedEnv(os.Environ(), env)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start outboxer: %v\n%s", err, output.String())
	}
	process := &runningProcess{cmd: cmd, output: &output}
	t.Cleanup(func() { stopOutboxer(t, process) })
	return process
}

func createPerfPubSubTopics(t *testing.T, ctx context.Context, routes int) []string {
	t.Helper()
	t.Setenv("PUBSUB_EMULATOR_HOST", getenv("OUTBOXER_PERF_PUBSUB_ENDPOINT", perfPubSubEndpoint))
	client, err := pubsub.NewClient(ctx, perfProjectID)
	if err != nil {
		t.Fatalf("create Pub/Sub client: %v", err)
	}
	defer client.Close()

	prefix := "perf_topic_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	topics := make([]string, routes)
	for route := range routes {
		topic := fmt.Sprintf("%s_%03d", prefix, route)
		name := fmt.Sprintf("projects/%s/topics/%s", perfProjectID, topic)
		if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
			t.Fatalf("create Pub/Sub topic %s: %v", topic, err)
		}
		topics[route] = topic
	}
	return topics
}

func createPerfSQSQueues(t *testing.T, ctx context.Context, routes int, target int) []string {
	t.Helper()
	client := newSQSClient(t, ctx)
	prefix := fmt.Sprintf("perf-sqs-%d-%d", target, time.Now().UnixNano())
	queues := make([]string, routes)
	for route := range routes {
		output, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(fmt.Sprintf("%s-%03d", prefix, route)),
		})
		if err != nil {
			t.Fatalf("create SQS queue %d: %v", route, err)
		}
		queues[route] = aws.ToString(output.QueueUrl)
	}
	return queues
}

func deletePerfSQSQueues(t *testing.T, ctx context.Context, queueURLs []string) {
	t.Helper()
	client := newSQSClient(t, ctx)
	for _, queueURL := range queueURLs {
		_, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
		if err != nil {
			t.Logf("delete SQS queue %s: %v", queueURL, err)
		}
	}
}

func newSQSClient(t *testing.T, ctx context.Context) *sqs.Client {
	t.Helper()
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(perfAWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load local SQS config: %v", err)
	}
	return sqs.NewFromConfig(awsCfg, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(getenv("OUTBOXER_PERF_SQS_ENDPOINT", perfSQSEndpoint))
	})
}

func waitForTableCount(t *testing.T, ctx context.Context, db *sql.DB, table string, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastRemaining := -1
	lastLog := time.Now()
	for {
		var remaining int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", ident(table))).Scan(&remaining); err != nil {
			t.Fatalf("count perf table: %v", err)
		}
		if remaining == want {
			return
		}
		if remaining != lastRemaining && time.Since(lastLog) >= 10*time.Second {
			t.Logf("waiting for %s count=%d want=%d", table, remaining, want)
			lastLog = time.Now()
			lastRemaining = remaining
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s count=%d want=%d", table, remaining, want)
		case <-ticker.C:
		}
	}
}

func buildOutboxer(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "outboxer")
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/outboxer")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build outboxer: %v\n%s", err, output)
	}
	return binary
}

func openPerfDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", getenv("OUTBOXER_PERF_PG_DSN", perfPGDSN))
	if err != nil {
		t.Fatalf("open perf database: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping perf database: %v", err)
	}
	return db
}

type runningProcess struct {
	cmd    *exec.Cmd
	output *bytes.Buffer
	once   sync.Once
}

func stopOutboxer(t *testing.T, process *runningProcess) {
	t.Helper()
	if process == nil || process.cmd.Process == nil {
		return
	}
	process.once.Do(func() {
		_ = process.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- process.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
				t.Logf("outboxer exited with %v\n%s", err, process.output.String())
			}
		case <-time.After(10 * time.Second):
			_ = process.cmd.Process.Kill()
			t.Fatalf("outboxer did not stop\n%s", process.output.String())
		}
	})
}

func mergedEnv(base []string, overrides map[string]string) []string {
	env := make([]string, 0, len(base)+len(overrides))
	seen := map[string]struct{}{}
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if value, ok := overrides[key]; ok {
			env = append(env, key+"="+value)
			seen[key] = struct{}{}
			continue
		}
		env = append(env, entry)
		seen[key] = struct{}{}
	}
	for key, value := range overrides {
		if _, ok := seen[key]; ok {
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func ident(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func sqlStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvIntList(key string, fallback []int) []int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		out = append(out, parsed)
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func getenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
