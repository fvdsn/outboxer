package outboxer

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadConfigUsesDefaults(t *testing.T) {
	unsetEnv(t, "EVENT_PAYLOAD", "EVENT_DESTINATION", "PG_HOST", "PG_USER", "HEALTH_PORT", "PORT", "POLL_INTERVAL_MS", "WATCHDOG_INTERVAL_MS")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.PGHost != "localhost" {
		t.Fatalf("expected default pg host, got %q", cfg.PGHost)
	}
	if cfg.PGUser != "postgres" {
		t.Fatalf("expected default pg user, got %q", cfg.PGUser)
	}
	if cfg.EventPayload != "payload" {
		t.Fatalf("expected default event payload column, got %q", cfg.EventPayload)
	}
	if cfg.EventDestination != "destination" {
		t.Fatalf("expected default event destination column, got %q", cfg.EventDestination)
	}
	if cfg.HealthPort != 0 {
		t.Fatalf("expected default healthcheck port 0, got %d", cfg.HealthPort)
	}
	if cfg.PollInterval != 0 {
		t.Fatalf("expected default poll interval 0, got %s", cfg.PollInterval)
	}
	if cfg.WatchdogInterval != 10*time.Minute {
		t.Fatalf("expected default watchdog interval 10m, got %s", cfg.WatchdogInterval)
	}
}

func TestLoadConfigVerifiesTLSByDefault(t *testing.T) {
	unsetEnv(t, "PG_SSL_REJECT_UNAUTHORIZED")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.PGSSLRejectUnauthorized {
		t.Fatal("expected TLS certificate verification to be enabled by default")
	}

	cfg, err = loadConfig([]string{"--pg-ssl-reject-unauthorized=false"}, io.Discard)
	if err != nil {
		t.Fatalf("load config with flag: %v", err)
	}
	if cfg.PGSSLRejectUnauthorized {
		t.Fatal("expected flag to disable TLS verification")
	}
}

func TestBuildTLSConfig(t *testing.T) {
	cfg := testConfig()
	cfg.PGHost = "db.example.com"
	cfg.PGSSLRejectUnauthorized = true

	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.ServerName != "db.example.com" {
		t.Fatalf("expected ServerName from host, got %q", tlsConfig.ServerName)
	}
	if tlsConfig.InsecureSkipVerify {
		t.Fatal("expected verification enabled when reject-unauthorized is true")
	}

	cfg.PGSSLRejectUnauthorized = false
	tlsConfig, err = buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Fatal("expected verification skipped when reject-unauthorized is false")
	}
}

func TestBuildTLSConfigRootCert(t *testing.T) {
	cfg := testConfig()

	cfg.PGSSLRootCert = filepath.Join(t.TempDir(), "missing.pem")
	if _, err := buildTLSConfig(cfg); err == nil {
		t.Fatal("expected error for a missing root cert file")
	}

	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(invalid, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write invalid cert: %v", err)
	}
	cfg.PGSSLRootCert = invalid
	if _, err := buildTLSConfig(cfg); err == nil {
		t.Fatal("expected error for a file with no certificates")
	}

	cfg.PGSSLRootCert = writeTestCACert(t)
	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("buildTLSConfig with valid cert: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("expected RootCAs to be set from the root cert")
	}
}

func TestOpenDBUsesSingleConnection(t *testing.T) {
	db, err := openDB(testConfig())
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("expected one max open connection, got %d", got)
	}
}

func TestLoadConfigUsesEnv(t *testing.T) {
	t.Setenv("PG_HOST", "db")
	t.Setenv("POLL_INTERVAL_MS", "250")
	t.Setenv("PORT", "9090")
	t.Setenv("HEALTH_PORT", "")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.PGHost != "db" {
		t.Fatalf("expected env pg host, got %q", cfg.PGHost)
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Fatalf("expected env poll interval, got %s", cfg.PollInterval)
	}
	if cfg.HealthPort != 9090 {
		t.Fatalf("expected PORT fallback, got %d", cfg.HealthPort)
	}
}

func TestLoadConfigHealthPortPrecedence(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("HEALTH_PORT", "9000")

	cfg, err := loadConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.HealthPort != 9000 {
		t.Fatalf("expected HEALTH_PORT to override PORT, got %d", cfg.HealthPort)
	}

	cfg, err = loadConfig([]string{"--health-port=0"}, io.Discard)
	if err != nil {
		t.Fatalf("load config with flag: %v", err)
	}
	if cfg.HealthPort != 0 {
		t.Fatalf("expected CLI flag to disable healthcheck server, got %d", cfg.HealthPort)
	}
}

func TestLoadConfigFlagsOverrideEnv(t *testing.T) {
	t.Setenv("EVENT_PAYLOAD", "env_payload")
	t.Setenv("EVENT_DESTINATION", "env_destination")
	t.Setenv("PG_HOST", "env-db")
	t.Setenv("PG_PORT", "5433")
	t.Setenv("PG_CONNECT_TIMEOUT_MS", "1000")
	t.Setenv("PG_SSL", "false")
	t.Setenv("POLL_INTERVAL_MS", "250")
	t.Setenv("WATCHDOG_INTERVAL_MS", "60000")

	cfg, err := loadConfig([]string{
		"--event-payload=flag_payload",
		"--event-destination=flag_destination",
		"--pg-host=flag-db",
		"--pg-port=6543",
		"--pg-connect-timeout-ms=2000",
		"--pg-ssl=true",
		"--poll-interval-ms=500",
		"--watchdog-interval-ms=30000",
	}, io.Discard)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.EventPayload != "flag_payload" {
		t.Fatalf("expected flag event payload column, got %q", cfg.EventPayload)
	}
	if cfg.EventDestination != "flag_destination" {
		t.Fatalf("expected flag event destination column, got %q", cfg.EventDestination)
	}
	if cfg.PGHost != "flag-db" {
		t.Fatalf("expected flag pg host, got %q", cfg.PGHost)
	}
	if cfg.PGPort != 6543 {
		t.Fatalf("expected flag pg port, got %d", cfg.PGPort)
	}
	if !cfg.PGSSL {
		t.Fatal("expected flag pg ssl to override env")
	}
	if cfg.PGConnectTimeout != 2*time.Second {
		t.Fatalf("expected flag pg connect timeout, got %s", cfg.PGConnectTimeout)
	}
	if cfg.PollInterval != 500*time.Millisecond {
		t.Fatalf("expected flag poll interval, got %s", cfg.PollInterval)
	}
	if cfg.WatchdogInterval != 30*time.Second {
		t.Fatalf("expected flag watchdog interval, got %s", cfg.WatchdogInterval)
	}
}

func TestLoadConfigHelpMentionsEnvVars(t *testing.T) {
	t.Setenv("PG_PASSWORD", "super-secret")

	var output bytes.Buffer
	_, err := loadConfig([]string{"--help"}, &output)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("expected flag.ErrHelp, got %v", err)
	}

	help := output.String()
	for _, expected := range []string{
		"Usage:",
		"Event table:",
		"Batch processing:",
		"HTTP / health:",
		"PostgreSQL:",
		"Google Pub/Sub:",
		"AWS SQS:",
		"--event-payload",
		"Env: EVENT_PAYLOAD",
		"--event-destination",
		"Env: EVENT_DESTINATION",
		"--health-port",
		"Env: HEALTH_PORT, PORT",
		"--pg-host",
		"Env: PG_HOST",
		"--pg-connect-timeout-ms",
		"Env: PG_CONNECT_TIMEOUT_MS",
		"--poll-interval-ms",
		"Env: POLL_INTERVAL_MS",
		"--watchdog-interval-ms",
		"Env: WATCHDOG_INTERVAL_MS",
		"--sqs-send-concurrency",
		"Env: SQS_SEND_CONCURRENCY",
		"--sqs-api-endpoint",
		"Env: SQS_API_ENDPOINT",
		"--ordered-group-batch-cap",
		"Env: ORDERED_GROUP_BATCH_CAP",
		"--publish-result-grace-ms",
		"Env: PUBLISH_RESULT_GRACE_MS",
		"--aws-role-session-name",
		"Env: AWS_ROLE_SESSION_NAME",
	} {
		if !strings.Contains(help, expected) {
			t.Fatalf("expected help to contain %q, got:\n%s", expected, help)
		}
	}
	if strings.Contains(help, "super-secret") {
		t.Fatalf("expected help to redact database password, got:\n%s", help)
	}
	if !strings.Contains(help, "Env: PG_PASSWORD") || !strings.Contains(help, "Default: <set>") {
		t.Fatalf("expected help to mention redacted pg password default, got:\n%s", help)
	}
}

func TestValidateRequiresAnEnabledBackend(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = false
	cfg.SQSEnabled = false
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when no backend is enabled")
	}

	cfg.PubSubEnabled = true
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected single enabled backend to be valid, got %v", err)
	}
}

func TestValidateRequiresTargetColumnWhenBothEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = true
	cfg.SQSEnabled = true

	cfg.EventTarget = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when both backends are enabled without a target column")
	}

	cfg.EventTarget = "target"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected both backends with a target column to be valid, got %v", err)
	}
}

func TestValidateRequiresIDAndPayloadColumns(t *testing.T) {
	cfg := testConfig()
	cfg.EventID = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when id column is empty")
	}

	cfg = testConfig()
	cfg.EventPayload = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when payload column is empty")
	}
}

func TestValidateRequiresDestinationOrDefault(t *testing.T) {
	cfg := testConfig()
	cfg.PubSubEnabled = true
	cfg.SQSEnabled = false
	cfg.EventDestination = ""
	cfg.DefaultPubSubTopic = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when Pub/Sub has neither a destination column nor a default topic")
	}
	cfg.DefaultPubSubTopic = "default"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected a default topic to satisfy Pub/Sub destination, got %v", err)
	}

	cfg = testConfig()
	cfg.PubSubEnabled = false
	cfg.SQSEnabled = true
	cfg.EventDestination = ""
	cfg.DefaultSQSQueueURL = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when SQS has neither a destination column nor a default queue URL")
	}
	cfg.DefaultSQSQueueURL = "https://sqs.example/q"
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected a default queue URL to satisfy SQS destination, got %v", err)
	}
}

func TestCheckRequiredColumns(t *testing.T) {
	base := testConfig()

	cases := []struct {
		name    string
		mutate  func(*appConfig)
		columns []string
		wantErr bool
	}{
		{"both enabled needs target and destination", nil, []string{"id", "payload", "target", "destination"}, false},
		{"missing id", nil, []string{"payload", "target", "destination"}, true},
		{"missing target when both enabled", nil, []string{"id", "payload", "destination"}, true},
		{
			"destination optional once both defaults cover it",
			func(c *appConfig) { c.DefaultPubSubTopic = "default"; c.DefaultSQSQueueURL = "https://sqs.example/q" },
			[]string{"id", "payload", "target"},
			false,
		},
		{
			"pubsub only without default needs destination",
			func(c *appConfig) { c.SQSEnabled = false; c.DefaultPubSubTopic = "" },
			[]string{"id", "payload"},
			true,
		},
		{
			"pubsub only with default skips optional columns",
			func(c *appConfig) { c.SQSEnabled = false },
			[]string{"id", "payload"},
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			a := &app{cfg: cfg}
			err := a.checkRequiredColumns(tc.columns)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidateAWSWebIdentity(t *testing.T) {
	base := testConfig()
	base.AWSWebIdentityProvider = "google"
	base.AWSWebIdentityAudience = "//iam.example/aws"
	base.AWSRoleARN = "arn:aws:iam::123456789012:role/outboxer"

	if err := base.validate(); err != nil {
		t.Fatalf("expected a fully configured google web identity to be valid, got %v", err)
	}

	unsupported := base
	unsupported.AWSWebIdentityProvider = "azure"
	if err := unsupported.validate(); err == nil {
		t.Fatal("expected error for an unsupported web identity provider")
	}

	noRole := base
	noRole.AWSRoleARN = ""
	if err := noRole.validate(); err == nil {
		t.Fatal("expected error when web identity is set without a role ARN")
	}

	noAudience := base
	noAudience.AWSWebIdentityAudience = ""
	if err := noAudience.validate(); err == nil {
		t.Fatal("expected error when web identity is set without an audience")
	}

	off := testConfig()
	if err := off.validate(); err != nil {
		t.Fatalf("expected web identity to be optional, got %v", err)
	}
}

func TestValidateWatchdogMustExceedPollInterval(t *testing.T) {
	cfg := testConfig()
	cfg.PollInterval = time.Minute
	cfg.WatchdogInterval = 5 * time.Minute
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when watchdog interval is less than 10x the poll interval")
	}

	cfg.WatchdogInterval = 10 * time.Minute
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected watchdog interval of exactly 10x poll interval to be valid, got %v", err)
	}

	// A zero poll interval (the default hot loop) imposes no constraint.
	cfg.PollInterval = 0
	cfg.WatchdogInterval = time.Hour
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected zero poll interval to skip the watchdog check, got %v", err)
	}
}

func TestValidateWatchdogMustExceedBatchSendBound(t *testing.T) {
	cfg := testConfig()
	cfg.WatchdogInterval = cfg.batchSendBound()
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when watchdog interval does not exceed batch send bound")
	}

	cfg.WatchdogInterval = cfg.batchSendBound() + time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected watchdog interval over batch send bound to be valid, got %v", err)
	}
}

func TestValidateRequiresPositivePublishTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*appConfig)
	}{
		{"both backends", func(*appConfig) {}},
		{"pubsub only", func(cfg *appConfig) { cfg.SQSEnabled = false }},
		{"sqs only", func(cfg *appConfig) { cfg.PubSubEnabled = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.edit(&cfg)
			cfg.PublishTimeout = 0
			if err := cfg.validate(); err == nil {
				t.Fatal("expected error when publish timeout is zero")
			}

			cfg.PublishTimeout = -time.Millisecond
			if err := cfg.validate(); err == nil {
				t.Fatal("expected error when publish timeout is negative")
			}
		})
	}
}

func TestValidateRequiresNonNegativePublishResultGrace(t *testing.T) {
	cfg := testConfig()
	cfg.PublishResultGrace = -time.Millisecond
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when publish result grace is negative")
	}
}

func TestValidateRequiresPositiveOrderedGroupBatchCap(t *testing.T) {
	cfg := testConfig()
	cfg.OrderedGroupBatchCap = 0
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when ordered group batch cap is zero")
	}
}

func TestValidateRequiresPositiveSQSConcurrencyWhenSQSEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.SQSSendConcurrency = 0
	if err := cfg.validate(); err == nil {
		t.Fatal("expected error when SQS concurrency is zero and SQS is enabled")
	}

	cfg.SQSEnabled = false
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected SQS concurrency to be ignored when SQS is disabled, got %v", err)
	}
}

func TestBatchSendBound(t *testing.T) {
	cfg := testConfig()
	cfg.BatchSize = 32
	cfg.SQSSendConcurrency = 8
	cfg.OrderedGroupBatchCap = 8
	cfg.PublishTimeout = 30 * time.Second
	cfg.PublishResultGrace = 5 * time.Second

	if got, want := cfg.batchSendBound(), 8*35*time.Second; got != want {
		t.Fatalf("both backend bound = %s, want %s", got, want)
	}

	cfg.PubSubEnabled = false
	if got, want := cfg.batchSendBound(), 8*30*time.Second; got != want {
		t.Fatalf("SQS-only bound = %s, want %s", got, want)
	}

	cfg.OrderedGroupBatchCap = 1
	if got, want := cfg.batchSendBound(), 4*30*time.Second; got != want {
		t.Fatalf("SQS size-split bound = %s, want %s", got, want)
	}
	cfg.OrderedGroupBatchCap = 8

	cfg.PubSubEnabled = true
	cfg.SQSEnabled = false
	if got, want := cfg.batchSendBound(), 8*35*time.Second; got != want {
		t.Fatalf("Pub/Sub-only bound = %s, want %s", got, want)
	}
}

func TestSleepContextReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	sleepContext(ctx, time.Hour)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepContext blocked for %s on a cancelled context", elapsed)
	}
}

func TestDeadlockDetectorConcurrentAccess(_ *testing.T) {
	// Exercises the watchdog counter from two goroutines so the race detector
	// would flag a regression back to an unsynchronized int64.
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				deadlockDetector.Store(randomInt63())
			}
		}
	}()

	var previous int64
	for i := 0; i < 100000; i++ {
		previous = deadlockDetector.Load()
	}
	_ = previous

	close(stop)
	wg.Wait()
}

func TestFailureLoggerRateLimitsBySignature(t *testing.T) {
	now := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)
	window := 2 * time.Minute
	logger := newFailureLogger(window)
	logger.now = func() time.Time { return now }

	if ok, suppressed := logger.shouldLog("destination-a|retryable"); !ok || suppressed != 0 {
		t.Fatalf("first occurrence = (%t, %d), want (true, 0)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); ok || suppressed != 0 {
		t.Fatalf("second occurrence = (%t, %d), want (false, 0)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|permission"); !ok || suppressed != 0 {
		t.Fatalf("different signature = (%t, %d), want (true, 0)", ok, suppressed)
	}

	now = now.Add(window + time.Nanosecond)
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); !ok || suppressed != 1 {
		t.Fatalf("post-window occurrence = (%t, %d), want (true, 1)", ok, suppressed)
	}
	if ok, suppressed := logger.shouldLog("destination-a|retryable"); ok || suppressed != 0 {
		t.Fatalf("post-summary repeat = (%t, %d), want (false, 0)", ok, suppressed)
	}
}

func TestFailureLoggerSkipsContextCancellationFallout(t *testing.T) {
	logger := newFailureLogger(time.Minute)
	a := &app{failureLogger: logger}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a.logFailure(ctx, "should be skipped", "signature")
	if len(logger.entries) != 0 {
		t.Fatalf("expected canceled failure log to be skipped, got %#v", logger.entries)
	}
}

func TestHealthcheckReturnsOK(t *testing.T) {
	a := &app{cfg: testConfig()}
	server := a.newHTTPServer()

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if string(body) != "all good" {
		t.Fatalf("expected healthcheck body, got %q", string(body))
	}
}
