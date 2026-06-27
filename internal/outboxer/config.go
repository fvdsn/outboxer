package outboxer

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type appConfig struct {
	EventTable       string
	EventID          string
	EventTimestamp   string
	EventPayload     string
	EventTarget      string
	EventDestination string
	EventOptions     string

	CollectBatchTarget int
	SQSSendConcurrency int
	DLQTable           string
	NotifyChannel      string

	LogLevel  string
	LogFormat string

	WatchdogInterval   time.Duration
	HealthPort         int
	PubSubEnabled      bool
	SQSEnabled         bool
	DefaultPubSubTopic string
	DefaultSQSQueueURL string
	PubSubDestinations []string
	SQSDestinations    []string
	PubSubProjectID    string
	PubSubAPIEndpoint  string
	SQSAPIEndpoint     string
	ErrorCooldown      time.Duration
	PollInterval       time.Duration
	PublishTimeout     time.Duration
	PublishResultGrace time.Duration
	MaxEventAge        time.Duration
	StatsInterval      time.Duration

	PGHost                  string
	PGPort                  uint16
	PGUser                  string
	PGPassword              string
	PGDatabase              string
	PGSSL                   bool
	PGSSLRejectUnauthorized bool
	PGSSLRootCert           string
	PGConnectTimeout        time.Duration
	PGQueryTimeout          time.Duration

	// Provisioning-only settings, used by the init command. PGInitUser/Password
	// are the connection identity for `init --apply`; their presence enables
	// run-role management. PGProducerRoles are existing roles granted SELECT,
	// INSERT on the event table.
	PGInitUser      string
	PGInitPassword  string
	PGProducerRoles []string

	AWSRegion                  string
	AWSRoleARN                 string
	AWSRoleSessionName         string
	AWSRoleDuration            time.Duration
	AWSCredentialRefreshWindow time.Duration
	AWSWebIdentityProvider     string
	AWSWebIdentityAudience     string
}

type cliOption struct {
	category string
	name     string
	usage    string
}

func loadConfig(args []string, output io.Writer) (appConfig, error) {
	loadDotEnv(".env")
	cfg := loadConfigFromEnv()

	flags := flag.NewFlagSet("outboxer", flag.ContinueOnError)
	flags.SetOutput(output)
	options := []cliOption{}
	flags.Usage = func() {
		printUsage(output, options)
	}

	finalize := bindConfigFlags(flags, &cfg, &options)

	if err := flags.Parse(args); err != nil {
		return appConfig{}, err
	}
	finalize()
	if flags.NArg() > 0 {
		return appConfig{}, fmt.Errorf("unexpected argument: %q", flags.Arg(0))
	}

	return cfg, nil
}

// loadInitConfig parses the init command's arguments. It reuses every relay
// flag/env so a single configuration drives both verbs, and adds the
// provisioning-only flags (--apply and the PG_INIT_*/PG_PRODUCER_ROLES settings).
func loadInitConfig(args []string, output io.Writer) (appConfig, bool, error) {
	loadDotEnv(".env")
	cfg := loadConfigFromEnv()

	flags := flag.NewFlagSet("outboxer init", flag.ContinueOnError)
	flags.SetOutput(output)
	options := []cliOption{}
	flags.Usage = func() {
		printUsage(output, options)
	}

	finalize := bindConfigFlags(flags, &cfg, &options)

	var apply bool
	flags.BoolVar(&apply, "apply", false, "Execute the generated SQL against the database instead of printing it to stdout.")
	options = append(options, cliOption{category: "Provisioning", name: "apply", usage: "Execute the generated SQL against the database instead of printing it to stdout."})

	addStringFlag(flags, &options, "Provisioning", &cfg.PGInitUser, "pg-init-user", cfg.PGInitUser, "Provisioning role to connect as for --apply. When set, init also creates and grants to the run role.", "PG_INIT_USER")
	addValueFlag(flags, &options, "Provisioning", newSecretStringValue(&cfg.PGInitPassword), "pg-init-password", "Password for the provisioning role.", "PG_INIT_PASSWORD", redactDefault(cfg.PGInitPassword))
	producerRoles := strings.Join(cfg.PGProducerRoles, ",")
	addStringFlag(flags, &options, "Provisioning", &producerRoles, "pg-producer-roles", producerRoles, "Comma-separated existing roles granted SELECT, INSERT on the event table.", "PG_PRODUCER_ROLES")

	if err := flags.Parse(args); err != nil {
		return appConfig{}, false, err
	}
	finalize()
	cfg.PGProducerRoles = parseStringList(producerRoles)
	if flags.NArg() > 0 {
		return appConfig{}, false, fmt.Errorf("unexpected argument: %q", flags.Arg(0))
	}

	return cfg, apply, nil
}

// bindConfigFlags registers every relay configuration flag on the flag set and
// returns a finalize function that must be called after Parse to convert the
// millisecond/list scratch values back onto the config.
func bindConfigFlags(flags *flag.FlagSet, cfg *appConfig, options *[]cliOption) func() {
	addStringFlag(flags, options, "Event table", &cfg.EventTable, "event-table", cfg.EventTable, "Outbox table name.", "EVENT_TABLE")
	addStringFlag(flags, options, "Event table", &cfg.EventID, "event-id", cfg.EventID, "Event id column.", "EVENT_ID")
	addStringFlag(flags, options, "Event table", &cfg.EventTimestamp, "event-timestamp", cfg.EventTimestamp, "Event timestamp column.", "EVENT_TIMESTAMP")
	addStringFlag(flags, options, "Event table", &cfg.EventPayload, "event-payload", cfg.EventPayload, "Event payload column.", "EVENT_PAYLOAD")
	addStringFlag(flags, options, "Event table", &cfg.EventTarget, "event-target", cfg.EventTarget, "Backend selector column. Values pubsub or sqs.", "EVENT_TARGET")
	addStringFlag(flags, options, "Event table", &cfg.EventDestination, "event-destination", cfg.EventDestination, "Pub/Sub topic name or SQS queue URL column.", "EVENT_DESTINATION")
	addStringFlag(flags, options, "Event table", &cfg.EventOptions, "event-options", cfg.EventOptions, "Backend-specific JSON options column. Empty disables options.", "EVENT_OPTIONS")

	addIntFlag(flags, options, "Batch processing", &cfg.CollectBatchTarget, "collect-batch-target", cfg.CollectBatchTarget, "Approximate target rows selected per batch, spread across eligible routes.", "COLLECT_BATCH_TARGET")
	addIntFlag(flags, options, "Batch processing", &cfg.SQSSendConcurrency, "sqs-send-concurrency", cfg.SQSSendConcurrency, "Maximum concurrent SQS send requests.", "SQS_SEND_CONCURRENCY")
	addStringFlag(flags, options, "Batch processing", &cfg.DLQTable, "dlq-table", cfg.DLQTable, "Dead letter table for poison events. Empty disables DLQ.", "DLQ_TABLE")

	var watchdogIntervalMS = int(cfg.WatchdogInterval / time.Millisecond)
	var errorCooldownMS = int(cfg.ErrorCooldown / time.Millisecond)
	var pollIntervalMS = int(cfg.PollInterval / time.Millisecond)
	var publishTimeoutMS = int(cfg.PublishTimeout / time.Millisecond)
	var publishResultGraceMS = int(cfg.PublishResultGrace / time.Millisecond)
	var maxEventAgeMS = int(cfg.MaxEventAge / time.Millisecond)
	var statsIntervalMS = int(cfg.StatsInterval / time.Millisecond)
	var pgTimeoutMS = int(cfg.PGConnectTimeout / time.Millisecond)
	var pgQueryTimeoutMS = int(cfg.PGQueryTimeout / time.Millisecond)
	var awsRoleDurationSeconds = int(cfg.AWSRoleDuration / time.Second)
	var awsCredentialRefreshWindowMS = int(cfg.AWSCredentialRefreshWindow / time.Millisecond)
	var pubsubDestinations = strings.Join(cfg.PubSubDestinations, ",")
	var sqsDestinations = strings.Join(cfg.SQSDestinations, ",")

	addIntFlag(flags, options, "Batch processing", &errorCooldownMS, "error-cooldown-ms", errorCooldownMS, "Sleep after batch or database errors in milliseconds.", "ERROR_COOLDOWN_MS")
	addIntFlag(flags, options, "Batch processing", &pollIntervalMS, "poll-interval-ms", pollIntervalMS, "Sleep after an empty batch in milliseconds.", "POLL_INTERVAL_MS")
	addIntFlag(flags, options, "Batch processing", &watchdogIntervalMS, "watchdog-interval-ms", watchdogIntervalMS, "Watchdog interval in milliseconds.", "WATCHDOG_INTERVAL_MS")
	addIntFlag(flags, options, "Batch processing", &publishTimeoutMS, "publish-timeout-ms", publishTimeoutMS, "Timeout for a single publish call in milliseconds. Must be positive.", "PUBLISH_TIMEOUT_MS")
	addIntFlag(flags, options, "Batch processing", &publishResultGraceMS, "publish-result-grace-ms", publishResultGraceMS, "Extra wait after provider publish timeout for async publish results.", "PUBLISH_RESULT_GRACE_MS")
	addIntFlag(flags, options, "Batch processing", &maxEventAgeMS, "max-event-age-ms", maxEventAgeMS, "Maximum selected event age in milliseconds. 0 disables age-based poison.", "MAX_EVENT_AGE_MS")
	addIntFlag(flags, options, "Batch processing", &statsIntervalMS, "stats-interval-ms", statsIntervalMS, "Periodic statistics logging interval in milliseconds. 0 disables statistics.", "STATS_INTERVAL_MS")
	addStringFlag(flags, options, "Batch processing", &cfg.NotifyChannel, "notify-channel", cfg.NotifyChannel, "PostgreSQL LISTEN channel for the optional new-event notification trigger. Only used when POLL_INTERVAL_MS > 0.", "NOTIFY_CHANNEL")

	addIntFlag(flags, options, "HTTP / health", &cfg.HealthPort, "health-port", cfg.HealthPort, "HTTP health server port. Set to 0 to disable.", "HEALTH_PORT, PORT")

	addStringFlag(flags, options, "Logging", &cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, or error.", "LOG_LEVEL")
	addStringFlag(flags, options, "Logging", &cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: text or json.", "LOG_FORMAT")

	addStringFlag(flags, options, "PostgreSQL", &cfg.PGHost, "pg-host", cfg.PGHost, "PostgreSQL host.", "PG_HOST")
	addValueFlag(flags, options, "PostgreSQL", (*uint16Value)(&cfg.PGPort), "pg-port", "PostgreSQL port.", "PG_PORT", cfg.PGPort)
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGUser, "pg-user", cfg.PGUser, "PostgreSQL user.", "PG_USER")
	addValueFlag(flags, options, "PostgreSQL", newSecretStringValue(&cfg.PGPassword), "pg-password", "PostgreSQL password.", "PG_PASSWORD", redactDefault(cfg.PGPassword))
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGDatabase, "pg-database", cfg.PGDatabase, "PostgreSQL database.", "PG_DATABASE")
	addBoolFlag(flags, options, "PostgreSQL", &cfg.PGSSL, "pg-ssl", cfg.PGSSL, "Enable PostgreSQL TLS.", "PG_SSL")
	addBoolFlag(flags, options, "PostgreSQL", &cfg.PGSSLRejectUnauthorized, "pg-ssl-reject-unauthorized", cfg.PGSSLRejectUnauthorized, "Verify PostgreSQL TLS certificate and hostname.", "PG_SSL_REJECT_UNAUTHORIZED")
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGSSLRootCert, "pg-ssl-root-cert", cfg.PGSSLRootCert, "Path to a CA certificate (PEM) used to verify the PostgreSQL server.", "PG_SSL_ROOT_CERT")
	addIntFlag(flags, options, "PostgreSQL", &pgTimeoutMS, "pg-connect-timeout-ms", pgTimeoutMS, "PostgreSQL connect timeout in milliseconds.", "PG_CONNECT_TIMEOUT_MS")
	addIntFlag(flags, options, "PostgreSQL", &pgQueryTimeoutMS, "pg-query-timeout-ms", pgQueryTimeoutMS, "Timeout for a single database query in milliseconds. 0 disables the timeout.", "PG_QUERY_TIMEOUT_MS")

	addBoolFlag(flags, options, "Google Pub/Sub", &cfg.PubSubEnabled, "pubsub-enabled", cfg.PubSubEnabled, "Enable publishing to Google Pub/Sub.", "PUBSUB_ENABLED")
	addStringFlag(flags, options, "Google Pub/Sub", &cfg.DefaultPubSubTopic, "default-pubsub-topic", cfg.DefaultPubSubTopic, "Pub/Sub topic used when an event has no destination.", "DEFAULT_PUBSUB_TOPIC")
	addStringFlag(flags, options, "Google Pub/Sub", &pubsubDestinations, "pubsub-destinations", pubsubDestinations, "Comma-separated Pub/Sub destinations this process owns. Empty means all Pub/Sub destinations.", "PUBSUB_DESTINATIONS")
	addStringFlag(flags, options, "Google Pub/Sub", &cfg.PubSubProjectID, "pubsub-project-id", cfg.PubSubProjectID, "Google Cloud project for Pub/Sub. Detected from ADC when empty.", "PUBSUB_PROJECT_ID")
	addStringFlag(flags, options, "Google Pub/Sub", &cfg.PubSubAPIEndpoint, "pubsub-api-endpoint", cfg.PubSubAPIEndpoint, "Optional Pub/Sub API endpoint override.", "PUBSUB_API_ENDPOINT")

	addBoolFlag(flags, options, "AWS SQS", &cfg.SQSEnabled, "sqs-enabled", cfg.SQSEnabled, "Enable publishing to AWS SQS.", "SQS_ENABLED")
	addStringFlag(flags, options, "AWS SQS", &cfg.DefaultSQSQueueURL, "default-sqs-queue-url", cfg.DefaultSQSQueueURL, "SQS queue URL used when an event has no destination.", "DEFAULT_SQS_QUEUE_URL")
	addStringFlag(flags, options, "AWS SQS", &sqsDestinations, "sqs-destinations", sqsDestinations, "Comma-separated SQS destinations this process owns. Empty means all SQS destinations.", "SQS_DESTINATIONS")
	addStringFlag(flags, options, "AWS SQS", &cfg.SQSAPIEndpoint, "sqs-api-endpoint", cfg.SQSAPIEndpoint, "Optional SQS API endpoint override.", "SQS_API_ENDPOINT")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRegion, "aws-region", cfg.AWSRegion, "AWS region for SQS and STS.", "AWS_REGION")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRoleARN, "aws-role-arn", cfg.AWSRoleARN, "Optional AWS role to assume before publishing to SQS.", "AWS_ROLE_ARN")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRoleSessionName, "aws-role-session-name", cfg.AWSRoleSessionName, "AWS assume-role session name.", "AWS_ROLE_SESSION_NAME")
	addIntFlag(flags, options, "AWS SQS", &awsRoleDurationSeconds, "aws-role-duration-seconds", awsRoleDurationSeconds, "AWS assumed-role duration in seconds.", "AWS_ROLE_DURATION_SECONDS")
	addIntFlag(flags, options, "AWS SQS", &awsCredentialRefreshWindowMS, "aws-credential-refresh-window-ms", awsCredentialRefreshWindowMS, "Refresh assumed credentials before expiry in milliseconds.", "AWS_CREDENTIAL_REFRESH_WINDOW_MS")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSWebIdentityProvider, "aws-web-identity-provider", cfg.AWSWebIdentityProvider, "Set to 'google' to assume the AWS role with a Google OIDC token (GCP to AWS).", "AWS_WEB_IDENTITY_PROVIDER")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSWebIdentityAudience, "aws-web-identity-audience", cfg.AWSWebIdentityAudience, "Audience for the web identity token, matching the AWS IAM OIDC provider.", "AWS_WEB_IDENTITY_AUDIENCE")

	return func() {
		cfg.WatchdogInterval = time.Duration(watchdogIntervalMS) * time.Millisecond
		cfg.ErrorCooldown = time.Duration(errorCooldownMS) * time.Millisecond
		cfg.PollInterval = time.Duration(pollIntervalMS) * time.Millisecond
		cfg.PublishTimeout = time.Duration(publishTimeoutMS) * time.Millisecond
		cfg.PublishResultGrace = time.Duration(publishResultGraceMS) * time.Millisecond
		cfg.MaxEventAge = time.Duration(maxEventAgeMS) * time.Millisecond
		cfg.StatsInterval = time.Duration(statsIntervalMS) * time.Millisecond
		cfg.PGConnectTimeout = time.Duration(pgTimeoutMS) * time.Millisecond
		cfg.PGQueryTimeout = time.Duration(pgQueryTimeoutMS) * time.Millisecond
		cfg.AWSRoleDuration = time.Duration(awsRoleDurationSeconds) * time.Second
		cfg.AWSCredentialRefreshWindow = time.Duration(awsCredentialRefreshWindowMS) * time.Millisecond
		cfg.PubSubDestinations = parseStringList(pubsubDestinations)
		cfg.SQSDestinations = parseStringList(sqsDestinations)
	}
}

func (cfg appConfig) validate() error {
	if cfg.EventTable == "" {
		return fmt.Errorf("an event table is required: set EVENT_TABLE")
	}
	if cfg.EventID == "" {
		return fmt.Errorf("an id column is required: set EVENT_ID")
	}
	if cfg.EventPayload == "" {
		return fmt.Errorf("a payload column is required: set EVENT_PAYLOAD")
	}
	if !cfg.PubSubEnabled && !cfg.SQSEnabled {
		return fmt.Errorf("no publishing backend enabled: set PUBSUB_ENABLED=true and/or SQS_ENABLED=true")
	}
	if cfg.PubSubEnabled && cfg.SQSEnabled && cfg.EventTarget == "" {
		return fmt.Errorf("a target column is required when both Pub/Sub and SQS are enabled: set EVENT_TARGET")
	}
	if cfg.PubSubEnabled && cfg.DefaultPubSubTopic == "" && cfg.EventDestination == "" {
		return fmt.Errorf("Pub/Sub needs a destination: set EVENT_DESTINATION or DEFAULT_PUBSUB_TOPIC")
	}
	if cfg.SQSEnabled && cfg.DefaultSQSQueueURL == "" && cfg.EventDestination == "" {
		return fmt.Errorf("SQS needs a destination: set EVENT_DESTINATION or DEFAULT_SQS_QUEUE_URL")
	}
	if cfg.DLQTable != "" && cfg.DLQTable == cfg.EventTable {
		return fmt.Errorf("DLQ_TABLE must not equal EVENT_TABLE")
	}
	if !cfg.PubSubEnabled && len(cfg.PubSubDestinations) > 0 {
		return fmt.Errorf("PUBSUB_DESTINATIONS requires PUBSUB_ENABLED=true")
	}
	if !cfg.SQSEnabled && len(cfg.SQSDestinations) > 0 {
		return fmt.Errorf("SQS_DESTINATIONS requires SQS_ENABLED=true")
	}
	if cfg.CollectBatchTarget <= 0 {
		return fmt.Errorf("batch collection target (%d) must be positive: set COLLECT_BATCH_TARGET", cfg.CollectBatchTarget)
	}
	if cfg.PublishTimeout <= 0 {
		return fmt.Errorf("publish timeout (%s) must be positive: set PUBLISH_TIMEOUT_MS", cfg.PublishTimeout)
	}
	if cfg.PublishResultGrace < 0 {
		return fmt.Errorf("publish result grace (%s) must not be negative: set PUBLISH_RESULT_GRACE_MS", cfg.PublishResultGrace)
	}
	if cfg.MaxEventAge < 0 {
		return fmt.Errorf("max event age (%s) must not be negative: set MAX_EVENT_AGE_MS", cfg.MaxEventAge)
	}
	if cfg.MaxEventAge > 0 && cfg.EventTimestamp == "" {
		return fmt.Errorf("MAX_EVENT_AGE_MS requires an event timestamp column: set EVENT_TIMESTAMP")
	}
	if cfg.StatsInterval < 0 {
		return fmt.Errorf("stats interval (%s) must not be negative: set STATS_INTERVAL_MS", cfg.StatsInterval)
	}
	if cfg.SQSEnabled && cfg.SQSSendConcurrency <= 0 {
		return fmt.Errorf("SQS send concurrency (%d) must be positive: set SQS_SEND_CONCURRENCY", cfg.SQSSendConcurrency)
	}
	if cfg.PollInterval > 0 && cfg.WatchdogInterval < 10*cfg.PollInterval {
		return fmt.Errorf("watchdog interval (%s) must be at least 10x the poll interval (%s) to avoid false deadlocks: increase WATCHDOG_INTERVAL_MS or decrease POLL_INTERVAL_MS", cfg.WatchdogInterval, cfg.PollInterval)
	}
	if cfg.PollInterval > 0 && cfg.NotifyChannel == "" {
		return fmt.Errorf("notify channel must not be empty when polling is enabled: set NOTIFY_CHANNEL or POLL_INTERVAL_MS=0")
	}
	if cfg.AWSWebIdentityProvider != "" {
		if cfg.AWSWebIdentityProvider != awsWebIdentityProviderGoogle {
			return fmt.Errorf("unsupported AWS_WEB_IDENTITY_PROVIDER %q: the only supported value is %q", cfg.AWSWebIdentityProvider, awsWebIdentityProviderGoogle)
		}
		if cfg.AWSRoleARN == "" {
			return fmt.Errorf("AWS_WEB_IDENTITY_PROVIDER requires AWS_ROLE_ARN (the role to assume with the web identity token)")
		}
		if cfg.AWSWebIdentityAudience == "" {
			return fmt.Errorf("AWS_WEB_IDENTITY_PROVIDER requires AWS_WEB_IDENTITY_AUDIENCE")
		}
	}
	return nil
}

func loadConfigFromEnv() appConfig {
	return appConfig{
		EventTable:       getenv("EVENT_TABLE", "events"),
		EventID:          getenv("EVENT_ID", "id"),
		EventTimestamp:   getenv("EVENT_TIMESTAMP", "timestamp"),
		EventPayload:     getenv("EVENT_PAYLOAD", "payload"),
		EventTarget:      getenv("EVENT_TARGET", "target"),
		EventDestination: getenv("EVENT_DESTINATION", "destination"),
		EventOptions:     getenv("EVENT_OPTIONS", "options"),

		CollectBatchTarget: getenvInt("COLLECT_BATCH_TARGET", 5000),
		SQSSendConcurrency: getenvInt("SQS_SEND_CONCURRENCY", 8),
		DLQTable:           getenv("DLQ_TABLE", ""),
		NotifyChannel:      getenv("NOTIFY_CHANNEL", "outboxer_events"),

		LogLevel:  getenv("LOG_LEVEL", "info"),
		LogFormat: getenv("LOG_FORMAT", "text"),

		WatchdogInterval:   time.Duration(getenvInt("WATCHDOG_INTERVAL_MS", 10*60*1000)) * time.Millisecond,
		HealthPort:         getenvInt("HEALTH_PORT", getenvInt("PORT", 0)),
		PubSubEnabled:      os.Getenv("PUBSUB_ENABLED") == "true",
		SQSEnabled:         os.Getenv("SQS_ENABLED") == "true",
		DefaultPubSubTopic: getenv("DEFAULT_PUBSUB_TOPIC", "default"),
		DefaultSQSQueueURL: getenv("DEFAULT_SQS_QUEUE_URL", ""),
		PubSubDestinations: parseStringList(getenv("PUBSUB_DESTINATIONS", "")),
		SQSDestinations:    parseStringList(getenv("SQS_DESTINATIONS", "")),
		PubSubProjectID:    getenv("PUBSUB_PROJECT_ID", ""),
		PubSubAPIEndpoint:  getenv("PUBSUB_API_ENDPOINT", ""),
		SQSAPIEndpoint:     getenv("SQS_API_ENDPOINT", ""),
		ErrorCooldown:      time.Duration(getenvInt("ERROR_COOLDOWN_MS", 5000)) * time.Millisecond,
		PollInterval:       time.Duration(getenvInt("POLL_INTERVAL_MS", 0)) * time.Millisecond,
		PublishTimeout:     time.Duration(getenvInt("PUBLISH_TIMEOUT_MS", 30000)) * time.Millisecond,
		PublishResultGrace: time.Duration(getenvInt("PUBLISH_RESULT_GRACE_MS", 5000)) * time.Millisecond,
		MaxEventAge:        time.Duration(getenvInt("MAX_EVENT_AGE_MS", 0)) * time.Millisecond,
		StatsInterval:      time.Duration(getenvInt("STATS_INTERVAL_MS", 10000)) * time.Millisecond,

		PGHost:                  getenv("PG_HOST", "localhost"),
		PGPort:                  uint16(getenvInt("PG_PORT", 5432)),
		PGUser:                  getenv("PG_USER", "postgres"),
		PGPassword:              getenv("PG_PASSWORD", ""),
		PGDatabase:              getenv("PG_DATABASE", "postgres"),
		PGSSL:                   getenvBool("PG_SSL", false),
		PGSSLRejectUnauthorized: getenvBool("PG_SSL_REJECT_UNAUTHORIZED", true),
		PGSSLRootCert:           getenv("PG_SSL_ROOT_CERT", ""),
		PGConnectTimeout:        time.Duration(getenvInt("PG_CONNECT_TIMEOUT_MS", 10000)) * time.Millisecond,
		PGQueryTimeout:          time.Duration(getenvInt("PG_QUERY_TIMEOUT_MS", 30000)) * time.Millisecond,

		PGInitUser:      getenv("PG_INIT_USER", ""),
		PGInitPassword:  getenv("PG_INIT_PASSWORD", ""),
		PGProducerRoles: parseStringList(getenv("PG_PRODUCER_ROLES", "")),

		AWSRegion:                  getenv("AWS_REGION", ""),
		AWSRoleARN:                 getenv("AWS_ROLE_ARN", ""),
		AWSRoleSessionName:         getenv("AWS_ROLE_SESSION_NAME", "outboxer"),
		AWSRoleDuration:            time.Duration(getenvInt("AWS_ROLE_DURATION_SECONDS", 3600)) * time.Second,
		AWSCredentialRefreshWindow: time.Duration(getenvInt("AWS_CREDENTIAL_REFRESH_WINDOW_MS", 5*60*1000)) * time.Millisecond,
		AWSWebIdentityProvider:     getenv("AWS_WEB_IDENTITY_PROVIDER", ""),
		AWSWebIdentityAudience:     getenv("AWS_WEB_IDENTITY_AUDIENCE", ""),
	}
}

func parseStringList(value string) []string {
	parts := strings.Split(value, ",")
	out := []string{}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func optionHelp(description string, envVar string, defaultValue any) string {
	return fmt.Sprintf("%s Env: %s. Default: %v.", description, envVar, defaultValue)
}

func addStringFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *string, name string, value string, description string, envVar string) {
	usage := optionHelp(description, envVar, value)
	flags.StringVar(destination, name, value, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage})
}

func addIntFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *int, name string, value int, description string, envVar string) {
	usage := optionHelp(description, envVar, value)
	flags.IntVar(destination, name, value, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage})
}

func addBoolFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *bool, name string, value bool, description string, envVar string) {
	usage := optionHelp(description, envVar, value)
	flags.BoolVar(destination, name, value, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage})
}

func addValueFlag(flags *flag.FlagSet, options *[]cliOption, category string, value flag.Value, name string, description string, envVar string, defaultValue any) {
	usage := optionHelp(description, envVar, defaultValue)
	flags.Var(value, name, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage})
}

func printUsage(output io.Writer, options []cliOption) {
	_, _ = fmt.Fprintln(output, "Usage:")
	_, _ = fmt.Fprintln(output, "  outboxer [options]")

	currentCategory := ""
	for _, option := range options {
		if option.category != currentCategory {
			currentCategory = option.category
			_, _ = fmt.Fprintf(output, "\n%s:\n", currentCategory)
		}
		_, _ = fmt.Fprintf(output, "  --%s\n      %s\n", option.name, option.usage)
	}
}

func redactDefault(value string) string {
	if value == "" {
		return ""
	}
	return "<set>"
}

type uint16Value uint16

func (v *uint16Value) String() string {
	return strconv.Itoa(int(*v))
}

func (v *uint16Value) Set(value string) error {
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return err
	}
	*v = uint16Value(parsed)
	return nil
}

type secretStringValue struct {
	value *string
}

func newSecretStringValue(value *string) *secretStringValue {
	return &secretStringValue{value: value}
}

func (v *secretStringValue) String() string {
	if v == nil || v.value == nil || *v.value == "" {
		return ""
	}
	return "<set>"
}

func (v *secretStringValue) Set(value string) error {
	*v.value = value
	return nil
}

func loadDotEnv(path string) {
	content, err := os.ReadFile(path)
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		_ = os.Setenv(key, value)
	}
}

func getenv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return value == "true"
}

func getenvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
