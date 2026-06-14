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
	EventOrderingKey string
	EventAttributes  string

	BatchSize          int
	BatchWorkers       int
	BatchMaxSequential int

	WatchdogInterval  time.Duration
	HealthPort        int
	DefaultTopic      string
	PubSubAPIEndpoint string
	ErrorCooldown     time.Duration
	PollInterval      time.Duration

	PGHost                  string
	PGPort                  uint16
	PGUser                  string
	PGPassword              string
	PGDatabase              string
	PGSSL                   bool
	PGSSLRejectUnauthorized bool
	PGConnectTimeout        time.Duration
	PGMaxConnections        int

	AWSRegion                  string
	AWSRoleARN                 string
	AWSRoleSessionName         string
	AWSRoleDuration            time.Duration
	AWSCredentialRefreshWindow time.Duration
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

	addStringFlag(flags, &options, "Event table", &cfg.EventTable, "event-table", cfg.EventTable, "Outbox table name.", "EVENT_TABLE")
	addStringFlag(flags, &options, "Event table", &cfg.EventID, "event-id", cfg.EventID, "Event id column.", "EVENT_ID")
	addStringFlag(flags, &options, "Event table", &cfg.EventTimestamp, "event-timestamp", cfg.EventTimestamp, "Event timestamp column.", "EVENT_TIMESTAMP")
	addStringFlag(flags, &options, "Event table", &cfg.EventPayload, "event-payload", cfg.EventPayload, "Event payload column.", "EVENT_PAYLOAD")
	addStringFlag(flags, &options, "Event table", &cfg.EventTarget, "event-target", cfg.EventTarget, "Target column. Use sqs for SQS, anything else for Pub/Sub.", "EVENT_TARGET")
	addStringFlag(flags, &options, "Event table", &cfg.EventDestination, "event-destination", cfg.EventDestination, "Pub/Sub topic name or SQS queue URL column.", "EVENT_DESTINATION")
	addStringFlag(flags, &options, "Event table", &cfg.EventOrderingKey, "event-ordering-key", cfg.EventOrderingKey, "Ordering key / FIFO message group column.", "EVENT_ORDERING_KEY")
	addStringFlag(flags, &options, "Event table", &cfg.EventAttributes, "event-attributes", cfg.EventAttributes, "JSON attributes column.", "EVENT_ATTRIBUTES")

	addIntFlag(flags, &options, "Batch processing", &cfg.BatchSize, "batch-size", cfg.BatchSize, "Maximum rows selected per batch.", "BATCH_SIZE")
	addIntFlag(flags, &options, "Batch processing", &cfg.BatchWorkers, "batch-workers", cfg.BatchWorkers, "Number of parallel publisher workers per batch.", "BATCH_WORKERS")
	addIntFlag(flags, &options, "Batch processing", &cfg.BatchMaxSequential, "batch-max-sequential", cfg.BatchMaxSequential, "Maximum ordered events assigned to one worker in a batch.", "BATCH_MAX_SEQUENTIAL")

	var watchdogIntervalMS = int(cfg.WatchdogInterval / time.Millisecond)
	var errorCooldownMS = int(cfg.ErrorCooldown / time.Millisecond)
	var pollIntervalMS = int(cfg.PollInterval / time.Millisecond)
	var pgTimeoutMS = int(cfg.PGConnectTimeout / time.Millisecond)
	var awsRoleDurationSeconds = int(cfg.AWSRoleDuration / time.Second)
	var awsCredentialRefreshWindowMS = int(cfg.AWSCredentialRefreshWindow / time.Millisecond)

	addIntFlag(flags, &options, "Batch processing", &errorCooldownMS, "error-cooldown-ms", errorCooldownMS, "Sleep after batch or database errors in milliseconds.", "ERROR_COOLDOWN_MS")
	addIntFlag(flags, &options, "Batch processing", &pollIntervalMS, "poll-interval-ms", pollIntervalMS, "Sleep after an empty batch in milliseconds.", "POLL_INTERVAL_MS")
	addIntFlag(flags, &options, "Batch processing", &watchdogIntervalMS, "watchdog-interval-ms", watchdogIntervalMS, "Watchdog interval in milliseconds.", "WATCHDOG_INTERVAL_MS")

	addIntFlag(flags, &options, "HTTP / health", &cfg.HealthPort, "health-port", cfg.HealthPort, "HTTP health server port. Set to 0 to disable.", "HEALTH_PORT, PORT")

	addStringFlag(flags, &options, "PostgreSQL", &cfg.PGHost, "pg-host", cfg.PGHost, "PostgreSQL host.", "PG_HOST")
	addValueFlag(flags, &options, "PostgreSQL", (*uint16Value)(&cfg.PGPort), "pg-port", "PostgreSQL port.", "PG_PORT", cfg.PGPort)
	addStringFlag(flags, &options, "PostgreSQL", &cfg.PGUser, "pg-user", cfg.PGUser, "PostgreSQL user.", "PG_USER")
	addValueFlag(flags, &options, "PostgreSQL", newSecretStringValue(&cfg.PGPassword), "pg-password", "PostgreSQL password.", "PG_PASSWORD", redactDefault(cfg.PGPassword))
	addStringFlag(flags, &options, "PostgreSQL", &cfg.PGDatabase, "pg-database", cfg.PGDatabase, "PostgreSQL database.", "PG_DATABASE")
	addBoolFlag(flags, &options, "PostgreSQL", &cfg.PGSSL, "pg-ssl", cfg.PGSSL, "Enable PostgreSQL TLS.", "PG_SSL")
	addBoolFlag(flags, &options, "PostgreSQL", &cfg.PGSSLRejectUnauthorized, "pg-ssl-reject-unauthorized", cfg.PGSSLRejectUnauthorized, "Verify PostgreSQL TLS certificates.", "PG_SSL_REJECT_UNAUTHORIZED")
	addIntFlag(flags, &options, "PostgreSQL", &pgTimeoutMS, "pg-connect-timeout-ms", pgTimeoutMS, "PostgreSQL connect timeout in milliseconds.", "PG_CONNECT_TIMEOUT_MS")
	addIntFlag(flags, &options, "PostgreSQL", &cfg.PGMaxConnections, "pg-max-connections", cfg.PGMaxConnections, "PostgreSQL max open connections.", "PG_MAX_CONNECTIONS")

	addStringFlag(flags, &options, "Google Pub/Sub", &cfg.DefaultTopic, "default-topic", cfg.DefaultTopic, "Pub/Sub topic used when an event has no topic.", "DEFAULT_TOPIC")
	addStringFlag(flags, &options, "Google Pub/Sub", &cfg.PubSubAPIEndpoint, "pubsub-api-endpoint", cfg.PubSubAPIEndpoint, "Optional Pub/Sub API endpoint override.", "PUBSUB_API_ENDPOINT")

	addStringFlag(flags, &options, "AWS SQS", &cfg.AWSRegion, "aws-region", cfg.AWSRegion, "AWS region for SQS and STS.", "AWS_REGION")
	addStringFlag(flags, &options, "AWS SQS", &cfg.AWSRoleARN, "aws-role-arn", cfg.AWSRoleARN, "Optional AWS role to assume before publishing to SQS.", "AWS_ROLE_ARN")
	addStringFlag(flags, &options, "AWS SQS", &cfg.AWSRoleSessionName, "aws-role-session-name", cfg.AWSRoleSessionName, "AWS assume-role session name.", "AWS_ROLE_SESSION_NAME")
	addIntFlag(flags, &options, "AWS SQS", &awsRoleDurationSeconds, "aws-role-duration-seconds", awsRoleDurationSeconds, "AWS assumed-role duration in seconds.", "AWS_ROLE_DURATION_SECONDS")
	addIntFlag(flags, &options, "AWS SQS", &awsCredentialRefreshWindowMS, "aws-credential-refresh-window-ms", awsCredentialRefreshWindowMS, "Refresh assumed credentials before expiry in milliseconds.", "AWS_CREDENTIAL_REFRESH_WINDOW_MS")

	if err := flags.Parse(args); err != nil {
		return appConfig{}, err
	}

	cfg.WatchdogInterval = time.Duration(watchdogIntervalMS) * time.Millisecond
	cfg.ErrorCooldown = time.Duration(errorCooldownMS) * time.Millisecond
	cfg.PollInterval = time.Duration(pollIntervalMS) * time.Millisecond
	cfg.PGConnectTimeout = time.Duration(pgTimeoutMS) * time.Millisecond
	cfg.AWSRoleDuration = time.Duration(awsRoleDurationSeconds) * time.Second
	cfg.AWSCredentialRefreshWindow = time.Duration(awsCredentialRefreshWindowMS) * time.Millisecond

	return cfg, nil
}

func loadConfigFromEnv() appConfig {
	return appConfig{
		EventTable:       getenv("EVENT_TABLE", "events"),
		EventID:          getenv("EVENT_ID", "id"),
		EventTimestamp:   getenv("EVENT_TIMESTAMP", "timestamp"),
		EventPayload:     getenv("EVENT_PAYLOAD", "payload"),
		EventTarget:      getenv("EVENT_TARGET", "target"),
		EventDestination: getenv("EVENT_DESTINATION", "destination"),
		EventOrderingKey: getenv("EVENT_ORDERING_KEY", "ordering_key"),
		EventAttributes:  getenv("EVENT_ATTRIBUTES", "attributes"),

		BatchSize:          getenvInt("BATCH_SIZE", 32),
		BatchWorkers:       getenvInt("BATCH_WORKERS", 8),
		BatchMaxSequential: getenvInt("BATCH_MAX_SEQUENTIAL", 8),

		WatchdogInterval:  time.Duration(getenvInt("WATCHDOG_INTERVAL_MS", 10*60*1000)) * time.Millisecond,
		HealthPort:        getenvInt("HEALTH_PORT", getenvInt("PORT", 0)),
		DefaultTopic:      getenv("DEFAULT_TOPIC", "default"),
		PubSubAPIEndpoint: getenv("PUBSUB_API_ENDPOINT", ""),
		ErrorCooldown:     time.Duration(getenvInt("ERROR_COOLDOWN_MS", 5000)) * time.Millisecond,
		PollInterval:      time.Duration(getenvInt("POLL_INTERVAL_MS", 0)) * time.Millisecond,

		PGHost:                  getenv("PG_HOST", "localhost"),
		PGPort:                  uint16(getenvInt("PG_PORT", 5432)),
		PGUser:                  getenv("PG_USER", "postgres"),
		PGPassword:              getenv("PG_PASSWORD", ""),
		PGDatabase:              getenv("PG_DATABASE", "postgres"),
		PGSSL:                   os.Getenv("PG_SSL") == "true",
		PGSSLRejectUnauthorized: os.Getenv("PG_SSL_REJECT_UNAUTHORIZED") == "true",
		PGConnectTimeout:        time.Duration(getenvInt("PG_CONNECT_TIMEOUT_MS", 10000)) * time.Millisecond,
		PGMaxConnections:        getenvInt("PG_MAX_CONNECTIONS", 10),

		AWSRegion:                  getenv("AWS_REGION", ""),
		AWSRoleARN:                 getenv("AWS_ROLE_ARN", ""),
		AWSRoleSessionName:         getenv("AWS_ROLE_SESSION_NAME", "outboxer"),
		AWSRoleDuration:            time.Duration(getenvInt("AWS_ROLE_DURATION_SECONDS", 3600)) * time.Second,
		AWSCredentialRefreshWindow: time.Duration(getenvInt("AWS_CREDENTIAL_REFRESH_WINDOW_MS", 5*60*1000)) * time.Millisecond,
	}
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
