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

type cliOption struct {
	category string
	name     string
	usage    string
	// envVars are the environment variables that populate this flag, in priority
	// order. The first one that is set is applied through the flag's own parser,
	// so an environment value is validated exactly like the equivalent CLI flag.
	envVars []string
}

type configParser struct {
	cfg      appConfig
	flags    *flag.FlagSet
	options  []cliOption
	finalize func()
}

func newConfigParser(name string, output io.Writer) *configParser {
	parser := &configParser{cfg: defaultConfig()}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		printUsage(output, parser.options)
	}
	parser.flags = flags
	parser.finalize = bindConfigFlags(flags, &parser.cfg, &parser.options)
	return parser
}

func (p *configParser) parse(args []string) error {
	if err := applyEnv(p.flags, p.options); err != nil {
		return err
	}
	if err := p.flags.Parse(args); err != nil {
		return err
	}
	p.finalize()
	if p.flags.NArg() > 0 {
		return fmt.Errorf("unexpected argument: %q", p.flags.Arg(0))
	}
	return nil
}

// bindConfigFlags registers every relay configuration flag on the flag set and
// returns a finalize function that must be called after Parse to convert the
// millisecond/list scratch values back onto the config.
func bindConfigFlags(flags *flag.FlagSet, cfg *appConfig, options *[]cliOption) func() {
	addStringFlag(flags, options, "Event table", &cfg.EventTable, "event-table", cfg.EventTable, "Outbox table name.", "EVENT_TABLE")
	addStringFlag(flags, options, "Event table", &cfg.EventID, "event-id", cfg.EventID, "Event id column.", "EVENT_ID")
	addDisableableFlag(flags, options, "Event table", &cfg.EventTimestamp, "event-timestamp", cfg.EventTimestamp, "Event timestamp column.", "EVENT_TIMESTAMP")
	addStringFlag(flags, options, "Event table", &cfg.EventPayload, "event-payload", cfg.EventPayload, "Event payload column.", "EVENT_PAYLOAD")
	addDisableableFlag(flags, options, "Event table", &cfg.EventTarget, "event-target", cfg.EventTarget, "Backend selector column. Values pubsub or sqs.", "EVENT_TARGET")
	addDisableableFlag(flags, options, "Event table", &cfg.EventDestination, "event-destination", cfg.EventDestination, "Pub/Sub topic name or SQS queue URL column.", "EVENT_DESTINATION")
	addDisableableFlag(flags, options, "Event table", &cfg.EventOptions, "event-options", cfg.EventOptions, "Backend-specific JSON options column.", "EVENT_OPTIONS")

	addIntFlag(flags, options, "Batch processing", &cfg.CollectBatchTarget, "collect-batch-target", cfg.CollectBatchTarget, "Approximate target rows selected per batch, spread evenly across eligible routes.", "COLLECT_BATCH_TARGET")
	addIntFlag(flags, options, "Batch processing", &cfg.SQSSendConcurrency, "sqs-send-concurrency", cfg.SQSSendConcurrency, "Maximum concurrent SQS send requests.", "SQS_SEND_CONCURRENCY")
	addDisableableFlag(flags, options, "Batch processing", &cfg.DLQTable, "dlq-table", cfg.DLQTable, "Dead letter table for poison events.", "DLQ_TABLE")

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
	addIntFlag(flags, options, "Batch processing", &pollIntervalMS, "poll-interval-ms", pollIntervalMS, "Sleep after an empty batch in milliseconds, cut short by a LISTEN/NOTIFY wake-up. 0 polls continuously with no sleep.", "POLL_INTERVAL_MS")
	addIntFlag(flags, options, "Batch processing", &watchdogIntervalMS, "watchdog-interval-ms", watchdogIntervalMS, "Watchdog interval in milliseconds.", "WATCHDOG_INTERVAL_MS")
	addIntFlag(flags, options, "Batch processing", &publishTimeoutMS, "publish-timeout-ms", publishTimeoutMS, "Timeout for a single publish call in milliseconds. Must be positive.", "PUBLISH_TIMEOUT_MS")
	addIntFlag(flags, options, "Batch processing", &publishResultGraceMS, "publish-result-grace-ms", publishResultGraceMS, "Extra wait after provider publish timeout for async publish results.", "PUBLISH_RESULT_GRACE_MS")
	addIntFlag(flags, options, "Batch processing", &maxEventAgeMS, "max-event-age-ms", maxEventAgeMS, "Maximum selected event age in milliseconds. 0 disables age-based poison.", "MAX_EVENT_AGE_MS")
	addIntFlag(flags, options, "Batch processing", &statsIntervalMS, "stats-interval-ms", statsIntervalMS, "Periodic statistics logging interval in milliseconds. 0 disables statistics.", "STATS_INTERVAL_MS")
	addStringFlag(flags, options, "Batch processing", &cfg.NotifyChannel, "notify-channel", cfg.NotifyChannel, "PostgreSQL channel for the new-event notification trigger that init provisions; the relay also LISTENs on it when POLL_INTERVAL_MS > 0.", "NOTIFY_CHANNEL")

	addIntFlag(flags, options, "HTTP / health", &cfg.HealthPort, "health-port", cfg.HealthPort, "HTTP health server port. Set to 0 to disable.", "HEALTH_PORT, PORT")

	addStringFlag(flags, options, "Logging", &cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, or error.", "LOG_LEVEL")
	addStringFlag(flags, options, "Logging", &cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: text or json.", "LOG_FORMAT")

	addStringFlag(flags, options, "PostgreSQL", &cfg.PGHost, "pg-host", cfg.PGHost, "PostgreSQL host.", "PG_HOST")
	addValueFlag(flags, options, "PostgreSQL", (*uint16Value)(&cfg.PGPort), "pg-port", "PostgreSQL port.", "PG_PORT", cfg.PGPort)
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGUser, "pg-user", cfg.PGUser, "PostgreSQL user.", "PG_USER")
	addValueFlag(flags, options, "PostgreSQL", newSecretStringValue(&cfg.PGPassword), "pg-password", "PostgreSQL password.", "PG_PASSWORD", redactDefault(cfg.PGPassword))
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGDatabase, "pg-database", cfg.PGDatabase, "PostgreSQL database.", "PG_DATABASE")
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGSchema, "pg-schema", cfg.PGSchema, "PostgreSQL schema containing the outbox objects.", "PG_SCHEMA")
	addBoolFlag(flags, options, "PostgreSQL", &cfg.PGSSL, "pg-ssl", cfg.PGSSL, "Enable PostgreSQL TLS.", "PG_SSL")
	addBoolFlag(flags, options, "PostgreSQL", &cfg.PGSSLRejectUnauthorized, "pg-ssl-reject-unauthorized", cfg.PGSSLRejectUnauthorized, "Verify PostgreSQL TLS certificate and hostname.", "PG_SSL_REJECT_UNAUTHORIZED")
	addStringFlag(flags, options, "PostgreSQL", &cfg.PGSSLRootCert, "pg-ssl-root-cert", cfg.PGSSLRootCert, "Path to a CA certificate (PEM) used to verify the PostgreSQL server.", "PG_SSL_ROOT_CERT")
	addIntFlag(flags, options, "PostgreSQL", &pgTimeoutMS, "pg-connect-timeout-ms", pgTimeoutMS, "PostgreSQL connect timeout in milliseconds.", "PG_CONNECT_TIMEOUT_MS")
	addIntFlag(flags, options, "PostgreSQL", &pgQueryTimeoutMS, "pg-query-timeout-ms", pgQueryTimeoutMS, "Timeout for a single database query in milliseconds. 0 disables the timeout.", "PG_QUERY_TIMEOUT_MS")

	addBoolFlag(flags, options, "Google Pub/Sub", &cfg.PubSubEnabled, "pubsub-enabled", cfg.PubSubEnabled, "Enable publishing to Google Pub/Sub.", "PUBSUB_ENABLED")
	addDisableableFlag(flags, options, "Google Pub/Sub", &cfg.DefaultPubSubTopic, "default-pubsub-topic", cfg.DefaultPubSubTopic, "Pub/Sub topic used when an event has no destination.", "DEFAULT_PUBSUB_TOPIC")
	addStringFlag(flags, options, "Google Pub/Sub", &pubsubDestinations, "pubsub-destinations", pubsubDestinations, "Comma-separated Pub/Sub destinations this process owns. Empty means all Pub/Sub destinations.", "PUBSUB_DESTINATIONS")
	addStringFlag(flags, options, "Google Pub/Sub", &cfg.PubSubProjectID, "pubsub-project-id", cfg.PubSubProjectID, "Google Cloud project for Pub/Sub. Detected from ADC when empty.", "PUBSUB_PROJECT_ID")
	addStringFlag(flags, options, "Google Pub/Sub", &cfg.PubSubAPIEndpoint, "pubsub-api-endpoint", cfg.PubSubAPIEndpoint, "Optional Pub/Sub API endpoint override.", "PUBSUB_API_ENDPOINT")

	addBoolFlag(flags, options, "AWS SQS", &cfg.SQSEnabled, "sqs-enabled", cfg.SQSEnabled, "Enable publishing to AWS SQS.", "SQS_ENABLED")
	addDisableableFlag(flags, options, "AWS SQS", &cfg.DefaultSQSQueueURL, "default-sqs-queue-url", cfg.DefaultSQSQueueURL, "SQS queue URL used when an event has no destination.", "DEFAULT_SQS_QUEUE_URL")
	addStringFlag(flags, options, "AWS SQS", &sqsDestinations, "sqs-destinations", sqsDestinations, "Comma-separated SQS destinations this process owns. Empty means all SQS destinations.", "SQS_DESTINATIONS")
	addStringFlag(flags, options, "AWS SQS", &cfg.SQSAPIEndpoint, "sqs-api-endpoint", cfg.SQSAPIEndpoint, "Optional SQS API endpoint override.", "SQS_API_ENDPOINT")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRegion, "aws-region", cfg.AWSRegion, "AWS region for SQS and STS.", "AWS_REGION")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRoleARN, "aws-role-arn", cfg.AWSRoleARN, "Optional AWS role to assume before publishing to SQS.", "AWS_ROLE_ARN")
	addStringFlag(flags, options, "AWS SQS", &cfg.AWSRoleSessionName, "aws-role-session-name", cfg.AWSRoleSessionName, "AWS assume-role session name.", "AWS_ROLE_SESSION_NAME")
	addIntFlag(flags, options, "AWS SQS", &awsRoleDurationSeconds, "aws-role-duration-seconds", awsRoleDurationSeconds, "AWS assumed-role duration in seconds.", "AWS_ROLE_DURATION_SECONDS")
	addIntFlag(flags, options, "AWS SQS", &awsCredentialRefreshWindowMS, "aws-credential-refresh-window-ms", awsCredentialRefreshWindowMS, "Refresh assumed credentials before expiry in milliseconds.", "AWS_CREDENTIAL_REFRESH_WINDOW_MS")
	addDisableableFlag(flags, options, "AWS SQS", &cfg.AWSWebIdentityProvider, "aws-web-identity-provider", cfg.AWSWebIdentityProvider, "Set to 'google' to assume the AWS role with a Google OIDC token (GCP to AWS).", "AWS_WEB_IDENTITY_PROVIDER")
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

// applyEnv applies environment variables to the registered flags before CLI
// parsing, so the precedence is CLI > environment > default and an environment
// value is validated by the same flag parser as the equivalent CLI flag.
func applyEnv(flags *flag.FlagSet, options []cliOption) error {
	for _, option := range options {
		for _, name := range option.envVars {
			value, ok := os.LookupEnv(name)
			if !ok {
				continue
			}
			if err := flags.Set(option.name, value); err != nil {
				return fmt.Errorf("invalid %s: %w", name, err)
			}
			break
		}
	}
	return nil
}

func optionHelp(description string, envVar string, defaultValue any) string {
	return fmt.Sprintf("%s Env: %s. Default: %v.", description, envVar, defaultValue)
}

func addStringFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *string, name string, value string, description string, envVar string) {
	*destination = value
	usage := optionHelp(description, envVar, value)
	flags.Var(&nonEmptyStringValue{value: destination}, name, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage, envVars: parseStringList(envVar)})
}

// addDisableableFlag registers an optional string setting whose value may be the
// disableSentinel ("disabled") to omit it. An empty value is rejected.
func addDisableableFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *string, name string, value string, description string, envVar string) {
	*destination = value
	defaultDisplay := value
	if defaultDisplay == "" {
		defaultDisplay = disableSentinel
	}
	usage := optionHelp(description+fmt.Sprintf(" Set to %q to omit it.", disableSentinel), envVar, defaultDisplay)
	flags.Var(&disableableStringValue{value: destination}, name, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage, envVars: parseStringList(envVar)})
}

func addIntFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *int, name string, value int, description string, envVar string) {
	usage := optionHelp(description, envVar, value)
	flags.IntVar(destination, name, value, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage, envVars: parseStringList(envVar)})
}

func addBoolFlag(flags *flag.FlagSet, options *[]cliOption, category string, destination *bool, name string, value bool, description string, envVar string) {
	usage := optionHelp(description, envVar, value)
	flags.BoolVar(destination, name, value, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage, envVars: parseStringList(envVar)})
}

func addValueFlag(flags *flag.FlagSet, options *[]cliOption, category string, value flag.Value, name string, description string, envVar string, defaultValue any) {
	usage := optionHelp(description, envVar, defaultValue)
	flags.Var(value, name, usage)
	*options = append(*options, cliOption{category: category, name: name, usage: usage, envVars: parseStringList(envVar)})
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

// nonEmptyStringValue is a string flag that rejects an empty value, so an
// ambiguous FOO="" (or --foo="") is an error rather than a silent empty value.
type nonEmptyStringValue struct {
	value *string
}

func (v *nonEmptyStringValue) String() string {
	if v == nil || v.value == nil {
		return ""
	}
	return *v.value
}

func (v *nonEmptyStringValue) Set(value string) error {
	if value == "" {
		return fmt.Errorf("value must not be empty")
	}
	*v.value = value
	return nil
}

// disableableStringValue is a string flag that rejects an empty value but maps
// the disableSentinel to an empty internal value, the explicit way to omit an
// optional column or table.
type disableableStringValue struct {
	value *string
}

func (v *disableableStringValue) String() string {
	if v == nil || v.value == nil || *v.value == "" {
		return disableSentinel
	}
	return *v.value
}

func (v *disableableStringValue) Set(value string) error {
	if value == "" {
		return fmt.Errorf("value must not be empty; use %q to omit it", disableSentinel)
	}
	if strings.EqualFold(value, disableSentinel) {
		*v.value = ""
		return nil
	}
	*v.value = value
	return nil
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
	if value == "" {
		return fmt.Errorf("value must not be empty")
	}
	*v.value = value
	return nil
}
