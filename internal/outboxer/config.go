package outboxer

import (
	"io"
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
	PGSchema                string
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

// disableSentinel is the explicit value for omitting an optional column or table
// (for example EVENT_OPTIONS=disabled or --dlq-table=disabled). An empty value is
// rejected as ambiguous; this sentinel is the clear way to express "no column".
const disableSentinel = "disabled"

func loadConfig(args []string, output io.Writer) (appConfig, error) {
	parser := newConfigParser("outboxer", output)
	if err := parser.parse(args); err != nil {
		return appConfig{}, err
	}
	return parser.cfg, nil
}

// loadInitConfig parses the init command's arguments. It reuses every relay
// flag/env so a single configuration drives both verbs, and adds the
// provisioning-only flags (--apply and the PG_INIT_*/PG_PRODUCER_ROLES settings).
func loadInitConfig(args []string, output io.Writer) (appConfig, bool, error) {
	parser := newConfigParser("outboxer init", output)

	var apply bool
	parser.flags.BoolVar(&apply, "apply", false, "Execute the generated SQL against the database instead of printing it to stdout.")
	parser.options = append(parser.options, cliOption{category: "Provisioning", name: "apply", usage: "Execute the generated SQL against the database instead of printing it to stdout."})

	addStringFlag(parser.flags, &parser.options, "Provisioning", &parser.cfg.PGInitUser, "pg-init-user", parser.cfg.PGInitUser, "Provisioning role to connect as for --apply. When set, init also creates and grants to the run role.", "PG_INIT_USER")
	addValueFlag(parser.flags, &parser.options, "Provisioning", newSecretStringValue(&parser.cfg.PGInitPassword), "pg-init-password", "Password for the provisioning role.", "PG_INIT_PASSWORD", redactDefault(parser.cfg.PGInitPassword))
	producerRoles := strings.Join(parser.cfg.PGProducerRoles, ",")
	addStringFlag(parser.flags, &parser.options, "Provisioning", &producerRoles, "pg-producer-roles", producerRoles, "Comma-separated existing roles granted SELECT, INSERT on the event table.", "PG_PRODUCER_ROLES")

	if err := parser.parse(args); err != nil {
		return appConfig{}, false, err
	}
	parser.cfg.PGProducerRoles = parseStringList(producerRoles)
	return parser.cfg, apply, nil
}

// defaultConfig returns the configuration with every value at its built-in
// default. Environment variables and CLI flags are layered on top by loadConfig.
func defaultConfig() appConfig {
	return appConfig{
		EventTable:       "events",
		EventID:          "id",
		EventTimestamp:   "timestamp",
		EventPayload:     "payload",
		EventTarget:      "target",
		EventDestination: "destination",
		EventOptions:     "options",

		CollectBatchTarget: 5000,
		SQSSendConcurrency: 8,
		DLQTable:           "",
		NotifyChannel:      "outboxer_events",

		LogLevel:  "info",
		LogFormat: "text",

		WatchdogInterval:   10 * time.Minute,
		HealthPort:         0,
		PubSubEnabled:      false,
		SQSEnabled:         false,
		DefaultPubSubTopic: "default",
		DefaultSQSQueueURL: "",
		PubSubProjectID:    "",
		PubSubAPIEndpoint:  "",
		SQSAPIEndpoint:     "",
		ErrorCooldown:      5 * time.Second,
		PollInterval:       time.Second,
		PublishTimeout:     30 * time.Second,
		PublishResultGrace: 5 * time.Second,
		MaxEventAge:        0,
		StatsInterval:      10 * time.Second,

		PGHost:                  "localhost",
		PGPort:                  5432,
		PGUser:                  "postgres",
		PGPassword:              "",
		PGDatabase:              "postgres",
		PGSchema:                "public",
		PGSSL:                   false,
		PGSSLRejectUnauthorized: true,
		PGSSLRootCert:           "",
		PGConnectTimeout:        10 * time.Second,
		PGQueryTimeout:          30 * time.Second,

		PGInitUser:     "",
		PGInitPassword: "",

		AWSRegion:                  "",
		AWSRoleARN:                 "",
		AWSRoleSessionName:         "outboxer",
		AWSRoleDuration:            time.Hour,
		AWSCredentialRefreshWindow: 5 * time.Minute,
		AWSWebIdentityProvider:     "",
		AWSWebIdentityAudience:     "",
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
