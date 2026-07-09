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
	BacklogCountLimit  int
	SQSSendConcurrency int
	DLQTable           string
	NotifyChannel      string

	LogLevel  string
	LogFormat string

	WatchdogInterval   time.Duration
	HealthPort         int
	HealthStaleAfter   time.Duration
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

// Fixed internal settings. Each of these was a configuration knob once; the
// July 2026 benchmark campaign and the configuration audit
// (specs/config_surface_audit.md) decided them, and a decided value is a
// constant, not a knob.
const (
	// Measured on ECS Fargate and EKS + SQS (eu-central-1): throughput scales
	// with send concurrency up to ~128, where publish time converges with the
	// batch's database time. The HTTP connection pool is sized to match. Idle
	// deployments pay nothing for the headroom: goroutines and connections
	// only materialize under load.
	sqsSendConcurrency = 128

	// The backlog probe scans at most this many routable rows per stats
	// interval; measured at 10-20ms at the cap on perf-sized databases.
	backlogCountLimit = 100000

	// Sleep after a failed batch before retrying, long enough to avoid
	// hammering a struggling provider or database, short enough that a
	// transient error costs little.
	errorCooldown = 5 * time.Second

	// Extra wait after the provider publish timeout for async publish
	// results to land before the batch is judged.
	publishResultGrace = 5 * time.Second

	// Cadence of the periodic statistics log line; also paces the backlog
	// probe.
	statsInterval = 10 * time.Second

	pgConnectTimeout = 10 * time.Second

	awsRoleDuration            = time.Hour
	awsCredentialRefreshWindow = 5 * time.Minute

	// The relay reports /healthz unhealthy after this long without a
	// committed batch (and never for provider failures, which a restart
	// cannot fix). Raised to 10x the poll interval when that is larger, so
	// an idle relay cannot flap.
	healthStaleAfterFloor = 5 * time.Minute

	// Watchdog sweep cadence, raised to 10x the poll interval when that is
	// larger to avoid false deadlock reports.
	watchdogIntervalFloor = 10 * time.Minute

	// The notification channel is derived from the event table
	// (outboxer_<table>), so multiple tables in one database get distinct
	// wake-up channels with nothing to coordinate. Postgres channel names
	// share the 63-byte identifier limit.
	notifyChannelPrefix   = "outboxer_"
	notifyChannelMaxBytes = 63
)

// deriveNotifyChannel names the notification channel for an event table.
// Relays on same-named tables in different schemas share a channel, which is
// harmless: notifications are wake-up hints, so the cost is a spurious empty
// select, never a correctness problem.
func deriveNotifyChannel(eventTable string) string {
	channel := notifyChannelPrefix + eventTable
	if len(channel) > notifyChannelMaxBytes {
		channel = channel[:notifyChannelMaxBytes]
	}
	return channel
}

// deriveConfig fills in the settings computed from public configuration.
// Called after flag/env parsing (and again after any test override of the
// inputs).
func deriveConfig(cfg *appConfig) {
	cfg.NotifyChannel = deriveNotifyChannel(cfg.EventTable)
	cfg.WatchdogInterval = max(watchdogIntervalFloor, 10*cfg.PollInterval)
	cfg.HealthStaleAfter = max(healthStaleAfterFloor, 10*cfg.PollInterval)
}

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

		// Swept 1k-20k on GKE and EKS (July 2026): throughput keeps rising
		// with batch size (per-batch fixed cost dominates below 10k), but
		// peak in-flight memory scales as batch x payload size and a failed
		// batch redelivers whole. 10k takes most of the throughput
		// (+37% Pub/Sub, +21% SQS over 5k) while staying ~100MB in flight
		// at 10KB payloads; deployments with small events can raise it.
		CollectBatchTarget: 10000,
		BacklogCountLimit:  backlogCountLimit,
		SQSSendConcurrency: sqsSendConcurrency,
		DLQTable:           "",
		NotifyChannel:      deriveNotifyChannel("events"),

		LogLevel:  "info",
		LogFormat: "text",

		WatchdogInterval:   watchdogIntervalFloor,
		HealthPort:         0,
		HealthStaleAfter:   healthStaleAfterFloor,
		PubSubEnabled:      false,
		SQSEnabled:         false,
		DefaultPubSubTopic: "default",
		DefaultSQSQueueURL: "",
		PubSubProjectID:    "",
		PubSubAPIEndpoint:  "",
		SQSAPIEndpoint:     "",
		ErrorCooldown:      errorCooldown,
		PollInterval:       time.Second,
		PublishTimeout:     30 * time.Second,
		PublishResultGrace: publishResultGrace,
		MaxEventAge:        0,
		StatsInterval:      statsInterval,

		PGHost:                  "localhost",
		PGPort:                  5432,
		PGUser:                  "postgres",
		PGPassword:              "",
		PGDatabase:              "postgres",
		PGSchema:                "public",
		PGSSL:                   false,
		PGSSLRejectUnauthorized: true,
		PGSSLRootCert:           "",
		PGConnectTimeout:        pgConnectTimeout,
		PGQueryTimeout:          30 * time.Second,

		PGInitUser:     "",
		PGInitPassword: "",

		AWSRegion:                  "",
		AWSRoleARN:                 "",
		AWSRoleSessionName:         "outboxer",
		AWSRoleDuration:            awsRoleDuration,
		AWSCredentialRefreshWindow: awsCredentialRefreshWindow,
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
