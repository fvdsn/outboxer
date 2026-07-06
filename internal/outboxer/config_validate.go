package outboxer

import (
	"fmt"
)

type configValidationMode uint8

const (
	configValidationRelay configValidationMode = iota
	configValidationInit
)

// validate is the single entry point for command configuration validation.
// Storage rules apply to both commands; relay-only rules are skipped by init
// because provisioning does not require a runnable publishing configuration.
func (cfg appConfig) validate(mode configValidationMode) error {
	if err := cfg.validateStorage(); err != nil {
		return err
	}

	switch mode {
	case configValidationInit:
		return nil
	case configValidationRelay:
		return cfg.validateRuntime()
	default:
		return fmt.Errorf("unknown configuration validation mode %d", mode)
	}
}

func (cfg appConfig) validateStorage() error {
	if cfg.EventTable == "" {
		return fmt.Errorf("an event table is required: set EVENT_TABLE")
	}
	if cfg.EventID == "" {
		return fmt.Errorf("an id column is required: set EVENT_ID")
	}
	if cfg.EventPayload == "" {
		return fmt.Errorf("a payload column is required: set EVENT_PAYLOAD")
	}
	if cfg.PGSchema == "" {
		return fmt.Errorf("a PostgreSQL schema is required: set PG_SCHEMA")
	}
	if cfg.DLQTable != "" && cfg.DLQTable == cfg.EventTable {
		return fmt.Errorf("DLQ_TABLE must not equal EVENT_TABLE")
	}
	if cfg.PollInterval > 0 && cfg.NotifyChannel == "" {
		return fmt.Errorf("notify channel must not be empty when polling is enabled: set NOTIFY_CHANNEL or POLL_INTERVAL_MS=0")
	}

	seen := map[string]string{}
	columns := []struct{ value, label string }{
		{cfg.EventID, "EVENT_ID"},
		{cfg.EventPayload, "EVENT_PAYLOAD"},
		{cfg.EventTarget, "EVENT_TARGET"},
		{cfg.EventDestination, "EVENT_DESTINATION"},
		{cfg.EventTimestamp, "EVENT_TIMESTAMP"},
		{cfg.EventOptions, "EVENT_OPTIONS"},
	}
	for _, column := range columns {
		if column.value == "" {
			continue
		}
		if previous, ok := seen[column.value]; ok {
			return fmt.Errorf("%s and %s both resolve to the same column name %q", previous, column.label, column.value)
		}
		seen[column.value] = column.label
	}
	return nil
}

func (cfg appConfig) validateRuntime() error {
	routes := configuredProviderRoutes(cfg)
	if len(routes) == 0 {
		return fmt.Errorf("no publishing provider enabled")
	}
	if len(routes) > 1 && cfg.EventTarget == "" {
		return fmt.Errorf("a target column is required when multiple providers are enabled: set EVENT_TARGET")
	}
	for _, route := range routes {
		if route.defaultDestination == "" && cfg.EventDestination == "" {
			return fmt.Errorf("provider %q needs a destination: set EVENT_DESTINATION or its default destination", route.target)
		}
	}
	for _, spec := range providerSpecs {
		route := spec.route(cfg)
		if !spec.enabled(cfg) && len(route.ownedDestinations) > 0 {
			return fmt.Errorf("destination ownership for provider %q requires that provider to be enabled", route.target)
		}
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
	if cfg.WatchdogInterval <= 0 {
		return fmt.Errorf("watchdog interval (%s) must be positive: set WATCHDOG_INTERVAL_MS", cfg.WatchdogInterval)
	}
	if cfg.PollInterval > 0 && cfg.WatchdogInterval < 10*cfg.PollInterval {
		return fmt.Errorf("watchdog interval (%s) must be at least 10x the poll interval (%s) to avoid false deadlocks: increase WATCHDOG_INTERVAL_MS or decrease POLL_INTERVAL_MS", cfg.WatchdogInterval, cfg.PollInterval)
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
