package outboxer

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type appConfig struct {
	EventTable       string
	EventID          string
	EventTimestamp   string
	EventData        string
	EventTarget      string
	EventTopic       string
	EventOrderingKey string
	EventAttributes  string

	BatchSize          int
	BatchWorkers       int
	BatchMaxSequential int

	DeadlockCheckInterval time.Duration
	HealthcheckPort       int
	DefaultTopic          string
	PubSubAPIEndpoint     string
	ErrorCooldown         time.Duration
	PollInterval          time.Duration

	PGHost                  string
	PGPort                  uint16
	PGUser                  string
	PGPassword              string
	PGDatabase              string
	PGSSL                   bool
	PGSSLRejectUnauthorized bool
	PGTimeout               time.Duration
	PGMaxConnections        int

	AWSRegion                  string
	AWSRoleARN                 string
	AWSRoleSessionName         string
	AWSRoleDuration            time.Duration
	AWSCredentialRefreshWindow time.Duration
}

func loadConfig() appConfig {
	return appConfig{
		EventTable:       getenv("EVENT_TABLE", "events"),
		EventID:          getenv("EVENT_ID", "id"),
		EventTimestamp:   getenv("EVENT_TIMESTAMP", "timestamp"),
		EventData:        getenv("EVENT_DATA", "data"),
		EventTarget:      getenv("EVENT_TARGET", "target"),
		EventTopic:       getenv("EVENT_TOPIC", "topic"),
		EventOrderingKey: getenv("EVENT_ORDERING_KEY", "ordering_key"),
		EventAttributes:  getenv("EVENT_ATTRIBUTES", "attributes"),

		BatchSize:          getenvInt("BATCH_SIZE", 32),
		BatchWorkers:       getenvInt("BATCH_WORKERS", 8),
		BatchMaxSequential: getenvInt("BATCH_MAX_SEQUENTIAL", 8),

		DeadlockCheckInterval: time.Duration(getenvInt("DEADLOCK_CHECK_INTERVAL_SEC", 10*60)) * time.Second,
		HealthcheckPort:       getenvInt("HEALTHCHECK_PORT", getenvInt("PORT", 8080)),
		DefaultTopic:          getenv("DEFAULT_TOPIC", "default"),
		PubSubAPIEndpoint:     getenv("PUBSUB_API_ENDPOINT", ""),
		ErrorCooldown:         time.Duration(getenvInt("ERROR_COOLDOWN_MS", 5000)) * time.Millisecond,
		PollInterval:          time.Duration(getenvInt("POLL_INTERVAL_MS", 0)) * time.Millisecond,

		PGHost:                  getenv("PG_HOST", "localhost"),
		PGPort:                  uint16(getenvInt("PG_PORT", 5432)),
		PGUser:                  getenv("PG_USER", "postgres"),
		PGPassword:              getenv("PG_PASSWORD", ""),
		PGDatabase:              getenv("PG_DATABASE", "postgres"),
		PGSSL:                   os.Getenv("PG_SSL") == "true",
		PGSSLRejectUnauthorized: os.Getenv("PG_SSL_REJECT_UNAUTHORIZED") == "true",
		PGTimeout:               time.Duration(getenvInt("PG_TIMEOUT", 10000)) * time.Millisecond,
		PGMaxConnections:        getenvInt("PG_MAX_CONNECTIONS", 10),

		AWSRegion:                  getenv("AWS_REGION", ""),
		AWSRoleARN:                 getenv("AWS_ROLE_ARN", ""),
		AWSRoleSessionName:         getenv("AWS_ROLE_SESSION_NAME", "outboxer"),
		AWSRoleDuration:            time.Duration(getenvInt("AWS_ROLE_DURATION_SECONDS", 3600)) * time.Second,
		AWSCredentialRefreshWindow: time.Duration(getenvInt("AWS_CREDENTIAL_REFRESH_WINDOW_MS", 5*60*1000)) * time.Millisecond,
	}
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
