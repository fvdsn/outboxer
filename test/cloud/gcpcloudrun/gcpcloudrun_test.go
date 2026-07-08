//go:build cloud

// Package gcpcloudrun_test drives the harness scenarios against the
// deploy/gcp-cloudrun stack. Bring the stack up first:
//
//	just cloud-gcp-cloudrun-up
//	just cloud-gcp-cloudrun-test
//	just cloud-gcp-cloudrun-down
package gcpcloudrun_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/fvdsn/outboxer/test/cloud/harness"
)

const environment = "gcp-cloudrun"

func setup(ctx context.Context, t *testing.T) (harness.Env, harness.MessageSink, *harness.Metrics) {
	t.Helper()
	env := harness.LoadEnv(t, "tfoutputs.json")
	client := harness.NewPubSubClient(ctx, t, env.ProjectID)
	sink := harness.NewPubSubSink(client, env.ProjectID, env.Subscription)
	return env, sink, harness.NewMetrics(t, env)
}

func smokeEvents(env harness.Env) harness.SmokeEvents {
	return harness.SmokeEvents{
		Unordered: func(payload string, i int) harness.Event {
			destination := env.Topic // explicit destination
			if i%2 == 0 {
				destination = "" // resolved by DEFAULT_PUBSUB_TOPIC
			}
			return harness.Event{Payload: payload, Destination: destination}
		},
		Ordered: func(payload string, key string, _ int) harness.Event {
			return harness.Event{Payload: payload, OrderingKey: key}
		},
		Poison: func(payload string, _ int) harness.Event {
			return harness.Event{Payload: payload, Destination: "syntactically/invalid/topic"}
		},
	}
}

func TestGCPCloudRunSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	env, sink, metrics := setup(ctx, t)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	harness.Smoke(ctx, t, env, db, sink, metrics, smokeEvents(env))
}

func TestGCPCloudRunPerf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	events := 200000
	if raw := os.Getenv("OUTBOXER_CLOUD_PERF_EVENTS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid OUTBOXER_CLOUD_PERF_EVENTS: %v", err)
		}
		events = parsed
	}

	env, sink, metrics := setup(ctx, t)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	harness.Perf(ctx, t, environment, db, sink, metrics, events, "../results")
}

func TestGCPCloudRunLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	env, sink, metrics := setup(ctx, t)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	harness.Latency(ctx, t, environment, db, sink, metrics, 60, "../results")
}
