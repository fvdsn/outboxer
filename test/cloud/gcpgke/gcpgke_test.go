//go:build cloud

// Package gcpgke_test drives the harness scenarios against the
// deploy/gcp-gke stack. Bring the stack up first:
//
//	just cloud-gcp-gke-up
//	just cloud-gcp-gke-test
//	just cloud-gcp-gke-down
package gcpgke_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/fvdsn/outboxer/test/cloud/harness"
)

const environment = "gcp-gke"

func TestGCPGKESmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	env := harness.LoadEnv(t, "tfoutputs.json")
	// Inside a cluster the relay has no external URL; reach its health port
	// through kubectl port-forward.
	env.ServiceURL = harness.StartPortForward(ctx, t, "outboxer", "deployment/outboxer", 8080)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	pubsubClient := harness.NewPubSubClient(ctx, t, env.ProjectID)

	harness.Smoke(ctx, t, env, db, pubsubClient)
}

func TestGCPGKEPerf(t *testing.T) {
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

	env := harness.LoadEnv(t, "tfoutputs.json")
	env.ServiceURL = harness.StartPortForward(ctx, t, "outboxer", "deployment/outboxer", 8080)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	pubsubClient := harness.NewPubSubClient(ctx, t, env.ProjectID)

	harness.Perf(ctx, t, environment, env, db, pubsubClient, events, "../results")
}

func TestGCPGKELatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	env := harness.LoadEnv(t, "tfoutputs.json")
	env.ServiceURL = harness.StartPortForward(ctx, t, "outboxer", "deployment/outboxer", 8080)
	db := harness.StartCloudSQLProxy(ctx, t, env)
	pubsubClient := harness.NewPubSubClient(ctx, t, env.ProjectID)

	harness.Latency(ctx, t, environment, env, db, pubsubClient, 60, "../results")
}
