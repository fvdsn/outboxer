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

func TestGCPCloudRunSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	env := harness.LoadEnv(t, "tfoutputs.json")
	db := harness.StartCloudSQLProxy(ctx, t, env)
	pubsubClient := harness.NewPubSubClient(ctx, t, env.ProjectID)

	harness.Smoke(ctx, t, env, db, pubsubClient)
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

	env := harness.LoadEnv(t, "tfoutputs.json")
	db := harness.StartCloudSQLProxy(ctx, t, env)
	pubsubClient := harness.NewPubSubClient(ctx, t, env.ProjectID)

	harness.Perf(ctx, t, environment, env, db, pubsubClient, events, "../results")
}
