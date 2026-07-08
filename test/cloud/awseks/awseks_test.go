//go:build cloud

// Package awseks_test drives the harness scenarios against the
// deploy/aws-eks stack. Bring the stack up first:
//
//	just cloud-aws-eks-up
//	just cloud-aws-eks-test
//	just cloud-aws-eks-down
package awseks_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/fvdsn/outboxer/test/cloud/harness"
)

const environment = "aws-eks"

func setup(ctx context.Context, t *testing.T) (harness.Env, harness.MessageSink, *harness.Metrics) {
	t.Helper()
	env := harness.LoadEnv(t, "tfoutputs.json")
	// Inside a cluster the relay has no external URL; reach its health port
	// through kubectl port-forward.
	env.ServiceURL = harness.StartPortForward(ctx, t, "outboxer", "deployment/outboxer", 8080)
	client := harness.NewSQSClient(ctx, t, env.Region)
	sink := harness.NewSQSSink(client, env.QueueURL, env.FifoQueueURL)
	return env, sink, harness.NewPlainMetrics(env)
}

func smokeEvents(env harness.Env) harness.SmokeEvents {
	return harness.SmokeEvents{
		Unordered: func(payload string, i int) harness.Event {
			destination := env.QueueURL // explicit destination
			if i%2 == 0 {
				destination = "" // resolved by DEFAULT_SQS_QUEUE_URL
			}
			return harness.Event{Payload: payload, Destination: destination}
		},
		Ordered: func(payload string, key string, _ int) harness.Event {
			return harness.Event{
				Payload:     payload,
				Target:      "sqs",
				Destination: env.FifoQueueURL,
				OrderingKey: key,
			}
		},
		Poison: func(_ string, _ int) harness.Event {
			// An empty body is content poison for SQS.
			return harness.Event{Payload: "", Destination: env.QueueURL}
		},
	}
}

func dsn(env harness.Env) string {
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=require",
		env.DBUser, env.DBPassword, env.DBHost, env.DBName)
}

func TestAWSEKSSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	env, sink, metrics := setup(ctx, t)
	db := harness.ConnectDB(ctx, t, dsn(env))
	harness.Smoke(ctx, t, env, db, sink, metrics, smokeEvents(env))
}

func TestAWSEKSPerf(t *testing.T) {
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
	db := harness.ConnectDB(ctx, t, dsn(env))
	harness.Perf(ctx, t, environment, db, sink, metrics, events, "../results")
}

func TestAWSEKSLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	env, sink, metrics := setup(ctx, t)
	db := harness.ConnectDB(ctx, t, dsn(env))
	harness.Latency(ctx, t, environment, db, sink, metrics, 60, "../results")
}
