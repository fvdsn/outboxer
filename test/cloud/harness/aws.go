package harness

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ECSServiceURL discovers the public IP of an ECS service's running task and
// returns the relay's base URL. Fargate tasks get a fresh IP per task, so
// this resolves at test time rather than deploy time. Uses the aws CLI, which
// the deploy scripts require anyway.
func ECSServiceURL(ctx context.Context, t *testing.T, region string, cluster string, service string) string {
	t.Helper()

	awsQuery := func(args ...string) string {
		full := append([]string{"--region", region, "--output", "text"}, args...)
		out, err := exec.CommandContext(ctx, "aws", full...).Output()
		if err != nil {
			t.Fatalf("aws %s: %v", strings.Join(args[:2], " "), err)
		}
		return strings.TrimSpace(string(out))
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		taskARN := awsQuery("ecs", "list-tasks", "--cluster", cluster,
			"--service-name", service, "--desired-status", "RUNNING",
			"--query", "taskArns[0]")
		if taskARN != "" && taskARN != "None" {
			eni := awsQuery("ecs", "describe-tasks", "--cluster", cluster, "--tasks", taskARN,
				"--query", "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value | [0]")
			if eni != "" && eni != "None" {
				ip := awsQuery("ec2", "describe-network-interfaces", "--network-interface-ids", eni,
					"--query", "NetworkInterfaces[0].Association.PublicIp")
				if ip != "" && ip != "None" {
					return fmt.Sprintf("http://%s:8080", ip)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no running task with a public IP found for %s/%s", cluster, service)
		}
		time.Sleep(3 * time.Second)
	}
}

// ConnectDB opens a direct Postgres connection, for stacks where the harness
// reaches the database over the network (RDS behind a security group) rather
// than through a proxy.
func ConnectDB(ctx context.Context, t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}
