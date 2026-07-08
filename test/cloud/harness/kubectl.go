package harness

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"testing"
	"time"
)

// StartPortForward exposes a workload's port on localhost with
// `kubectl port-forward` and returns the local base URL. This is how the
// harness reaches the relay's /metrics inside a cluster, where the relay has
// no external URL. Requires kubectl configured for the target cluster.
func StartPortForward(ctx context.Context, t *testing.T, namespace string, target string, remotePort int) string {
	t.Helper()

	port := freePort(t)
	forwardCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(forwardCtx, "kubectl", "port-forward",
		"-n", namespace, target, fmt.Sprintf("%d:%d", port, remotePort))
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start kubectl port-forward (is kubectl installed and configured?): %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			_ = conn.Close()
			return fmt.Sprintf("http://127.0.0.1:%d", port)
		}
		if time.Now().After(deadline) {
			t.Fatalf("kubectl port-forward to %s did not become ready: %v", target, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
