package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// StartPortForward exposes a workload's port on localhost with
// `kubectl port-forward` and returns the local base URL. This is how the
// harness reaches the relay's /metrics inside a cluster, where the relay has
// no external URL. Requires kubectl configured for the target cluster.
//
// kubectl is restarted if it exits before the tunnel is up: on a freshly
// created cluster the first connection can fail transiently (exec credential
// plugins are cold, the API server has just come up), and a port-forward
// that dies takes the whole scenario with it.
func StartPortForward(ctx context.Context, t *testing.T, namespace string, target string, remotePort int) string {
	t.Helper()

	port := freePort(t)
	overallDeadline := time.Now().Add(2 * time.Minute)
	var lastDetail string
	for {
		forwardCtx, cancel := context.WithCancel(ctx)
		var stderr bytes.Buffer
		cmd := exec.CommandContext(forwardCtx, "kubectl", "port-forward",
			"-n", namespace, target, fmt.Sprintf("%d:%d", port, remotePort))
		cmd.Stdout = io.Discard
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatalf("start kubectl port-forward (is kubectl installed and configured?): %v", err)
		}
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()

		attemptDeadline := time.Now().Add(20 * time.Second)
	dial:
		for {
			select {
			case err := <-exited:
				lastDetail = fmt.Sprintf("kubectl exited: %v (%s)", err, strings.TrimSpace(stderr.String()))
				break dial
			default:
			}
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
			if err == nil {
				_ = conn.Close()
				t.Cleanup(func() {
					cancel()
					<-exited
				})
				return fmt.Sprintf("http://127.0.0.1:%d", port)
			}
			lastDetail = err.Error()
			if time.Now().After(attemptDeadline) {
				break dial
			}
			time.Sleep(200 * time.Millisecond)
		}

		cancel()
		<-exited
		if time.Now().After(overallDeadline) {
			t.Fatalf("kubectl port-forward to %s did not become ready: %s", target, lastDetail)
		}
		t.Logf("kubectl port-forward to %s not ready, restarting it: %s", target, lastDetail)
		time.Sleep(2 * time.Second)
	}
}
