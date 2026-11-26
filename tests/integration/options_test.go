// Package integration — options that reach the store.
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FDK0901/dureq/internal/client"
	"github.com/FDK0901/dureq/internal/monitor"
	"github.com/FDK0901/dureq/internal/server"
	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// How many workflows or batches a name filter looks at can be set on a server and on a client,
// and a list says so when it has not looked at all.
func TestMaxNameScan_IsSet(t *testing.T) {
	flushDB(t)
	opts := []server.Option{server.WithNodeID("scan"), server.WithKeyPrefix(keyPrefix), server.WithShutdownTimeout(3 * time.Second), server.WithMaxNameScan(7)}
	copts := []client.Option{client.WithKeyPrefix(keyPrefix), client.WithMaxNameScan(9)}
	if isStandalone() {
		opts = append(opts, server.WithRedisURL(os.Getenv("REDIS_URL")), server.WithRedisPassword(redisPW), server.WithRedisDB(redisDB()))
		copts = append(copts, client.WithRedisURL(os.Getenv("REDIS_URL")), client.WithRedisPassword(redisPW), client.WithRedisDB(redisDB()))
	} else {
		opts = append(opts, server.WithRedisCluster(redisAddrs()), server.WithRedisPassword(redisPW))
		copts = append(copts, client.WithClusterAddrs(redisAddrs()), client.WithRedisPassword(redisPW))
	}
	srv, err := server.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })
	cli, err := client.New(copts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)
	if n := srv.Store().Config().MaxNameScan; n != 7 {
		t.Errorf("server: %d, want 7", n)
	}
	if n := cli.Store().Config().MaxNameScan; n != 9 {
		t.Errorf("client: %d, want 9", n)
	}
	if n := newClient(t).Store().Config().MaxNameScan; n != 100_000 {
		t.Errorf("by default: %d", n)
	}

	ctx := context.Background()
	for i := range 10 {
		if _, err := srv.Store().SaveWorkflow(ctx, &types.WorkflowInstance{ID: fmt.Sprint(i), WorkflowName: "w", Status: types.WorkflowStatusRunning, CreatedAt: time.Now().Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	svc := monitor.NewAPIService(srv.Store(), srv.Dispatcher(), nil)
	t.Cleanup(svc.Shutdown)
	name := "w"
	if _, total, more, err := svc.ListWorkflowsPage(ctx, store.WorkflowFilter{WorkflowName: &name, Limit: 3}); err != nil || total != 7 || !more {
		t.Errorf("%d found, more %v, %v; want 7 of those looked at, and more", total, more, err)
	}
}
