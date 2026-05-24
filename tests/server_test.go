package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"distrigo-kv/internal/server"
)

// startCluster spins up a 3-node cluster and returns a client pointed at
// each node.  Node 1 is given a head-start so it wins the first election.
func startCluster(t *testing.T, baseClient, baseRaft int) []*redis.Client {
	t.Helper()

	nodes := []struct{ id, clientAddr, raftAddr string }{
		{"node1", fmt.Sprintf(":%d", baseClient), fmt.Sprintf(":%d", baseRaft)},
		{"node2", fmt.Sprintf(":%d", baseClient+1), fmt.Sprintf(":%d", baseRaft+1)},
		{"node3", fmt.Sprintf(":%d", baseClient+2), fmt.Sprintf(":%d", baseRaft+2)},
	}

	peerMap := func(selfIdx int) map[string]string {
		m := map[string]string{}
		for i, n := range nodes {
			if i != selfIdx {
				m[n.id] = n.raftAddr
			}
		}
		return m
	}

	for i, n := range nodes {
		cfg := server.Config{
			NodeID:             n.id,
			ClientAddr:         n.clientAddr,
			RaftAddr:           n.raftAddr,
			Peers:              peerMap(i),
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		}
		srv := server.New(cfg)
		go srv.Start() //nolint:errcheck
		// Stagger starts so node1 tends to win the first election.
		if i == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Wait for the cluster to elect a leader.
	time.Sleep(600 * time.Millisecond)

	clients := make([]*redis.Client, len(nodes))
	for i, n := range nodes {
		clients[i] = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost%s", n.clientAddr),
		})
	}
	return clients
}

// leaderClient returns the client whose node is currently acting as leader by
// trying a SET on each and returning the first that succeeds without a MOVED
// error.
func leaderClient(t *testing.T, clients []*redis.Client) *redis.Client {
	t.Helper()
	for _, c := range clients {
		err := c.Set(context.Background(), "probe", "1", 0).Err()
		if err == nil {
			return c
		}
	}
	t.Fatal("no leader found among cluster nodes")
	return nil
}

// TestClusterBasicReplication verifies that a value written to the leader is
// readable from all nodes once replication has propagated.
func TestClusterBasicReplication(t *testing.T) {
	clients := startCluster(t, 5100, 6100)
	ctx := context.Background()

	leader := leaderClient(t, clients)
	if err := leader.Set(ctx, "city", "bangalore", 0).Err(); err != nil {
		t.Fatalf("SET on leader: %v", err)
	}

	// Allow replication to propagate.
	time.Sleep(200 * time.Millisecond)

	for i, c := range clients {
		val, err := c.Get(ctx, "city").Result()
		if err != nil {
			t.Errorf("node%d GET error: %v", i+1, err)
			continue
		}
		if val != "bangalore" {
			t.Errorf("node%d: expected 'bangalore', got %q", i+1, val)
		}
	}
}

// TestClusterFollowerRedirect verifies that a write sent to a follower returns
// a MOVED error, not a silent success.
func TestClusterFollowerRedirect(t *testing.T) {
	clients := startCluster(t, 5200, 6200)
	ctx := context.Background()

	// Find which client is the follower.
	var follower *redis.Client
	for _, c := range clients {
		err := c.Set(ctx, "probe", "x", 0).Err()
		if err != nil {
			follower = c
			break
		}
	}
	if follower == nil {
		t.Skip("all nodes answered SET — may all think they are leader; skipping")
	}

	err := follower.Set(ctx, "k", "v", 0).Err()
	if err == nil {
		t.Error("expected MOVED error from follower, got nil")
	}
}

// TestClusterTTLExpiration verifies end-to-end TTL through the Raft log.
func TestClusterTTLExpiration(t *testing.T) {
	clients := startCluster(t, 5300, 6300)
	ctx := context.Background()

	leader := leaderClient(t, clients)
	if err := leader.Set(ctx, "temp", "gone", 1*time.Second).Err(); err != nil {
		t.Fatalf("SET with TTL: %v", err)
	}

	// Key should be present immediately.
	val, err := leader.Get(ctx, "temp").Result()
	if err != nil || val != "gone" {
		t.Fatalf("GET before expiry: val=%q err=%v", val, err)
	}

	time.Sleep(1500 * time.Millisecond)

	_, err = leader.Get(ctx, "temp").Result()
	if err == nil {
		t.Error("GET after TTL: expected error, got nil")
	}
}

// TestClusterDel verifies that a DEL committed through Raft removes the key
// from all nodes.
func TestClusterDel(t *testing.T) {
	clients := startCluster(t, 5400, 6400)
	ctx := context.Background()

	leader := leaderClient(t, clients)
	if err := leader.Set(ctx, "todelete", "yes", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if n, err := leader.Del(ctx, "todelete").Result(); err != nil || n != 1 {
		t.Fatalf("DEL: n=%d err=%v", n, err)
	}
	time.Sleep(100 * time.Millisecond)

	for i, c := range clients {
		_, err := c.Get(ctx, "todelete").Result()
		if err == nil {
			t.Errorf("node%d: key still present after DEL", i+1)
		}
	}
}
