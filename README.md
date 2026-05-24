# DistriGo-KV

A Redis-compatible, replicated key-value store built in Go. A 3-node cluster
uses a from-scratch Raft consensus implementation for leader election and log
replication, so every committed write reaches a quorum of nodes before the
client receives `+OK`.

Built to demonstrate the class of problems Apple's Storage Infrastructure &
Reliability team works on: multi-node coordination across geographically
dispersed data centers, hardware dropout handling, and consistent state across
replicas at scale.

---

## Contents

- [Architecture](#architecture)
- [Raft Implementation](#raft-implementation)
- [Failure Scenarios](#failure-scenarios)
- [Quick Start — Docker](#quick-start--docker)
- [Quick Start — Local Build](#quick-start--local-build)
- [Screenshots](#screenshots)
- [Running Tests](#running-tests)
- [Package Layout](#package-layout)
- [CLI Flags](#cli-flags)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DistriGo-KV Cluster                           │
│                                                                     │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐     │
│  │    node1      │     │    node2      │     │    node3      │     │
│  │  (LEADER)     │     │  (follower)   │     │  (follower)   │     │
│  │               │     │               │     │               │     │
│  │ RESP  :5001   │     │ RESP  :5002   │     │ RESP  :5003   │     │
│  │ Raft  :6001   │◄───►│ Raft  :6002   │◄───►│ Raft  :6003   │     │
│  └──────┬────────┘     └───────────────┘     └───────────────┘     │
│         │  AppendEntries RPC (JSON/TCP)                             │
└─────────┼───────────────────────────────────────────────────────────┘
          │
    redis-cli / go-redis
```

Two TCP ports per node are intentional. The RESP port serves external clients
(redis-cli, application code). The Raft port handles internal cluster RPCs and
is never exposed outside the cluster network — mirroring how production storage
systems isolate replication traffic from client traffic on separate interfaces.

### Write path

1. Client sends `SET key value` over RESP to any node.
2. If the node is a **follower**, it returns `-MOVED <leaderID> <raftAddr>` so
   the client can retry against the leader.
3. The **leader** appends the command to its local Raft log.
4. `AppendEntries` RPCs fan out to both followers concurrently.
5. Once **2 of 3 nodes** acknowledge the entry, the leader advances
   `commitIndex`.
6. The FSM apply loop delivers the committed entry to the KV store on every
   node, in log-index order.
7. The leader replies `+OK` to the client.

### Read path

`GET` is served from the local KV store without a Raft round-trip, giving
low-latency reads at the cost of possible stale reads on followers. For
linearisable reads, route `GET` to the leader. A read-index protocol (§6.4 of
the Raft paper) is the standard production extension.

---

## Raft Implementation

The `internal/raft` package implements core Raft from scratch — no external
consensus library.

| Mechanism | Implementation |
|---|---|
| **Leader election** | Randomised timeouts (300–600 ms); `RequestVote` RPCs sent to all peers concurrently; wins on quorum |
| **Log replication** | `AppendEntries` carries new entries + heartbeat; followers check `prevLogIndex`/`prevLogTerm` before appending |
| **Commit rule** | `commitIndex` advances only when an entry from the **current term** is on a quorum of nodes (§5.4.2) |
| **Fast log roll-back** | Rejected followers return `conflictIndex` — first index of the conflicting term — so the leader skips to the right rollback point in one round-trip |
| **FSM apply loop** | Separate goroutine drains `[lastApplied+1 … commitIndex]`, calling `FSM.Apply` in strict index order |
| **Heartbeat** | Leader ticks every 100 ms; followers reset their election timers on any valid `AppendEntries` |

### RPC transport

Inter-node RPCs are **JSON-over-TCP** on the dedicated `raftAddr` port. Each
call is a short-lived connection:

```
dial → encode rpcEnvelope{type, payload} → decode reply → close
```

This makes the wire format inspectable with `nc` during development and keeps
the dependency tree minimal. The transport layer is fully decoupled from the
Raft logic — replacing it with gRPC + protobuf is a transport-layer change
only.

---

## Failure Scenarios

This section maps to the hardware dropout and network partition concerns Apple's
storage team operates under at data-center scale.

### Scenario 1: Follower dies mid-replication

**Setup:** node1 is leader, node2 is a follower that has acknowledged log index
5, node3 crashes after acknowledging index 3.

```
node1 (leader)  log: [1,2,3,4,5,6]   commitIndex=5
node2           log: [1,2,3,4,5]     (one entry behind)
node3           DEAD
```

**What happens:**

1. node1 continues to attempt `AppendEntries` to node3 on each heartbeat tick
   (every 100 ms). All calls fail with a connection-refused dial error; the
   error is logged and the goroutine exits cleanly — no panic, no stuck lock.
2. node1 can still advance `commitIndex` for index 6 because node2 alone
   provides the second acknowledgement needed for a 3-node quorum (2 of 3).
   **Writes continue uninterrupted.**
3. When node3 restarts, node1's next heartbeat carries `prevLogIndex=3` and
   the entries `[4,5,6,...]`. node3 performs the consistency check, appends
   the missing entries, and catches up. No manual intervention required.

**Key invariant:** the cluster tolerates `floor(n/2)` failures. With 3 nodes
that means 1 node can be down and reads/writes continue normally.

---

### Scenario 2: Leader dies with uncommitted entries

**Setup:** node1 (leader, term 2) has appended log index 7 locally and sent
`AppendEntries` to node2, but crashes before node2 replies — and before node3
receives the RPC at all.

```
node1 (leader)  DEAD   log: [..., 7]  term=2
node2           log: [..., 7]         (received from node1, not yet committed)
node3           log: [..., 6]         (never received index 7)
```

**What happens:**

1. node2 and node3 stop receiving heartbeats. After their randomised election
   timeouts fire (300–600 ms), one of them — say node2 — starts an election.
2. node2 increments its term to 3 and sends `RequestVote` to node3.
   node2's log is at least as up-to-date as node3's (it has index 7), so the
   log-completeness check passes and node3 grants its vote.
3. node2 becomes leader for term 3.
4. node2 issues a heartbeat / no-op `AppendEntries` to establish authority.
   The `prevLogIndex`/`prevLogTerm` check on node3 detects the gap: node3 is
   missing index 7. node3 returns `conflictIndex=7` and node2 retransmits that
   entry.
5. **Index 7 is now on both node2 and node3.** node2 advances `commitIndex` to
   7 and the FSM applies the entry on both nodes. The write that was "in-flight"
   when node1 died is successfully committed.
6. If node1 restarts, it receives an `AppendEntries` from node2 (term 3) with
   a higher term. It steps down to follower, rolls its log forward, and
   rejoins the cluster.

**Key invariant:** an entry that reaches a quorum before the leader dies will
always be committed by the new leader (Raft's Leader Completeness property,
§5.4). Entries that only existed on the dead leader's log are safely discarded.

---

### Scenario 3: Network partition (split-brain prevention)

**Setup:** a network partition isolates node1 from node2 and node3. node1
still thinks it is the leader.

```
Partition A:  node1 (old leader, term 2) — isolated
Partition B:  node2 + node3
```

**What happens:**

1. node1 cannot reach node2 or node3. It continues to accept client writes and
   appends them to its log, but it can never advance `commitIndex` because it
   cannot collect a quorum acknowledgement. **Client writes to node1 block
   indefinitely** — `Propose` never returns `+OK`.
2. node2 and node3 stop receiving heartbeats from node1. After a randomised
   timeout, one of them starts an election for term 3 and wins with 2 votes.
   This partition has a quorum and can commit writes.
3. When the partition heals, node1 receives an `AppendEntries` from the new
   leader (term 3 > term 2). `handleAppendEntries` calls `stepDown(3)`, and
   node1 becomes a follower. Any entries node1 appended but never committed are
   truncated via the log consistency check and replaced by the authoritative log
   from the new leader.

**Key invariant:** at most one partition can contain a quorum, so at most one
leader can commit writes at any time. Raft's term mechanism is what prevents
two nodes from simultaneously believing they are the committed-write authority.

---

### Scenario 4: Slow follower in a geo-distributed cluster

**Setup:** node3 is in a distant data center. Its `AppendEntries` round-trip
takes 80 ms; nodes 1 and 2 are co-located with a 2 ms round-trip.

**What happens:**

1. node1 fans out `AppendEntries` to node2 and node3 **concurrently** (see
   `replicateToAll`).
2. node2 replies in ~2 ms. That is the second acknowledgement — quorum is
   reached. `advanceCommitIndex` fires and the client gets `+OK` in ~2 ms.
3. node3's acknowledgement arrives ~80 ms later. `matchIndex[node3]` is
   updated; no further action needed.
4. node3 never blocks client-facing writes. It only affects availability: if
   both node1 and node3 die simultaneously, node2 alone cannot form a quorum.

**Key invariant:** write latency is determined by the **fastest quorum**, not
the slowest node. One distant replica adds durability without adding to the
common-case write latency.

---

## Quick Start — Docker

The fastest way to see the cluster running. Requires Docker and Docker Compose.

```bash
git clone https://github.com/Amit-netizen/DistriGo-KV.git
cd DistriGo-KV
docker compose up
```

All three nodes start, elect a leader (watch the logs), and expose their RESP
ports on localhost.

```bash
# Write to whichever node won the election (check logs — election is non-deterministic)
# In this example node2 won; adjust port accordingly
redis-cli -p 5001 SET fruit mango
# OK

# Read from any node — propagates within ~100 ms
redis-cli -p 5002 GET fruit
# "mango"

# Write to a follower — MOVED redirect
redis-cli -p 5003 SET k v
# (error) MOVED node1 :6001

# Simulate a node failure
docker compose stop node2

# Writes still succeed — node1 + node3 form a quorum
redis-cli -p 5001 SET resilient yes
# OK

# Bring node2 back — it catches up automatically
docker compose start node2
```

---

## Quick Start — Local Build

**Requirements:** Go 1.22+

```bash
git clone https://github.com/Amit-netizen/DistriGo-KV.git
cd DistriGo-KV
make build
```

### Launch a 3-node cluster (3 terminals)

```bash
# Terminal 1
make run-node1

# Terminal 2
make run-node2

# Terminal 3
make run-node3
```

Or launch all three in the background:

```bash
make cluster
```

### Connect with redis-cli

```bash
redis-cli -p 5001

127.0.0.1:5001> SET city bangalore
OK
127.0.0.1:5001> GET city
"bangalore"

# TTL — key expires after 10 seconds, committed through Raft
127.0.0.1:5001> SET session:abc token 10
OK

127.0.0.1:5001> DEL city
(integer) 1

# HELLO returns node identity and current role
127.0.0.1:5001> HELLO 2
```

---

---

## Screenshots

All screenshots are captured from a live run on Windows 11 + Docker Desktop.

| File | What it shows |
|---|---|
| [DG-1](Screenshots/DG-1.png) | All 3 nodes start; `node2 became leader term=1` — Raft election completes in under 1 s |
| [DG-2](Screenshots/DG-2.png) | `SET city Bengaluru` → `OK`, `GET city` → `"Bengaluru"` on the leader |
| [DG-3](Screenshots/DG-3.png) | TTL (`SET session:abc token 10`), `DEL`, and `HELLO 2` returning `node=node2 role=leader` |
| [DG-4](Screenshots/DG-4.png) | Write to follower (port 5003): `(error) MOVED node2 node2:6002` — redirect working |
| [DG-5](Screenshots/DG-5.png) | `docker compose stop node3` — container stopped in 0.6 s |
| [DG-6](Screenshots/DG-6.png) | `SET resilient yes` → `OK` with node3 down — cluster survives on 2-node quorum |
| [DG-7](Screenshots/DG-7.png) | Write to node1 follower (port 5001): `MOVED` to node2 — consistent across all followers |
| [DG-8.1](Screenshots/DG-8.1.png) | `stop node3` then `start node3` — node rejoins in 0.4 s |
| [DG-8.2](Screenshots/DG-8.2.png) | Cluster log: `node3 exited` → `node1 became leader term=3` — automatic re-election after dropout |
| [DG-9](Screenshots/DG-9.png) | `docker compose down` — all 3 containers cleanly removed |


## Running Tests

```bash
make test
# or
go test -v -timeout 30s ./tests/...
```

The test suite starts four independent 3-node clusters on distinct port ranges
and verifies:

| Test | What it checks |
|---|---|
| `TestClusterBasicReplication` | Value written to leader is readable from all 3 nodes after replication |
| `TestClusterFollowerRedirect` | Write to a follower returns a MOVED error |
| `TestClusterTTLExpiration` | TTL committed through Raft expires key on all nodes |
| `TestClusterDel` | DEL propagates via Raft and removes key from all nodes |

---

## Package Layout

```
distrigo-kv/
├── cmd/distrigo-kv/
│   └── main.go              CLI flags (-id, -addr, -raft, -peers)
├── internal/
│   ├── raft/
│   │   └── raft.go          Raft node: election, replication, FSM apply loop
│   ├── store/
│   │   └── store.go         KV store implementing raft.FSM; TTL + lazy eviction
│   └── server/
│       ├── server.go        RESP listener, command dispatch, Raft integration
│       └── peer.go          Per-connection RESP parser, typed command structs
├── tests/
│   └── server_test.go       Cluster integration tests (go-redis client)
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── go.mod
```

---

## CLI Flags

| Flag | Default | Description |
|---|---|---|
| `-id` | `node1` | Unique node ID within the cluster |
| `-addr` | `:5001` | RESP client listen address |
| `-raft` | `:6001` | Raft RPC listen address |
| `-peers` | `""` | `id=raftAddr` pairs, e.g. `"node2=:6002,node3=:6003"` |

---

## Tech Stack

- **Go 1.22** — goroutines, channels, `sync.RWMutex`, `encoding/json`
- **Raft consensus** — implemented from scratch in `internal/raft`
- [`tidwall/resp`](https://github.com/tidwall/resp) — RESP2 wire-format codec
- [`go-redis/v9`](https://github.com/redis/go-redis) — integration test client

---

## Design Decisions & Trade-offs

**Why JSON-over-TCP instead of gRPC for Raft RPCs?**
Keeps the dependency tree minimal and makes the wire format inspectable with
`nc` or Wireshark during development. The transport is fully decoupled from the
Raft logic — replacing it with gRPC + protobuf is a transport-layer change only,
which is also the correct production answer.

**Why not persist the Raft log?**
The log is intentionally in-memory — the focus is on demonstrating consensus
mechanics (election, quorum commit, fast log roll-back, split-brain prevention).
Adding a WAL-backed log store (`internal/raft/log_store.go`) is a
straightforward extension and the natural next step toward crash recovery.
This maps directly to the block storage layer work Apple's team does: durable
log storage is where the KV store touches block I/O.

**Stale reads on followers**
`GET` is served locally without a leader round-trip. This is the right default
for a read-heavy geo-distributed deployment where followers in nearby data
centers should absorb local read traffic. Strict linearisability requires either
routing reads to the leader or implementing the read-index protocol (§6.4 of
the Raft paper) to confirm the leader hasn't been deposed before serving a read.

**Why the `commitIndex` can only advance on current-term entries (§5.4.2)**
A leader cannot safely commit entries from a previous term by counting replicas
alone — a subsequent leader could overwrite them. The new leader instead commits
a no-op entry from its own term first, which transitively commits all prior
entries. This is the subtle correctness constraint that most Raft
re-implementations get wrong.
