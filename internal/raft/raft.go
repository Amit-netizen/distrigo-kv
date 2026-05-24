// Package raft implements a subset of the Raft consensus algorithm sufficient
// for a replicated key-value store:
//
//   - Randomised election timeouts and RequestVote RPCs for leader election.
//   - AppendEntries RPCs for log replication and heartbeating.
//   - Commit-index advancement once a quorum acknowledges an entry.
//   - A pluggable FSM interface so the KV store can apply committed entries.
//
// Internal RPC transport is JSON-over-TCP on a dedicated raftAddr, keeping
// the RESP client port completely separate.
package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"
)

// -----------------------------------------------------------------------
// Public types
// -----------------------------------------------------------------------

// NodeState represents the Raft role of a node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"` // opaque; decoded by FSM
}

// FSM is implemented by the KV store.  Apply is called in-order, exactly once,
// for every committed log entry.
type FSM interface {
	Apply(entry LogEntry) error
}

// Config holds everything a Node needs to start.
type Config struct {
	// ID is a unique name for this node, e.g. "node1".
	ID string
	// RaftAddr is the TCP address this node listens on for Raft RPCs.
	RaftAddr string
	// Peers maps peer IDs to their RaftAddr.
	Peers map[string]string
	// FSM is called for each committed entry.
	FSM FSM

	// Tunable timeouts (zero → sane defaults).
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func (c *Config) defaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 600 * time.Millisecond
	}
}

// -----------------------------------------------------------------------
// RPC message types (JSON-serialised over TCP)
// -----------------------------------------------------------------------

type rpcType string

const (
	rpcRequestVote  rpcType = "RequestVote"
	rpcAppendEntry  rpcType = "AppendEntries"
	rpcVoteReply    rpcType = "VoteReply"
	rpcAppendReply  rpcType = "AppendReply"
)

type rpcEnvelope struct {
	Type    rpcType         `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type requestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type requestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type appendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type appendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"` // hint for fast log roll-back
}

// -----------------------------------------------------------------------
// Node
// -----------------------------------------------------------------------

// Node is a single participant in the Raft cluster.
type Node struct {
	mu sync.Mutex
	cfg Config

	// Persistent state (would be written to stable storage in production).
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state.
	commitIndex uint64
	lastApplied uint64
	state       NodeState
	leaderID    string

	// Leader-only volatile state (re-initialised after election).
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Channels / signalling.
	heartbeatCh  chan struct{} // reset election timer
	applyCh      chan LogEntry // committed entries queued here
	proposeCh    chan proposeReq // client proposals
	shutdownCh   chan struct{}

	ln net.Listener // RPC listener
}

type proposeReq struct {
	command []byte
	replyCh chan error
}

// NewNode creates and starts a Raft node.  It returns once the RPC listener
// is bound; the election loop starts in the background.
func NewNode(cfg Config) (*Node, error) {
	cfg.defaults()

	ln, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: listen %s: %w", cfg.RaftAddr, err)
	}

	n := &Node{
		cfg:         cfg,
		log:         []LogEntry{},
		state:       Follower,
		heartbeatCh: make(chan struct{}, 8),
		applyCh:     make(chan LogEntry, 256),
		proposeCh:   make(chan proposeReq, 64),
		shutdownCh:  make(chan struct{}),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		ln:          ln,
	}

	go n.rpcAcceptLoop()
	go n.mainLoop()
	go n.applyLoop()

	slog.Info("raft node started", "id", cfg.ID, "raftAddr", cfg.RaftAddr)
	return n, nil
}

// Propose submits a command to the cluster.  Blocks until committed by a
// quorum or returns an error (e.g. not leader, timeout).
func (n *Node) Propose(command []byte) error {
	req := proposeReq{
		command: command,
		replyCh: make(chan error, 1),
	}
	select {
	case n.proposeCh <- req:
	case <-n.shutdownCh:
		return fmt.Errorf("raft: node shut down")
	}
	return <-req.replyCh
}

// State returns the current NodeState (Follower / Candidate / Leader).
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// LeaderID returns the ID of the known leader, or "" if unknown.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderAddr returns the RaftAddr of the current leader, or "" if unknown.
func (n *Node) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return ""
	}
	return n.cfg.Peers[n.leaderID]
}

// Shutdown stops the node.
func (n *Node) Shutdown() {
	close(n.shutdownCh)
	n.ln.Close()
}

// -----------------------------------------------------------------------
// Main event loop
// -----------------------------------------------------------------------

func (n *Node) mainLoop() {
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower, Candidate:
			n.runElectionTimer()
		case Leader:
			n.runLeader()
		}

		select {
		case <-n.shutdownCh:
			return
		default:
		}
	}
}

func (n *Node) electionTimeout() time.Duration {
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// runElectionTimer waits for a heartbeat or times out and starts an election.
func (n *Node) runElectionTimer() {
	timeout := n.electionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-n.heartbeatCh:
			// Reset timer on any valid leader contact.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.electionTimeout())
		case <-timer.C:
			n.startElection()
			return
		}
	}
}

// startElection transitions to Candidate and sends RequestVote to all peers.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	term := n.currentTerm
	lastIndex, lastTerm := n.lastLogIndexTerm()
	n.mu.Unlock()

	slog.Info("starting election", "id", n.cfg.ID, "term", term)

	votes := 1 // vote for self
	var mu sync.Mutex
	total := len(n.cfg.Peers) + 1
	quorum := total/2 + 1

	var wg sync.WaitGroup
	for peerID, peerAddr := range n.cfg.Peers {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			args := requestVoteArgs{
				Term:         term,
				CandidateID:  n.cfg.ID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			var reply requestVoteReply
			if err := n.callRPC(addr, rpcRequestVote, args, &reply); err != nil {
				slog.Debug("RequestVote RPC failed", "peer", id, "err", err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}
			if reply.VoteGranted && n.state == Candidate && n.currentTerm == term {
				mu.Lock()
				votes++
				v := votes
				mu.Unlock()
				if v >= quorum {
					n.becomeLeader()
				}
			}
		}(peerID, peerAddr)
	}
	wg.Wait()
}

func (n *Node) becomeLeader() {
	// Caller holds n.mu.
	n.state = Leader
	n.leaderID = n.cfg.ID
	nextIdx := uint64(len(n.log)) + 1
	for id := range n.cfg.Peers {
		n.nextIndex[id] = nextIdx
		n.matchIndex[id] = 0
	}
	slog.Info("became leader", "id", n.cfg.ID, "term", n.currentTerm)
}

func (n *Node) stepDown(term uint64) {
	// Caller holds n.mu.
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
}

// -----------------------------------------------------------------------
// Leader loop: heartbeats + proposal handling
// -----------------------------------------------------------------------

func (n *Node) runLeader() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	// Pending proposals waiting for quorum acknowledgement.
	type pending struct {
		index   uint64
		replyCh chan error
	}
	var pendingProposals []pending

	for {
		select {
		case <-n.shutdownCh:
			return
		case req := <-n.proposeCh:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				req.replyCh <- fmt.Errorf("raft: not leader")
				continue
			}
			entry := LogEntry{
				Index:   uint64(len(n.log)) + 1,
				Term:    n.currentTerm,
				Command: req.command,
			}
			n.log = append(n.log, entry)
			n.mu.Unlock()
			pendingProposals = append(pendingProposals, pending{entry.Index, req.replyCh})
			// Replicate immediately rather than waiting for the heartbeat tick.
			n.replicateToAll()

		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.replicateToAll()

			// Check whether any pending proposals have reached quorum.
			n.mu.Lock()
			commit := n.commitIndex
			n.mu.Unlock()
			var still []pending
			for _, p := range pendingProposals {
				if p.index <= commit {
					p.replyCh <- nil
				} else {
					still = append(still, p)
				}
			}
			pendingProposals = still
		}
	}
}

// replicateToAll sends AppendEntries to every peer concurrently.
func (n *Node) replicateToAll() {
	for peerID, peerAddr := range n.cfg.Peers {
		go n.replicateToPeer(peerID, peerAddr)
	}
}

func (n *Node) replicateToPeer(peerID, peerAddr string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	// Entries to send: everything from nextIndex onwards.
	entries := make([]LogEntry, 0)
	if int(nextIdx) <= len(n.log) {
		entries = append(entries, n.log[nextIdx-1:]...)
	}
	args := appendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	var reply appendEntriesReply
	if err := n.callRPC(peerAddr, rpcAppendEntry, args, &reply); err != nil {
		slog.Debug("AppendEntries RPC failed", "peer", peerID, "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	if n.state != Leader {
		return
	}
	if reply.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatch
			n.nextIndex[peerID] = newMatch + 1
		}
		n.advanceCommitIndex()
	} else {
		// Back off by the conflict hint.
		if reply.ConflictIndex > 0 && reply.ConflictIndex < n.nextIndex[peerID] {
			n.nextIndex[peerID] = reply.ConflictIndex
		} else if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
	}
}

// advanceCommitIndex checks whether a new index has been replicated to a
// quorum and updates commitIndex accordingly.  Caller holds n.mu.
func (n *Node) advanceCommitIndex() {
	total := len(n.cfg.Peers) + 1
	quorum := total/2 + 1

	for idx := uint64(len(n.log)); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			break // only commit entries from current term (§5.4.2)
		}
		count := 1 // leader itself
		for _, m := range n.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count >= quorum {
			n.commitIndex = idx
			slog.Debug("advanced commit index", "leader", n.cfg.ID, "commitIndex", idx)
			break
		}
	}
}

// -----------------------------------------------------------------------
// Apply loop: deliver committed entries to the FSM in order
// -----------------------------------------------------------------------

func (n *Node) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log[n.lastApplied-1]
				n.mu.Unlock()
				if err := n.cfg.FSM.Apply(entry); err != nil {
					slog.Error("FSM apply error", "index", entry.Index, "err", err)
				}
				n.mu.Lock()
			}
			n.mu.Unlock()
		}
	}
}

// -----------------------------------------------------------------------
// RPC transport: JSON-over-TCP
// -----------------------------------------------------------------------

func (n *Node) rpcAcceptLoop() {
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			select {
			case <-n.shutdownCh:
				return
			default:
				slog.Error("raft: accept error", "err", err)
				continue
			}
		}
		go n.handleRPCConn(conn)
	}
}

func (n *Node) handleRPCConn(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	var env rpcEnvelope
	if err := dec.Decode(&env); err != nil {
		if err != io.EOF {
			slog.Debug("raft: decode envelope", "err", err)
		}
		return
	}

	switch env.Type {
	case rpcRequestVote:
		var args requestVoteArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply := n.handleRequestVote(args)
		enc.Encode(rpcEnvelope{Type: rpcVoteReply, Payload: mustMarshal(reply)})

	case rpcAppendEntry:
		var args appendEntriesArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply := n.handleAppendEntries(args)
		enc.Encode(rpcEnvelope{Type: rpcAppendReply, Payload: mustMarshal(reply)})
	}
}

// callRPC dials peerAddr, sends the request, and decodes the reply.
func (n *Node) callRPC(peerAddr string, rpcT rpcType, args, reply interface{}) error {
	conn, err := net.DialTimeout("tcp", peerAddr, 200*time.Millisecond)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(200 * time.Millisecond))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)

	payload, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if err := enc.Encode(rpcEnvelope{Type: rpcT, Payload: payload}); err != nil {
		return err
	}

	var resp rpcEnvelope
	if err := dec.Decode(&resp); err != nil {
		return err
	}
	return json.Unmarshal(resp.Payload, reply)
}

// -----------------------------------------------------------------------
// RPC handlers
// -----------------------------------------------------------------------

func (n *Node) handleRequestVote(args requestVoteArgs) requestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := requestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	lastIndex, lastTerm := n.lastLogIndexTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && logOK {
		n.votedFor = args.CandidateID
		n.currentTerm = args.Term
		reply.Term = n.currentTerm
		reply.VoteGranted = true
		n.sendHeartbeat()
	}
	return reply
}

func (n *Node) handleAppendEntries(args appendEntriesArgs) appendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := appendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	// Valid leader contact — reset election timer.
	n.sendHeartbeat()
	n.state = Follower
	n.leaderID = args.LeaderID
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}

	// Log consistency check.
	if args.PrevLogIndex > 0 {
		if uint64(len(n.log)) < args.PrevLogIndex {
			reply.ConflictIndex = uint64(len(n.log)) + 1
			return reply
		}
		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Find the first index of the conflicting term.
			conflictTerm := n.log[args.PrevLogIndex-1].Term
			ci := args.PrevLogIndex
			for ci > 1 && n.log[ci-2].Term == conflictTerm {
				ci--
			}
			reply.ConflictIndex = ci
			return reply
		}
	}

	// Append new entries, truncating any conflicting suffix.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if idx <= uint64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1]
			} else {
				continue // already have this entry
			}
		}
		n.log = append(n.log, entry)
	}

	// Advance commit index.
	if args.LeaderCommit > n.commitIndex {
		last := uint64(len(n.log))
		if args.LeaderCommit < last {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = last
		}
	}

	reply.Success = true
	return reply
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func (n *Node) lastLogIndexTerm() (uint64, uint64) {
	// Caller must hold n.mu.
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) sendHeartbeat() {
	// Non-blocking send so the RPC handler never blocks.
	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}
}

func mustMarshal(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
