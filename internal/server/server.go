// Package server wires together the RESP TCP frontend, the Raft node, and the
// KV store.
//
// Write path (SET / DEL):
//
//	Client → RESP → Server.handleMessage → Raft.Propose (blocks until quorum)
//	                                               ↓
//	                                       FSM.Apply → Store.Apply
//	                                               ↓
//	                                       Server replies +OK to client
//
// Read path (GET):
//
//	Client → RESP → Server.handleMessage → Store.Get → reply
//
// Follower redirect:
//
//	If a write arrives at a follower the client receives
//	  -MOVED <leaderID> <leaderRaftAddr>
//	mirroring Redis Cluster MOVED semantics so clients can re-route.
package server

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/tidwall/resp"

	"distrigo-kv/internal/raft"
	"distrigo-kv/internal/store"
)

// -----------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------

// Config holds all tunables for a Server instance.
type Config struct {
	// ClientAddr is the TCP address for RESP client connections, e.g. ":5001".
	ClientAddr string
	// RaftAddr is the TCP address for inter-node Raft RPCs, e.g. ":6001".
	RaftAddr string
	// NodeID is a unique cluster-wide name for this node, e.g. "node1".
	NodeID string
	// Peers maps peer NodeIDs to their RaftAddrs.
	Peers map[string]string
	// ElectionTimeoutMin / Max allow overriding defaults in tests.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// -----------------------------------------------------------------------
// Internal message plumbing (mirrors original design)
// -----------------------------------------------------------------------

type message struct {
	cmd  command
	peer *peer
}

// -----------------------------------------------------------------------
// Server
// -----------------------------------------------------------------------

// Server is the top-level struct that owns the listener, the Raft node, and
// the KV store.
type Server struct {
	cfg      Config
	kv       *store.Store
	node     *raft.Node
	ln       net.Listener
	peers    map[*peer]bool
	addPeerCh chan *peer
	delPeerCh chan *peer
	msgCh    chan message
	quitCh   chan struct{}
}

// New creates a Server but does not start it.
func New(cfg Config) *Server {
	return &Server{
		cfg:       cfg,
		kv:        store.New(),
		peers:     make(map[*peer]bool),
		addPeerCh: make(chan *peer),
		delPeerCh: make(chan *peer),
		msgCh:     make(chan message, 128),
		quitCh:    make(chan struct{}),
	}
}

// Start binds the Raft RPC listener, then the RESP client listener, and runs
// the event loops.  It blocks until the RESP listener fails.
func (s *Server) Start() error {
	// Boot the Raft node.
	raftCfg := raft.Config{
		ID:                 s.cfg.NodeID,
		RaftAddr:           s.cfg.RaftAddr,
		Peers:              s.cfg.Peers,
		FSM:                s.kv,
		ElectionTimeoutMin: s.cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: s.cfg.ElectionTimeoutMax,
	}
	node, err := raft.NewNode(raftCfg)
	if err != nil {
		return fmt.Errorf("server: raft init: %w", err)
	}
	s.node = node

	// Bind the RESP listener.
	ln, err := net.Listen("tcp", s.cfg.ClientAddr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.cfg.ClientAddr, err)
	}
	s.ln = ln

	go s.loop()

	slog.Info("distrigo-kv node ready",
		"nodeID", s.cfg.NodeID,
		"clientAddr", s.cfg.ClientAddr,
		"raftAddr", s.cfg.RaftAddr,
	)

	return s.acceptLoop()
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	p := newPeer(conn, s.msgCh, s.delPeerCh)
	s.addPeerCh <- p
	if err := p.readLoop(); err != nil {
		slog.Error("peer read error", "err", err, "remoteAddr", conn.RemoteAddr())
	}
}

// loop is the single goroutine that serialises all state mutations.
func (s *Server) loop() {
	for {
		select {
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				slog.Error("handle message error", "err", err)
			}
		case <-s.quitCh:
			return
		case p := <-s.addPeerCh:
			slog.Info("client connected", "addr", p.conn.RemoteAddr())
			s.peers[p] = true
		case p := <-s.delPeerCh:
			slog.Info("client disconnected", "addr", p.conn.RemoteAddr())
			delete(s.peers, p)
		}
	}
}

// -----------------------------------------------------------------------
// Command dispatch
// -----------------------------------------------------------------------

func (s *Server) handleMessage(msg message) error {
	w := resp.NewWriter(msg.peer.conn)

	switch v := msg.cmd.(type) {

	case clientCmd:
		return w.WriteString("OK")

	case helloCmd:
		spec := map[string]string{
			"server":  "distrigo-kv",
			"node":    s.cfg.NodeID,
			"role":    s.node.State().String(),
			"version": "2.0",
		}
		_, err := msg.peer.send(respWriteMap(spec))
		return err

	case getCmd:
		val, ok := s.kv.Get(string(v.key))
		if !ok {
			// Redis convention: nil bulk string for missing key.
			_, err := msg.peer.send([]byte("$-1\r\n"))
			return err
		}
		return w.WriteString(string(val))

	case setCmd:
		return s.proposeWrite(msg.peer, store.Command{
			Op:  store.OpSet,
			Key: string(v.key),
			Val: v.val,
			TTL: v.ttl,
		})

	case delCmd:
		return s.proposeWrite(msg.peer, store.Command{
			Op:  store.OpDel,
			Key: string(v.key),
		})
	}
	return nil
}

// proposeWrite encodes cmd, sends it through Raft, and replies to the peer.
func (s *Server) proposeWrite(p *peer, cmd store.Command) error {
	// Redirect if we are not the leader.
	if s.node.State() != raft.Leader {
		leaderID := s.node.LeaderID()
		leaderAddr := s.node.LeaderAddr()
		if leaderAddr == "" {
			leaderAddr = "unknown"
		}
		// MOVED error mirrors Redis Cluster semantics.
		errMsg := fmt.Sprintf("MOVED %s %s", leaderID, leaderAddr)
		_, err := p.send([]byte("-" + errMsg + "\r\n"))
		return err
	}

	payload, err := store.EncodeCommand(cmd)
	if err != nil {
		return err
	}
	if err := s.node.Propose(payload); err != nil {
		_, sendErr := p.send([]byte("-ERR " + err.Error() + "\r\n"))
		if sendErr != nil {
			return sendErr
		}
		return err
	}
	w := resp.NewWriter(p.conn)
	return w.WriteString("OK")
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func respWriteMap(m map[string]string) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, fmt.Sprintf("%%%d\r\n", len(m))...)
	for k, v := range m {
		buf = append(buf, fmt.Sprintf("+%s\r\n+%s\r\n", k, v)...)
	}
	return buf
}


