package main

import (
	"flag"
	"log"
	"strings"

	"distrigo-kv/internal/server"
)

// peers is a custom flag type for parsing "id=addr,id=addr,..." strings.
type peerList map[string]string

func (p *peerList) String() string {
	parts := make([]string, 0, len(*p))
	for k, v := range *p {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (p *peerList) Set(s string) error {
	if s == "" {
		return nil
	}
	for _, token := range strings.Split(s, ",") {
		kv := strings.SplitN(token, "=", 2)
		if len(kv) != 2 {
			continue
		}
		(*p)[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return nil
}

func main() {
	var (
		nodeID     = flag.String("id", "node1", "unique node ID in the cluster")
		clientAddr = flag.String("addr", ":5001", "RESP client listen address")
		raftAddr   = flag.String("raft", ":6001", "Raft RPC listen address")
		peers      = peerList{}
	)
	flag.Var(&peers, "peers",
		`comma-separated peer list: "node2=:6002,node3=:6003"`)
	flag.Parse()

	cfg := server.Config{
		NodeID:     *nodeID,
		ClientAddr: *clientAddr,
		RaftAddr:   *raftAddr,
		Peers:      peers,
	}

	srv := server.New(cfg)
	log.Fatal(srv.Start())
}
