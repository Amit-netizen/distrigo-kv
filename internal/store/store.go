// Package store provides a thread-safe in-memory key-value store that
// implements the raft.FSM interface.  Mutations arrive as committed Raft log
// entries; the store decodes each entry and applies it atomically.
//
// Supported operations encoded in a LogEntry.Command:
//
//	SET <key> <value> [ttl_seconds]
//	DEL <key>
package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"distrigo-kv/internal/raft"
)

// -----------------------------------------------------------------------
// Command encoding (shared between store and server packages)
// -----------------------------------------------------------------------

type OpType string

const (
	OpSet OpType = "SET"
	OpDel OpType = "DEL"
)

// Command is the payload written into a Raft log entry.
type Command struct {
	Op  OpType `json:"op"`
	Key string `json:"key"`
	Val []byte `json:"val,omitempty"`
	TTL int64  `json:"ttl,omitempty"` // seconds; 0 = no expiry
}

// EncodeCommand serialises a Command to JSON bytes.
func EncodeCommand(c Command) ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserialises JSON bytes into a Command.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	return c, json.Unmarshal(b, &c)
}

// -----------------------------------------------------------------------
// In-memory item
// -----------------------------------------------------------------------

type item struct {
	value      []byte
	expiration int64 // unix nano; 0 = never
}

// -----------------------------------------------------------------------
// Store
// -----------------------------------------------------------------------

// Store is the KV store.  It implements raft.FSM.
type Store struct {
	mu   sync.RWMutex
	data map[string]item
}

// New creates and returns a Store, starting the background TTL cleanup loop.
func New() *Store {
	s := &Store{data: make(map[string]item)}
	go s.cleanupLoop()
	return s
}

// Apply implements raft.FSM.  It is called by the Raft apply loop in index
// order, after an entry has been committed by a quorum.
func (s *Store) Apply(entry raft.LogEntry) error {
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		return fmt.Errorf("store: decode command index %d: %w", entry.Index, err)
	}
	switch cmd.Op {
	case OpSet:
		var exp int64
		if cmd.TTL > 0 {
			exp = time.Now().Add(time.Duration(cmd.TTL) * time.Second).UnixNano()
		}
		s.mu.Lock()
		s.data[cmd.Key] = item{value: cmd.Val, expiration: exp}
		s.mu.Unlock()
	case OpDel:
		s.mu.Lock()
		delete(s.data, cmd.Key)
		s.mu.Unlock()
	default:
		return fmt.Errorf("store: unknown op %q at index %d", cmd.Op, entry.Index)
	}
	return nil
}

// Get retrieves a value.  A lazy expiration check is performed on read.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	it, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if it.expiration > 0 && time.Now().UnixNano() > it.expiration {
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		return nil, false
	}
	return it.value, true
}

// cleanupLoop periodically sweeps expired keys.
func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now().UnixNano()
		s.mu.Lock()
		for k, v := range s.data {
			if v.expiration > 0 && now > v.expiration {
				delete(s.data, k)
			}
		}
		s.mu.Unlock()
	}
}
