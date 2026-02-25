package main

import (
	"sync"
	"time"
)

type item struct {
	value      []byte
	expiration int64 // unix nano timestamp, 0 = no expiration
}

type KV struct {
	mu   sync.RWMutex
	data map[string]item
}

func NewKV() *KV {
	kv := &KV{
		data: make(map[string]item),
	}

	// start background cleanup worker
	go kv.cleanupLoop()

	return kv
}

// Set without TTL
func (kv *KV) Set(key, val []byte) error {
	return kv.SetWithTTL(key, val, 0)
}

// Set with TTL duration
func (kv *KV) SetWithTTL(key, val []byte, ttl time.Duration) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var exp int64 = 0
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}

	kv.data[string(key)] = item{
		value:      val,
		expiration: exp,
	}

	return nil
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	kv.mu.RLock()
	it, ok := kv.data[string(key)]
	kv.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// check expiration
	if it.expiration > 0 && time.Now().UnixNano() > it.expiration {
		kv.mu.Lock()
		delete(kv.data, string(key))
		kv.mu.Unlock()
		return nil, false
	}

	return it.value, true
}

func (kv *KV) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {

		now := time.Now().UnixNano()

		kv.mu.Lock()

		for k, v := range kv.data {

			if v.expiration > 0 && now > v.expiration {

				delete(kv.data, k)

			}
		}

		kv.mu.Unlock()
	}
}