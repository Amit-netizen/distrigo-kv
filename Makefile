.PHONY: build run-node1 run-node2 run-node3 cluster test fmt vet

# ─── Build ───────────────────────────────────────────────────────────────────
build:
	go build -o bin/distrigo-kv ./cmd/distrigo-kv

# ─── Run a 3-node local cluster ──────────────────────────────────────────────
# Open three terminals and run each target in sequence.

run-node1:
	./bin/distrigo-kv \
	  -id node1 \
	  -addr :5001 \
	  -raft :6001 \
	  -peers "node2=:6002,node3=:6003"

run-node2:
	./bin/distrigo-kv \
	  -id node2 \
	  -addr :5002 \
	  -raft :6002 \
	  -peers "node1=:6001,node3=:6003"

run-node3:
	./bin/distrigo-kv \
	  -id node3 \
	  -addr :5003 \
	  -raft :6003 \
	  -peers "node1=:6001,node2=:6002"

# Convenience: launch all three in the background (for local testing only).
cluster: build
	./bin/distrigo-kv -id node1 -addr :5001 -raft :6001 -peers "node2=:6002,node3=:6003" &
	./bin/distrigo-kv -id node2 -addr :5002 -raft :6002 -peers "node1=:6001,node3=:6003" &
	./bin/distrigo-kv -id node3 -addr :5003 -raft :6003 -peers "node1=:6001,node2=:6002" &
	@echo "Cluster started. Use 'redis-cli -p 5001' to connect."
	@echo "Kill with: pkill -f distrigo-kv"

# ─── Tests ───────────────────────────────────────────────────────────────────
test:
	go test -v -timeout 30s ./tests/...

# ─── Quality ─────────────────────────────────────────────────────────────────
fmt:
	gofmt -w .

vet:
	go vet ./...

# ─── Docker ──────────────────────────────────────────────────────────────────
docker-up:
	docker compose up --build

docker-down:
	docker compose down

# Simulate a hardware dropout while keeping a quorum alive.
# Usage: make docker-kill-node NODE=node2
docker-kill-node:
	docker compose stop $(NODE)

docker-restart-node:
	docker compose start $(NODE)
