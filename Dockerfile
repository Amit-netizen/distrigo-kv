# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/distrigo-kv ./cmd/distrigo-kv

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /bin/distrigo-kv /distrigo-kv

# RESP client port (redis-cli / application)
EXPOSE 5001
# Raft RPC port (internal cluster only)
EXPOSE 6001

ENTRYPOINT ["/distrigo-kv"]
