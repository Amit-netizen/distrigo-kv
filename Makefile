run: build
	@./bin/distrigo-kv --listenAddr :5001

build:
	@go build -o bin/distrigo-kv ./cmd/distrigo-kv
