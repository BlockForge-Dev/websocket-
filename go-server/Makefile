.PHONY: setup format format-check lint test test-race build run check

setup:
	go mod download

format:
	gofmt -w ./cmd ./internal ./tests

format-check:
	@files="$$(gofmt -l ./cmd ./internal ./tests)"; \
	test -z "$$files" || (echo "Go files require formatting:"; echo "$$files"; exit 1)

lint:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

build:
	go build -o bin/server ./cmd/server

run:
	go run ./cmd/server

check: format-check lint test
