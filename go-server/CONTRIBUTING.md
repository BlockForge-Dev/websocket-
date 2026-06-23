# Contributing

## Development workflow

1. Install the Go version declared in `go.mod`.
2. Run `scripts\setup.cmd` or `make setup`.
3. Make one focused change.
4. Run `scripts\check.cmd` or `make check`.
5. Add tests and documentation for changed behavior.

## Definition of a valid change

- Go files are formatted with `gofmt`.
- `go vet ./...` passes.
- `go test ./...` passes.
- Public behavior and configuration are documented.
- No secrets, `.env` files, or machine-specific paths are committed.
- Work remains inside the current milestone unless a scope change is explicit.

## Package boundaries

- `cmd/server` composes dependencies and owns process lifecycle.
- `internal/config` owns configuration.
- `internal/httpapi` owns HTTP concerns and connection establishment.
- `internal/realtime` owns sessions and transient routing.
- `internal/observability` owns logs and metrics integration.

Do not place application-domain rules in the realtime hub. Do not perform
normal socket writes outside the session write path.

## Tests

Prefer behavioral tests and lifecycle invariants over private implementation
coverage. Concurrency-sensitive work must also pass:

```sh
go test -race ./...
```

When every Milestone 0 definition-of-done item is satisfied, its repository
marker is `milestone-00-foundation`.
