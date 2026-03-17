# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build ./...

# Run
go run cmd/main.go

# Test
go test ./...

# Single package test
go test ./internal/producer/...

# Lint (if golangci-lint is installed)
golangci-lint run
```

## Architecture

This is a Kafka producer library/binary written in Go. The entry point is `cmd/main.go`, which loads config and sends a single `UserCommandMessage`.

**Key packages:**

- `internal/config` — loads YAML config via `cleanenv`. `MustLoad(path)` is the only entrypoint; it fatals on missing file or parse error.
- `internal/producer` — core Kafka producer logic. `KafkaProducer` wraps two `kafka.Writer` instances: one for the main topic and one for the DLQ (dead-letter queue).

**Message flow:**

1. `SendMessage` / `SendUserCommand` → `writeWithRetry` → on failure → `sendToDLQ`
2. Idempotency keys are MD5 hashes of `(payload, data)` and are set as both the Kafka message key and a header.
3. The DLQ writer appends `dlq-reason` and `dlq-timestamp` headers to the original message.

**Config:** YAML files live in `config/`. `prod.yml` is the active config (loaded by `main.go`); `example.yml` is a template. Config supports broker list, topic, DLQ topic, retries, backoff, batch timeout, and compression.

**`MessageProducer` interface** (`internal/producer/interface.go`) defines `SendMessage` and `Close` — intended as the abstraction boundary for swapping broker implementations.

**Compression:** `GetCompression(string)` maps `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"` to `kafka.Compression` constants; defaults to Snappy for unknown values.
