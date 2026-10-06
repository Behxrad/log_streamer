# Log Streamer

A Go-based, gRPC-powered log aggregation service that ingests log entries and persists them to MongoDB with batched write optimization.

## Features

- **gRPC API** — receive log entries over a gRPC interface
- **MongoDB backend** — batched writes to MongoDB for efficient persistence
- **Graceful shutdown** — SIGINT/SIGTERM handling with configurable timeout
- **Configurable via environment variables** — no hardcoded values

## Architecture

```
┌─────────────┐       gRPC        ┌──────────────────────┐
│   Clients   │ ────────────────► │   gRPC Server        │
└─────────────┘                   │                      │
                                  │  Aggregator Service  │
                                  │  Watcher Service     │
                                  └──────────┬───────────┘
                                             │ MongoDB
                                     ┌───────▼────────┐
                                     │   MongoDB      │
                                     └────────────────┘
```

The service follows a layered architecture:

- **Adapter (inbound/outbound)** — gRPC transport layer and MongoDB repository
- **Service** — business logic for log aggregation and streaming
- **Port** — interfaces defining inbound and outbound contracts
- **Domain** — core data models (`LogEntry`)

## Quick Start

### Prerequisites

- Go 1.23+
- MongoDB running and accessible

### Setup

```bash
# Install dependencies
go mod download

# Configure environment variables (see Configuration below)

# Run
go run ./cmd/api/
```

## Configuration

All configuration is done through environment variables:

| Variable | Description | Default |
|---|---|---|
| `GRPC_ADDRESS` | gRPC server listen address | `:50051` |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | Shutdown timeout (seconds) | `120` |
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `LOG_AGGREGATOR_CHANNEL_SIZE` | Internal channel buffer size | `1000` |
| `LOG_AGGREGATOR_BATCH_SIZE` | Batch write size for MongoDB | `10` |
| `WATCHER_CHANNEL_SIZE` | Watcher channel buffer size | `1000` |

## gRPC API

The service exposes a `LogService` via gRPC with two RPCs.

### Proto Definition

```proto
service LogService {
  // Input stream for high-throughput ingestion
  rpc IngestLogs(stream LogChunk) returns (StatusResponse);

  // Output stream for real-time tailing
  rpc WatchLogs(WatchRequest) returns (stream LogEntry);
}
```

### Messages

| Message | Fields |
|---|---|
| `LogEntry` | `service_name`, `level`, `message`, `timestamp`, `metadata` |
| `LogChunk` | `logs` (repeated `LogEntry`) |
| `StatusResponse` | `success`, `message` |
| `WatchRequest` | `service_name` |

### Sample Requests

First, list available RPCs:

```bash
grpcurl -plaintext localhost:50051 list
```

**IngestLogs** — send a batch of log entries (client streaming):

```bash
grpcurl -plaintext -d '{
  "logs": [
    {
      "service_name": "auth-service",
      "level": "INFO",
      "message": "User logged in",
      "timestamp": 1728200000000,
      "metadata": {"user_id": "123"}
    },
    {
      "service_name": "auth-service",
      "level": "ERROR",
      "message": "Invalid token",
      "timestamp": 1728200001000,
      "metadata": {"user_id": "123", "token": "eyJ..."}
    }
  ]
}' localhost:50051 logger.LogService/IngestLogs
```

Expected response:

```json
{
  "success": true,
  "message": "parsed all"
}
```

**WatchLogs** — tail logs from a service in real time (server streaming):

```bash
grpcurl -plaintext -d '{"service_name": "auth-service"}' \
  localhost:50051 logger.LogService/WatchLogs
```

Expected output (streaming):

```
{
  "service_name": "auth-service",
  "level": "INFO",
  "message": "User logged in",
  "timestamp": 1728200000000,
  "metadata": {"user_id": "123"}
}
...
```

## Project Structure

```
├── api/proto/            # Protobuf definitions & generated code
├── cmd/api/              # Application entry point
├── configs/              # Configuration loading
├── internal/
│   ├── adapter/          # Inbound (gRPC) / outbound (MongoDB) adapters
│   ├── app/              # Application bootstrap
│   ├── domain/           # Core domain model (LogEntry)
│   ├── port/             # Interface definitions
│   └── service/          # Business logic
├── pkg/lib/              # Shared utilities
├── go.mod
└── go.sum
```