# qweb

A Rust web API gateway that provides HTTP and WebSocket interfaces for interacting with [qcore-rs](https://github.com/rqure/qcore-rs) through [qlib-rs](https://github.com/rqure/qlib-rs)'s `AsyncStoreProxy`.

## Features

- **HTTP REST API** - Standard HTTP endpoints for CRUD operations
- **WebSocket Support** - Persistent WebSocket connections for real-time interactions
- **Full qcore-rs Integration** - Complete access to entity management, field operations, and schema queries
- **Async/Await** - Built on Tokio and Actix-web for high-performance async I/O
- **Easy Configuration** - Environment variable-based configuration

## Quick Start

### Building

```bash
cargo build --release
```

### Running

```bash
# Default configuration (connects to qcore-rs at 127.0.0.1:8080, listens on 0.0.0.0:3000)
cargo run

# Custom configuration
QCORE_ADDRESS=192.168.1.100:8080 BIND_ADDRESS=0.0.0.0:8000 cargo run
```

### Configuration

Configure the application using environment variables:

- `QCORE_ADDRESS` - Address of the qcore-rs server (default: `127.0.0.1:8080`)
- `BIND_ADDRESS` - Address to bind the web server (default: `0.0.0.0:3000`)
- `RUST_LOG` - Log level (e.g., `info`, `debug`, `trace`)

## HTTP API

The application automatically connects to qcore-rs on startup using the `QCORE_ADDRESS` environment variable (default: `127.0.0.1:8080`). All endpoints are ready to use immediately after the server starts.

### Entity Operations

#### POST /api/read

Read field values from an entity.

**Request:**
```json
{
  "entity_id": "12884901888",
  "fields": ["Name", "Status"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "entity_id": "12884901888",
    "value": "String(\"Example\")",
    "timestamp": "2024-01-01 12:00:00.0 +00:00:00",
    "writer_id": "12884901889"
  }
}
```

#### POST /api/write

Write a value to an entity field.

**Request:**
```json
{
  "entity_id": "12884901888",
  "field": "Name",
  "value": "New Name"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "message": "Successfully wrote value"
  }
}
```

#### POST /api/create

Create a new entity.

**Request:**
```json
{
  "entity_type": "User",
  "name": "john_doe"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "entity_id": "12884901890",
    "entity_type": "User",
    "name": "john_doe"
  }
}
```

#### POST /api/delete

Delete an entity.

**Request:**
```json
{
  "entity_id": "12884901890"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "message": "Successfully deleted entity"
  }
}
```

#### POST /api/find

Find entities by type with optional filtering and pagination.

**Request:**
```json
{
  "entity_type": "User",
  "filter": "status='active'",  // Optional CEL expression
  "page_size": 10,              // Optional
  "page_number": 1              // Optional
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "entities": ["12884901888", "12884901889"],
    "total": 42,
    "next_cursor": 10
  }
}
```

### Schema Operations

#### POST /api/schema

Get entity schema information.

**Request:**
```json
{
  "entity_type": "User"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "entity_type": "User",
    "schema": "EntitySchema { ... }"
  }
}
```

## WebSocket API

Connect to the WebSocket endpoint at `ws://localhost:3000/ws` for persistent connections. The WebSocket uses the same qcore-rs connection established at server startup.

### Message Format

All messages are JSON with a `type` field indicating the operation.

### Supported Operations

#### Ping
```json
{
  "type": "Ping"
}
```

#### Read
```json
{
  "type": "Read",
  "entity_id": "12884901888",
  "fields": ["Name", "Status"]
}
```

#### Write
```json
{
  "type": "Write",
  "entity_id": "12884901888",
  "field": "Name",
  "value": "New Name"
}
```

#### Create
```json
{
  "type": "Create",
  "entity_type": "User",
  "name": "john_doe"
}
```

#### Delete
```json
{
  "type": "Delete",
  "entity_id": "12884901890"
}
```

#### Find
```json
{
  "type": "Find",
  "entity_type": "User",
  "filter": "status='active'"  // Optional
}
```

### Response Format

All WebSocket responses follow this format:

```json
{
  "success": true,
  "data": { /* operation-specific data */ },
  "error": null
}
```

On error:
```json
{
  "success": false,
  "data": null,
  "error": "Error message here"
}
```

## Value Types

The API supports the following value types for entity fields:

- **Boolean**: `true` or `false`
- **Integer**: `42`
- **Float**: `3.14`
- **String**: `"text"`
- **Entity Reference**: `"12884901888"` (entity ID as string)
- **Entity List**: `["12884901888", "12884901889"]` (array of entity IDs)
- **Null**: `null` (for empty entity references)

## Error Handling

All API responses include a `success` field. On error:

```json
{
  "success": false,
  "data": null,
  "error": "Error description"
}
```

Common error scenarios:
- Invalid entity ID format
- Entity or field type not found
- Invalid field value type
- Connection to qcore-rs lost

## Examples

### Using cURL

```bash
# Create an entity
curl -X POST http://localhost:3000/api/create \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "User", "name": "alice"}'

# Read entity fields
curl -X POST http://localhost:3000/api/read \
  -H "Content-Type: application/json" \
  -d '{"entity_id": "12884901888", "fields": ["Name"]}'

# Write to an entity
curl -X POST http://localhost:3000/api/write \
  -H "Content-Type: application/json" \
  -d '{"entity_id": "12884901888", "field": "Status", "value": "active"}'

# Find entities
curl -X POST http://localhost:3000/api/find \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "User", "filter": "name.contains(\"alice\")"}'
```

### Using WebSocket (JavaScript)

```javascript
const ws = new WebSocket('ws://localhost:3000/ws');

ws.onmessage = (event) => {
  const response = JSON.parse(event.data);
  console.log('Response:', response);
};

ws.onopen = () => {
  // Create an entity
  ws.send(JSON.stringify({
    type: "Create",
    entity_type: "User",
    name: "bob"
  }));
  
  // Read from entity
  ws.send(JSON.stringify({
    type: "Read",
    entity_id: "12884901888",
    fields: ["Name", "Email"]
  }));
};
```

## Architecture

```
┌─────────────┐         HTTP/WS          ┌──────────┐
│   Client    │ ◄─────────────────────► │   qweb   │
└─────────────┘                          └──────────┘
                                              │
                                              │ TCP (RESP)
                                              │ AsyncStoreProxy
                                              ▼
                                         ┌──────────┐
                                         │ qcore-rs │
                                         └──────────┘
```

- **qweb**: HTTP/WebSocket gateway (this project)
- **qlib-rs**: Client library with `AsyncStoreProxy` for RESP protocol communication
- **qcore-rs**: Backend database server

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.