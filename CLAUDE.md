# CLAUDE.md - Development Guide

This file provides context for Claude Code when working on this project.

## Project Overview

This is a Metabase driver for Apache Flink SQL Gateway. It allows Metabase to connect to Flink clusters via the SQL Gateway's JDBC interface.

## Key Files

- `src/metabase/driver/flink_sql.clj` - Main driver implementation
- `resources/metabase-plugin.yaml` - Plugin metadata and connection properties
- `deps.edn` - Clojure dependencies and build configuration
- `build.clj` - Build script for creating uber JAR
- `docker-compose.yaml` - Full testing environment

## Build Commands

```bash
# Build the driver JAR
clj -T:build uber

# Run tests
clj -X:test

# Clean build artifacts
clj -T:build clean
```

## Docker Commands

```bash
# Start full environment
docker-compose up -d

# View logs
docker-compose logs -f metabase
docker-compose logs -f sql-gateway

# Rebuild driver and restart
docker-compose up -d --build builder
docker-compose restart metabase

# Stop everything
docker-compose down

# Clean volumes
docker-compose down -v
```

## Driver Architecture

The driver extends Metabase's `sql-jdbc` parent driver and implements:

1. **Connection handling** - Builds JDBC URLs for Flink SQL Gateway
2. **Schema sync** - Uses SHOW TABLES and DESCRIBE for metadata
3. **Type mapping** - Maps Flink types to Metabase base types
4. **Query processing** - Uses MySQL-style quoting (backticks)
5. **Date/time functions** - Implements date_trunc and extraction

## Flink SQL Gateway

The driver connects to Flink SQL Gateway via its REST-based JDBC interface:
- Default port: 8083
- Driver class: `org.apache.flink.table.jdbc.FlinkDriver`
- URL format: `jdbc:flink://host:port/catalog/database`

## Testing

### Unit Tests

Located in `test/metabase/driver/flink_sql_test.clj`:
- Driver registration
- Connection spec building
- Error message handling
- Type mapping

### Integration Testing

1. Start Docker environment: `docker-compose up -d`
2. Wait for all services to be healthy
3. Access Metabase at http://localhost:3000
4. Add Flink SQL database connection
5. Verify table sync and query execution

### Automated Setup

```bash
python scripts/metabase_setup.py
```

This creates admin user, adds Flink connection, and creates sample questions.

## Common Issues

### "No suitable driver" error
Ensure the Flink JDBC driver JAR is bundled in the uber JAR.

### Tables not syncing
Flink SQL Gateway may not support all JDBC metadata methods. The driver falls back to SHOW TABLES and DESCRIBE.

### Connection timeout
Increase timeout in additional options or check SQL Gateway health.

## Metabase Plugin YAML

The `metabase-plugin.yaml` defines:
- Plugin name and version
- Driver name (`:flink-sql`)
- Connection properties (host, port, catalog, database)
- Initialization steps (namespace loading, JDBC driver registration)

## Dependencies

- `org.apache.flink/flink-sql-jdbc-driver-bundle` - Flink JDBC driver
- `org.clojure/clojure` - Clojure runtime

The driver JAR must include all dependencies as Metabase loads plugins in isolation.
