# Metabase Flink SQL Driver

A Metabase driver that enables querying Apache Flink SQL Gateway via JDBC.

## Overview

This driver allows Metabase to connect to Apache Flink SQL Gateway, enabling you to:
- Query Flink streaming and batch tables from Metabase
- Build dashboards on real-time Flink data
- Use Metabase's query builder with Flink SQL

## Requirements

- Metabase v0.50+
- Apache Flink 1.18+ with SQL Gateway enabled
- Java 11+

## Installation

### Option 1: Download Pre-built JAR

Download the latest release JAR and place it in your Metabase plugins directory:

```bash
# Create plugins directory if it doesn't exist
mkdir -p /path/to/metabase/plugins

# Copy the JAR
cp metabase-flink-driver-1.0.0-SNAPSHOT.jar /path/to/metabase/plugins/

# Restart Metabase
```

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/J0hnG4lt/metabase-flink-driver.git
cd metabase-flink-driver

# Build the driver JAR
clj -T:build uber

# Copy to Metabase plugins
cp target/metabase-flink-driver-1.0.0-SNAPSHOT.jar /path/to/metabase/plugins/
```

## Configuration

When adding a new database in Metabase, select "Flink SQL" and configure:

| Field | Description | Default |
|-------|-------------|---------|
| Host | Flink SQL Gateway hostname | localhost |
| Port | SQL Gateway REST port | 8083 |
| Catalog | Flink catalog name (optional) | - |
| Database | Flink database name (optional) | - |
| Additional Options | Extra JDBC URL parameters | - |

## Quick Start with Docker

The included Docker Compose setup provides a complete testing environment:

```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (2-3 minutes)
# Then access Metabase at http://localhost:3000

# Optional: Run automated setup
pip install requests
python scripts/metabase_setup.py
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| Metabase | 3000 | Business intelligence UI |
| Flink Web UI | 8081 | Flink dashboard |
| SQL Gateway | 8083 | JDBC endpoint |
| PostgreSQL | 5432 | Metabase app database |

### Default Credentials

- **Metabase**: admin@example.com / Metabase123!

## Flink SQL Gateway Setup

Before using this driver, ensure your Flink cluster has the SQL Gateway running:

```bash
# Start Flink SQL Gateway
./bin/sql-gateway.sh start \
  -Dsql-gateway.endpoint.rest.address=0.0.0.0 \
  -Dsql-gateway.endpoint.rest.port=8083
```

## Supported Features

| Feature | Supported |
|---------|-----------|
| Basic queries | Yes |
| Table sync | Yes |
| Query builder | Yes |
| Native queries | Yes |
| Foreign keys | No |
| Timezone conversion | No |
| Connection impersonation | No |

## Supported Data Types

| Flink Type | Metabase Type |
|------------|---------------|
| TINYINT, SMALLINT, INT, INTEGER | Integer |
| BIGINT | BigInteger |
| FLOAT, DOUBLE | Float |
| DECIMAL | Decimal |
| VARCHAR, STRING, CHAR | Text |
| BOOLEAN | Boolean |
| DATE | Date |
| TIME | Time |
| TIMESTAMP | DateTime |
| TIMESTAMP_LTZ | DateTimeWithLocalTZ |
| ARRAY | Array |
| MAP, ROW | Structured |

## Development

### Project Structure

```
metabase-flink-driver/
├── src/metabase/driver/
│   └── flink_sql.clj        # Main driver implementation
├── resources/
│   └── metabase-plugin.yaml # Plugin configuration
├── test/metabase/driver/
│   └── flink_sql_test.clj   # Unit tests
├── scripts/
│   ├── init-flink.sql       # Sample Flink tables
│   └── metabase_setup.py    # Automated setup
├── deps.edn                  # Dependencies
├── build.clj                 # Build script
└── docker-compose.yaml       # Testing environment
```

### Running Tests

```bash
# Run unit tests
clj -X:test

# Run with verbose output
clj -X:test :reporter :dot
```

### Building

```bash
# Clean and build uber JAR
clj -T:build uber

# Output: target/metabase-flink-driver-1.0.0-SNAPSHOT.jar
```

## Troubleshooting

### Connection Refused

Ensure the Flink SQL Gateway is running and accessible:

```bash
curl http://localhost:8083/v1/info
```

### No Tables Found

1. Verify tables exist in Flink:
   ```sql
   SHOW TABLES;
   ```
2. Trigger a manual sync in Metabase (Database Settings > Sync)
3. Check Metabase logs for errors

### Driver Not Found

Ensure the JAR is in the correct plugins directory and Metabase has been restarted.

## References

- [Flink SQL Gateway Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql-gateway/overview/)
- [Flink JDBC Driver](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/jdbcdriver/)
- [Metabase Driver Development](https://www.metabase.com/docs/latest/developers-guide/drivers/start)

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## Acknowledgments

Based on the [metabase-flightsql-driver](https://github.com/J0hnG4lt/metabase-flightsql-driver) architecture.
