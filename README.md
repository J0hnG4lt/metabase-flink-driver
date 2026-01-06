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
| Catalog | Flink catalog name (optional) | default_catalog |
| Database | Flink database name (optional) | default_database |
| Additional Options | Extra JDBC URL parameters | - |

### Multi-Catalog Support

The driver supports Flink's multi-catalog architecture. When you specify a Catalog and Database:

1. The driver connects to the SQL Gateway
2. Executes `USE CATALOG <catalog>` to switch to the specified catalog
3. Executes `USE <database>` to switch to the specified database
4. All subsequent queries run in that context

**Available Catalogs**: Use `SHOW CATALOGS` in a native query to see available catalogs.

**Catalog Types**:
- `default_catalog` - In-memory catalog (session-scoped tables)
- `hive_catalog` - Hive Metastore integration (persistent tables)
- Custom catalogs configured in your Flink cluster

To work with persistent tables across sessions, configure a Hive catalog or other persistent catalog in your Flink cluster.

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
| DDL statements (CREATE, DROP, ALTER) | Yes* |
| JOINs | Yes |
| Aggregations (GROUP BY, SUM, COUNT) | Yes |
| Subqueries | Yes |
| Large result sets (100K+ rows) | Yes |
| Foreign keys | No |
| Timezone conversion | No |
| Connection impersonation | No |

*DDL statements work but tables are session-scoped (see DDL Support section below)

## Supported Table Types and Streaming Limitation

### How Flink JDBC Works

The Flink JDBC driver operates in **batch mode only** (per [FLIP-293](https://cwiki.apache.org/confluence/display/FLINK/FLIP-293%3A+Introduce+Flink+JDBC+Driver)). This means:
- Queries must return **finite result sets**
- **Unbounded streaming tables** will hang forever
- This is by design, not a driver bug

### Supported Connectors

| Connector | Configuration | Works? | Notes |
|-----------|--------------|--------|-------|
| `datagen` with rows | `'number-of-rows' = '10000'` | Yes | Finite data, full SQL support |
| `datagen` without rows | No `number-of-rows` | **No** | Unbounded stream, queries hang |
| `filesystem` | CSV, Parquet files | Yes | Bounded by file content |
| `jdbc` | External database | Yes | Bounded by source query |
| `kafka` unbounded | No bounded mode | **No** | Infinite stream, queries hang |
| `kafka` bounded | `'scan.bounded.mode' = 'latest-offset'` | Yes | Treats stream as snapshot |

### Querying Kafka Tables

To query Kafka tables via JDBC/Metabase, configure them with a bounded scan mode:

```sql
CREATE TABLE kafka_events (
  event_id STRING,
  event_data STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',  -- THIS IS KEY!
  'format' = 'json'
);
```

#### Bounded Mode Options

| Mode | Description | Use Case |
|------|-------------|----------|
| `latest-offset` | Read up to latest offset at query time | Snapshot of current data |
| `timestamp` | Read up to specific timestamp | Historical queries |
| `specific-offsets` | Read to specific partition offsets | Precise boundaries |
| `group-offsets` | Bounded by consumer group commits | Resume from checkpoint |

### Why Unbounded Tables Hang

When you query an unbounded table (streaming source without bounds):
1. Flink starts reading from the source
2. JDBC waits for the query to "complete"
3. An unbounded stream **never completes**
4. The query hangs forever

This is expected behavior. To fix it, either:
- Use bounded connectors (datagen with rows, filesystem, etc.)
- Configure streaming connectors with bounded mode (`scan.bounded.mode`)

### Test Tables

The driver creates test tables for validation:

| Table | Type | Rows | SQL Support |
|-------|------|------|-------------|
| users | Bounded | 10,000 | Full |
| orders | Bounded | 50,000 | Full |
| products | Bounded | 1,000 | Full |
| page_views | Bounded | 100,000 | Full |
| streaming_events | **Unbounded** | **Infinite** | **DO NOT QUERY** |

The `streaming_events` table exists to demonstrate the limitation. **Do not query it**.

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

## DDL Support (CREATE TABLE, DROP TABLE, etc.)

This driver supports DDL statements via native queries in Metabase:

```sql
-- Create a bounded datagen table
CREATE TABLE my_table (
  id INT,
  name STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '1000'
);

-- Drop a table
DROP TABLE my_table;

-- Alter table (if supported by the connector)
ALTER TABLE my_table ADD new_column STRING;
```

### Session Scope Limitation

**Important**: Tables created via DDL are **session-scoped** in Flink. Each Metabase query runs in a separate JDBC session, meaning:

- Tables created in one query are NOT visible to subsequent queries
- This is standard Flink behavior, not a driver limitation
- The built-in test tables (users, orders, products, page_views) are re-created in each session automatically

### Persisting Tables to a Catalog

To create tables that persist across sessions, you must:

1. Configure a persistent catalog (Hive, JDBC, etc.) in your Flink cluster
2. Use the catalog in your DDL statements

```sql
-- Example with Hive catalog
USE CATALOG hive_catalog;
CREATE TABLE persistent_table (...) WITH (...);
```

See the [Flink Catalogs documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/catalogs/) for more information.

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
