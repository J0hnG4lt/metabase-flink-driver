# Metabase Flink SQL Driver

A Metabase driver that enables querying Apache Flink SQL Gateway via JDBC.

## Overview

This driver allows Metabase to connect to Apache Flink SQL Gateway, enabling you to:
- Query Flink streaming and batch tables from Metabase
- Build dashboards on real-time Flink data
- Use Metabase's query builder with Flink SQL

## Requirements

- Metabase v0.50+ (tested with v0.57.7)
- Apache Flink 1.18+ with SQL Gateway enabled
- Java 17+ (Java 21 also supported)

## Flink Version Compatibility

**Important**: Download the JAR that matches your Flink SQL Gateway version.

| Your Flink Version | Download JAR |
|-------------------|--------------|
| Flink 1.18.x | `metabase-flink-driver-1.0.0-flink-1.18.jar` |
| Flink 1.19.x | `metabase-flink-driver-1.0.0-flink-1.19.jar` |
| Flink 1.20.x | `metabase-flink-driver-1.0.0-flink-1.20.jar` |
| Flink 2.0.x / 2.1.x | `metabase-flink-driver-1.0.0-flink-2.1.jar` |

The JDBC driver version must match the SQL Gateway version for proper REST API compatibility.

## Installation

### Option 1: Download Pre-built JAR

Download the JAR for your Flink version from the [Releases page](https://github.com/J0hnG4lt/metabase-flink-driver/releases):

```bash
# Create plugins directory if it doesn't exist
mkdir -p /path/to/metabase/plugins

# Copy the JAR (use the version matching your Flink cluster)
cp metabase-flink-driver-1.0.0-flink-2.1.jar /path/to/metabase/plugins/

# Restart Metabase
```

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/J0hnG4lt/metabase-flink-driver.git
cd metabase-flink-driver

# Build for a specific Flink version (default: 2.1)
clojure -T:build uber                              # Builds for Flink 2.1
clojure -T:build uber :flink-version '"1.18"'      # Builds for Flink 1.18
clojure -T:build uber :flink-version '"1.19"'      # Builds for Flink 1.19
clojure -T:build uber :flink-version '"1.20"'      # Builds for Flink 1.20

# Or build all versions at once
clojure -T:build uber-all

# Copy to Metabase plugins
cp target/metabase-flink-driver-1.0.0-flink-*.jar /path/to/metabase/plugins/
```

## Configuration

When adding a new database in Metabase, select "Flink SQL" and configure:

| Field | Description | Default |
|-------|-------------|---------|
| Host | Flink SQL Gateway hostname | localhost |
| Port | SQL Gateway REST port | 8083 |
| Catalog | Flink catalog name (optional) | default_catalog |
| Database | Flink database name (optional) | default_database |
| Streaming Query Timeout | Timeout for streaming queries (seconds) | 30 |
| Additional Options | Extra JDBC URL parameters | - |

### Streaming Query Timeout

The driver includes a unique **streaming query timeout** feature that enables querying unbounded streaming tables (like Kafka) without hanging forever!

**How it works:**
1. Query executes against the streaming source
2. Rows are collected as they stream in
3. After the timeout, partial results are returned
4. The query is cancelled to free resources

**Configuration:**
- Default: 30 seconds
- Set to 0 to disable (queries will hang on unbounded tables)
- Adjust based on your expected query duration

**Example:** If you query a Kafka table without bounded mode, the query will return whatever rows were collected within 30 seconds instead of hanging forever.

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

## Supported Table Types and Streaming Support

### How Flink JDBC Works

The Flink JDBC driver operates in **batch mode only** (per [FLIP-293](https://cwiki.apache.org/confluence/display/FLINK/FLIP-293%3A+Introduce+Flink+JDBC+Driver)). This means:
- Queries must return **finite result sets**
- **Unbounded streaming tables** would normally hang forever
- This driver includes a **streaming timeout** feature to work around this!

### Streaming Query Timeout (NEW!)

This driver adds a unique feature: **streaming query timeout**. Instead of hanging forever on unbounded tables, queries will:
1. Collect rows as they stream in
2. Return partial results after the configured timeout (default: 30 seconds)
3. Cancel the query to free resources

This means you CAN query unbounded streaming tables like Kafka - you'll get a sample of the data!

### Supported Connectors

| Connector | Configuration | Works? | Notes |
|-----------|--------------|--------|-------|
| `datagen` with rows | `'number-of-rows' = '10000'` | ✅ Yes | Finite data, full SQL support |
| `datagen` without rows | No `number-of-rows` | ✅ Yes* | Returns partial results after timeout |
| `filesystem` | CSV, Parquet files | ✅ Yes | Bounded by file content |
| `jdbc` | External database | ✅ Yes | Bounded by source query |
| `kafka` unbounded | No bounded mode | ✅ Yes* | Returns partial results after timeout |
| `kafka` bounded | `'scan.bounded.mode' = 'latest-offset'` | ✅ Yes | Treats stream as snapshot (recommended) |

*With streaming timeout enabled (default). Set timeout to 0 to restore original hanging behavior.

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

### Why Unbounded Tables Would Hang (Without Timeout)

When you query an unbounded table (streaming source without bounds):
1. Flink starts reading from the source
2. JDBC waits for the query to "complete"
3. An unbounded stream **never completes**
4. Without timeout: query hangs forever
5. **With timeout (default)**: partial results returned after 30 seconds!

For best results with streaming sources:
- Use the streaming timeout feature (enabled by default)
- Or configure streaming connectors with bounded mode (`scan.bounded.mode`)
- Or use bounded connectors (datagen with rows, filesystem, etc.)

### Test Tables

The driver creates test tables for validation:

| Table | Type | Rows | Queryable? |
|-------|------|------|------------|
| users | Bounded | 10,000 | ✅ Full SQL support |
| orders | Bounded | 50,000 | ✅ Full SQL support |
| products | Bounded | 1,000 | ✅ Full SQL support |
| page_views | Bounded | 100,000 | ✅ Full SQL support |
| streaming_events | Unbounded | Streaming | ✅ With timeout (partial results) |

The `streaming_events` table demonstrates the streaming timeout feature - queries return partial results after the timeout!

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

This driver was developed with significant assistance from [Claude Code](https://claude.ai/claude-code), Anthropic's AI coding assistant. The author is not a Clojure developer, but Claude helped with the Clojure implementation, Metabase driver architecture, and debugging.

We welcome contributions from the community! If you're a Clojure developer or have experience with Metabase drivers, your help would be greatly appreciated.

## Contact

For inquiries or assistance, please open an issue in this repository or reach out directly to the maintainer at georvic.tur@gmail.com.
