---
description: Run end-to-end tests for the Metabase Flink SQL driver. Starts all services from scratch, runs setup, and validates tables sync and queries work.
allowed-tools: Bash, Read, Write, Glob, Grep
---

# End-to-End Test for Metabase Flink SQL Driver

Run a complete end-to-end test of the driver by starting all services fresh and validating functionality.

## Pre-requisites

- Podman (or Docker) must be installed and running
- Python 3 with `requests` library installed

## Test Steps

### 1. Clean up existing environment

```bash
cd $PROJECT_ROOT
podman compose down -v
```

This stops all containers and removes volumes for a fresh start.

### 2. Start all services

```bash
podman compose up -d
```

This starts:
- **builder**: Compiles the driver JAR using Clojure tools.deps
- **postgres**: Metabase application database
- **jobmanager**: Flink JobManager
- **taskmanager**: Flink TaskManager
- **sql-gateway**: Flink SQL Gateway (port 8083)
- **metabase**: Metabase BI tool (port 3000)

### 3. Wait for Metabase to be ready

```bash
for i in {1..60}; do
  if curl -s -f "http://localhost:3000/api/health" >/dev/null 2>&1; then
    echo "Metabase is ready!"
    break
  fi
  echo "Waiting... attempt $i"
  sleep 5
done
```

### 4. Run the setup script

```bash
pip install requests
python scripts/metabase_setup.py --admin-email admin@test.com --admin-password "Admin123!@#"
```

This script:
- Performs initial Metabase setup (creates admin user)
- Creates Flink SQL database connection
- Syncs database schema
- Tests query execution

### 5. Manual Validation

Open in browser: http://localhost:3000

Login with:
- Email: admin@test.com
- Password: Admin123!@#

Navigate to "Flink SQL Gateway" database and verify:
- 4 tables visible: users, orders, products, page_views
- Each table has fields synced
- Running a query returns data

### 6. API Validation

Test via API:
```bash
# Login and get session
SESSION=$(curl -s -X POST http://localhost:3000/api/session \
  -H "Content-Type: application/json" \
  -d '{"username": "admin@test.com", "password": "Admin123!@#"}' | \
  grep -o '"id":"[^"]*"' | cut -d'"' -f4)

# Check database metadata
curl -s "http://localhost:3000/api/database/2/metadata" \
  -H "X-Metabase-Session: $SESSION" | python -c "
import sys,json
d=json.load(sys.stdin)
tables = d.get('tables', [])
print(f'Tables: {len(tables)}')
for t in tables:
    fields = t.get('fields', [])
    print(f'  - {t[\"name\"]}: {len(fields)} fields')
"

# Test query
curl -s -X POST "http://localhost:3000/api/dataset" \
  -H "X-Metabase-Session: $SESSION" \
  -H "Content-Type: application/json" \
  -d '{"database": 2, "type": "native", "native": {"query": "SELECT * FROM users LIMIT 5"}}' | \
  python -c "
import sys,json
d=json.load(sys.stdin)
print(f'Status: {d.get(\"status\")}')
print(f'Rows: {len(d.get(\"data\",{}).get(\"rows\",[]))}')
"
```

## Expected Results

| Check | Expected |
|-------|----------|
| Tables synced | 5 (users, orders, products, page_views, streaming_events) |
| Fields per table | users: 6, orders: 7, products: 6, page_views: 7, streaming_events: 4 |
| User count | 10,000 |
| Order count | 50,000 |
| Product count | 1,000 |
| Page views count | 100,000 |
| Connection test | Passes |
| All bounded queries | Pass |
| DDL tests (CREATE, DROP, USE, SET) | Pass |
| Catalog exploration (SHOW CATALOGS, etc.) | Pass |

## DDL Statement Tests

The setup script tests DDL statement support:

| Statement | Expected |
|-----------|----------|
| CREATE TABLE ... | Returns "OK" |
| DROP TABLE ... | Returns "OK" |
| USE CATALOG ... | Returns "OK" |
| USE database | Returns "OK" |
| SET 'key' = 'value' | Returns "OK" |

**Note**: Tables created via DDL are **session-scoped**. They won't persist across queries.

## Catalog and Schema Exploration

The driver supports multi-catalog and multi-database environments:

| Query | Expected |
|-------|----------|
| SHOW CATALOGS | Lists all catalogs (e.g., default_catalog) |
| SHOW DATABASES | Lists databases in current catalog |
| SHOW CURRENT CATALOG | Shows current catalog |
| SHOW CURRENT DATABASE | Shows current database |

### Catalog/Database Connection Properties

When creating a database connection in Metabase, you can specify:
- **Catalog**: The Flink catalog to use (e.g., `default_catalog`, `hive_catalog`)
- **Database**: The database within the catalog (e.g., `default_database`)

The driver issues `USE CATALOG` and `USE DATABASE` statements at connection time.

## Table Types

| Table | Type | Rows | Notes |
|-------|------|------|-------|
| users | Bounded | 10,000 | Full SQL support |
| orders | Bounded | 50,000 | Full SQL support |
| products | Bounded | 1,000 | Full SQL support |
| page_views | Bounded | 100,000 | Full SQL support |
| streaming_events | **UNBOUNDED** | ∞ | ⚠️ DO NOT QUERY - will hang! |

## Streaming Table Limitation

The `streaming_events` table demonstrates the Flink JDBC batch-mode limitation:
- It uses `datagen` connector WITHOUT `number-of-rows`
- Any query will hang forever waiting for the stream to end
- This is expected behavior per FLIP-293

**To query streaming sources like Kafka**, configure them with:
```sql
'scan.bounded.mode' = 'latest-offset'
```

## Troubleshooting

### Check Metabase logs
```bash
podman logs metabase-flink-driver-metabase-1 2>&1 | tail -100
```

### Check SQL Gateway logs
```bash
podman logs metabase-flink-driver-sql-gateway-1 2>&1 | tail -50
```

### Check Flink cluster
```bash
# Open Flink Web UI
open http://localhost:8081
```

### Restart after driver changes
```bash
podman compose down metabase builder plugin-init
podman compose up -d builder
# Wait for JAR build (~60 seconds)
podman compose up -d plugin-init metabase
```

## Credentials

- **Metabase URL**: http://localhost:3000
- **Admin Email**: admin@test.com
- **Admin Password**: Admin123!@#
- **Flink SQL Gateway**: sql-gateway:8083
- **Flink Web UI**: http://localhost:8081
