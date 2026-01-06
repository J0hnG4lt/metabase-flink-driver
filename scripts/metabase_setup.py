#!/usr/bin/env python3
"""
Metabase Setup Script for Flink SQL Driver Testing

This script automates the setup of Metabase for testing the Flink SQL driver.
It creates an admin user, adds a Flink SQL database connection, and optionally
creates sample dashboards.

Usage:
    python metabase_setup.py [--host METABASE_HOST] [--port METABASE_PORT]
"""

import argparse
import json
import time
import sys
import requests
from typing import Optional


class MetabaseSetup:
    """Automates Metabase setup for Flink SQL driver testing."""

    def __init__(self, host: str = "localhost", port: int = 3000):
        self.base_url = f"http://{host}:{port}"
        self.session_token: Optional[str] = None

    def wait_for_metabase(self, timeout: int = 300, interval: int = 5) -> bool:
        """Wait for Metabase to be ready."""
        print(f"Waiting for Metabase at {self.base_url}...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.base_url}/api/health", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "ok":
                        print("Metabase is ready!")
                        return True
            except requests.exceptions.RequestException:
                pass

            print(f"Waiting... ({int(time.time() - start_time)}s)")
            time.sleep(interval)

        print("Timeout waiting for Metabase")
        return False

    def check_setup_status(self) -> dict:
        """Check if Metabase initial setup has been completed."""
        response = requests.get(f"{self.base_url}/api/session/properties")
        return response.json()

    def setup_admin_user(
        self,
        email: str = "admin@example.com",
        password: str = "Metabase123!",
        first_name: str = "Admin",
        last_name: str = "User",
    ) -> bool:
        """Create the initial admin user."""
        print("Setting up admin user...")

        # Get setup token
        props = self.check_setup_status()
        setup_token = props.get("setup-token")

        if not setup_token:
            print("Setup already completed or no setup token available")
            return self.login(email, password)

        # Complete setup
        setup_data = {
            "token": setup_token,
            "user": {
                "email": email,
                "password": password,
                "first_name": first_name,
                "last_name": last_name,
                "site_name": "Flink SQL Testing",
            },
            "prefs": {
                "site_name": "Flink SQL Testing",
                "site_locale": "en",
                "allow_tracking": False,
            },
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/setup", json=setup_data, timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                self.session_token = data.get("id")
                print("Admin user created successfully!")
                return True
            else:
                print(f"Setup failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Setup error: {e}")
            return False

    def login(self, email: str, password: str) -> bool:
        """Login to Metabase."""
        print(f"Logging in as {email}...")

        try:
            response = requests.post(
                f"{self.base_url}/api/session",
                json={"username": email, "password": password},
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                self.session_token = data.get("id")
                print("Login successful!")
                return True
            else:
                print(f"Login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"Login error: {e}")
            return False

    def _headers(self) -> dict:
        """Get headers with session token."""
        return {"X-Metabase-Session": self.session_token}

    def add_flink_database(
        self,
        name: str = "Flink SQL Gateway",
        host: str = "sql-gateway",
        port: int = 8083,
        catalog: str = "",
        database: str = "",
    ) -> Optional[int]:
        """Add Flink SQL database connection."""
        print(f"Adding Flink SQL database: {name}...")

        db_config = {
            "name": name,
            "engine": "flink-sql",
            "details": {
                "host": host,
                "port": port,
                "catalog": catalog,
                "database": database,
            },
            "is_full_sync": True,
            "is_on_demand": False,
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/database",
                headers=self._headers(),
                json=db_config,
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                db_id = data.get("id")
                print(f"Database added successfully! ID: {db_id}")
                return db_id
            else:
                print(f"Failed to add database: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Error adding database: {e}")
            return None

    def sync_database(self, db_id: int) -> bool:
        """Trigger database sync."""
        print(f"Syncing database {db_id}...")

        try:
            response = requests.post(
                f"{self.base_url}/api/database/{db_id}/sync",
                headers=self._headers(),
                timeout=30,
            )
            if response.status_code == 200:
                print("Sync triggered successfully!")
                return True
            else:
                print(f"Sync failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"Sync error: {e}")
            return False

    def list_tables(self, db_id: int) -> list:
        """List tables in database with field info."""
        try:
            response = requests.get(
                f"{self.base_url}/api/database/{db_id}/metadata",
                headers=self._headers(),
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                tables = data.get("tables", [])
                return tables
            return []
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def run_query(self, db_id: int, query: str) -> dict:
        """Run a native SQL query."""
        try:
            response = requests.post(
                f"{self.base_url}/api/dataset",
                headers=self._headers(),
                json={
                    "database": db_id,
                    "type": "native",
                    "native": {"query": query}
                },
                timeout=60,
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}

    def create_simple_question(
        self, db_id: int, table_name: str, name: str
    ) -> Optional[int]:
        """Create a simple question (card) for a table."""
        print(f"Creating question: {name}...")

        # First get table ID
        try:
            response = requests.get(
                f"{self.base_url}/api/database/{db_id}/metadata",
                headers=self._headers(),
                timeout=30,
            )
            if response.status_code != 200:
                print(f"Failed to get metadata: {response.status_code}")
                return None

            data = response.json()
            tables = data.get("tables", [])
            table = next((t for t in tables if t.get("name") == table_name), None)
            if not table:
                print(f"Table {table_name} not found")
                return None

            table_id = table.get("id")
        except Exception as e:
            print(f"Error getting table: {e}")
            return None

        # Create question
        question = {
            "name": name,
            "dataset_query": {
                "database": db_id,
                "type": "query",
                "query": {"source-table": table_id, "limit": 100},
            },
            "display": "table",
            "visualization_settings": {},
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/card",
                headers=self._headers(),
                json=question,
                timeout=30,
            )
            if response.status_code == 200:
                card_id = response.json().get("id")
                print(f"Question created! ID: {card_id}")
                return card_id
            else:
                print(f"Failed to create question: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error creating question: {e}")
            return None


def main():
    parser = argparse.ArgumentParser(description="Setup Metabase for Flink SQL testing")
    parser.add_argument("--host", default="localhost", help="Metabase host")
    parser.add_argument("--port", type=int, default=3000, help="Metabase port")
    parser.add_argument("--flink-host", default="sql-gateway", help="Flink SQL Gateway host")
    parser.add_argument("--flink-port", type=int, default=8083, help="Flink SQL Gateway port")
    parser.add_argument(
        "--admin-email", default="admin@example.com", help="Admin email"
    )
    parser.add_argument(
        "--admin-password", default="Metabase123!", help="Admin password"
    )
    parser.add_argument(
        "--skip-wait", action="store_true", help="Skip waiting for Metabase"
    )
    args = parser.parse_args()

    setup = MetabaseSetup(host=args.host, port=args.port)

    # Wait for Metabase to be ready
    if not args.skip_wait:
        if not setup.wait_for_metabase():
            sys.exit(1)

    # Setup admin user
    if not setup.setup_admin_user(
        email=args.admin_email, password=args.admin_password
    ):
        print("Failed to setup admin user")
        sys.exit(1)

    # Add Flink database
    db_id = setup.add_flink_database(
        host=args.flink_host,
        port=args.flink_port,
    )
    if not db_id:
        print("Failed to add Flink database")
        sys.exit(1)

    # Wait a bit for initial sync
    print("Waiting for initial database sync...")
    time.sleep(10)

    # Trigger sync
    setup.sync_database(db_id)

    # Wait and list tables
    print("Waiting for sync to complete...")
    time.sleep(15)

    tables = setup.list_tables(db_id)
    if tables:
        print(f"\nDiscovered {len(tables)} tables:")
        for table in tables:
            fields = table.get("fields", [])
            print(f"  - {table.get('name')}: {len(fields)} fields")
    else:
        print("No tables discovered yet. You may need to wait for the sync to complete.")
        sys.exit(1)

    # Test queries
    print("\n--- Testing Queries ---")
    test_queries = [
        ("SELECT COUNT(*) as cnt FROM users", "User count"),
        ("SELECT * FROM orders LIMIT 3", "Orders sample"),
        ("SELECT COUNT(*) as cnt FROM products", "Product count"),
    ]

    all_passed = True
    for query, description in test_queries:
        print(f"\nTest: {description}")
        print(f"Query: {query}")
        result = setup.run_query(db_id, query)
        if result.get("status") == "completed":
            rows = result.get("data", {}).get("rows", [])
            print(f"Result: SUCCESS - {len(rows)} rows returned")
            if rows:
                print(f"First row: {rows[0]}")
        else:
            error = result.get("error", "Unknown error")
            print(f"Result: FAILED - {str(error)[:200]}")
            all_passed = False

    print("\n" + "=" * 50)
    print("Setup complete!")
    print(f"Metabase URL: http://{args.host}:{args.port}")
    print(f"Login: {args.admin_email} / {args.admin_password}")
    print(f"Database ID: {db_id}")
    print(f"Tables: {len(tables)}")
    print(f"Query Tests: {'ALL PASSED' if all_passed else 'SOME FAILED'}")
    print("=" * 50)

    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
