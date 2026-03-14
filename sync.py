#!/usr/bin/env python3
"""
Ignition Historian Sync
Streams rows from Azure SQL (MSSQL) to PostgreSQL in chunks.
Fully config-driven via environment variables and an optional tables.json file.
A single Docker image handles any number of Ignition databases.
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import pymssql
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all overridable via environment variables
# ---------------------------------------------------------------------------
MSSQL_HOST     = os.getenv("MSSQL_HOST",     "ally-sql-01.database.windows.net")
MSSQL_PORT     = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER     = os.getenv("MSSQL_USER",     "pgloader_user")
MSSQL_PASS     = os.getenv("MSSQL_PASS",     "")
MSSQL_DB       = os.getenv("MSSQL_DB",       "")

PG_HOST        = os.getenv("PG_HOST",        "10.20.1.25")
PG_PORT        = int(os.getenv("PG_PORT",    "5432"))
PG_USER        = os.getenv("PG_USER",        "postgres")
PG_PASS        = os.getenv("PG_PASS",        "")
PG_DB          = os.getenv("PG_DB",          "")

CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "5000"))
WATERMARK_FILE = Path(os.getenv("WATERMARK_FILE", "/data/watermarks.json"))
TABLES_CONFIG  = Path(os.getenv("TABLES_CONFIG", "/config/tables.json"))

SQLT_LOOKBACK_MONTHS = int(os.getenv("SQLT_LOOKBACK_MONTHS", "1"))
SQLT_DISCOVERY       = os.getenv("SQLT_DISCOVERY", "true").lower() == "true"

# Set to "true" on first run to pull all historical data instead of 2h lookback
INITIAL_LOAD = os.getenv("INITIAL_LOAD", "false").lower() == "true"

# Safety overlap for datetime watermarks (seconds). Prevents missing rows
# written right at the boundary. Default 60s.
DATETIME_LOOKBACK_SECONDS = int(os.getenv("DATETIME_LOOKBACK_SECONDS", "60"))

# ---------------------------------------------------------------------------
# Validate required config
# ---------------------------------------------------------------------------
def validate_config():
    errors = []
    if not MSSQL_DB:
        errors.append("MSSQL_DB is required")
    if not MSSQL_PASS:
        errors.append("MSSQL_PASS is required")
    if not PG_DB:
        errors.append("PG_DB is required")
    if not PG_PASS:
        errors.append("PG_PASS is required")
    if errors:
        for e in errors:
            log.error(f"Config error: {e}")
        raise SystemExit(1)

# ---------------------------------------------------------------------------
# Load static table definitions from tables.json
# ---------------------------------------------------------------------------
def load_tables_config() -> list[dict]:
    if not TABLES_CONFIG.exists():
        log.info(f"No tables config found at {TABLES_CONFIG} — only sqlt_data tables will be synced")
        return []

    with open(TABLES_CONFIG) as f:
        tables = json.load(f)

    log.info(f"Loaded {len(tables)} static table(s) from {TABLES_CONFIG}
