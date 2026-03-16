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
import uuid
import logging
from datetime import datetime, timezone, timedelta
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

    with open(TABLES_CONFIG, encoding="utf-8-sig") as f:
        tables = json.load(f)

    log.info(f"Loaded {len(tables)} static table(s) from {TABLES_CONFIG}")

    required = {"mssql_table", "pg_schema", "pg_table", "watermark_col",
                "watermark_type", "pk_cols", "columns"}
    for t in tables:
        missing = required - set(t.keys())
        if missing:
            raise ValueError(f"Table '{t.get('mssql_table', '?')}' missing fields: {missing}")
        if "lookback_ms" not in t:
            t["lookback_ms"] = 60_000
        if "sync_mode" not in t:
            t["sync_mode"] = "incremental"
        if "col_map" not in t:
            t["col_map"] = {}  # optional: {"SourceColName": "pg_col_name"}

    return tables

# ---------------------------------------------------------------------------
# col_map helpers
# ---------------------------------------------------------------------------

def mssql_col_name(col: str, col_map: dict) -> str:
    """Return the MSSQL source column name for a given pg column name.
    If col_map has a reverse mapping (pg -> mssql), use it.
    Otherwise the col name is used as-is on both sides."""
    # col_map is stored as {mssql_name: pg_name}, so reverse it
    reverse = {v: k for k, v in col_map.items()}
    return reverse.get(col, col)


def pg_col_name(col: str, col_map: dict) -> str:
    """Return the Postgres column name for a given column list entry.
    col_map keys are MSSQL names, values are PG names."""
    return col_map.get(col, col)


def resolve_columns(table: dict):
    """Return two parallel lists:
    - mssql_cols: column names to use in the MSSQL SELECT (original names)
    - pg_cols:    column names to use in the Postgres INSERT (mapped names)
    """
    col_map = table.get("col_map", {})
    # tables.json 'columns' list may contain either mssql or pg names.
    # Convention: columns list always uses MSSQL names; col_map maps mssql->pg.
    mssql_cols = table["columns"]
    pg_cols = [col_map.get(c, c) for c in mssql_cols]
    return mssql_cols, pg_cols


# ---------------------------------------------------------------------------
# sqlt_data dynamic discovery
# ---------------------------------------------------------------------------
SQLT_COLUMNS = ["tagid", "intvalue", "floatvalue", "stringvalue",
                "datevalue", "dataintegrity", "t_stamp"]

SQLT_TABLE_PATTERN = re.compile(
    r"^sqlt_data_(\d+)_(\d{4})_(\d{2})$", re.IGNORECASE
)


def discover_sqlt_tables(ms_conn) -> list[dict]:
    if not SQLT_DISCOVERY:
        log.info("[discovery] sqlt_data discovery disabled via SQLT_DISCOVERY=false")
        return []

    now = datetime.now(timezone.utc)
    targets = set()
    for offset in range(SQLT_LOOKBACK_MONTHS + 1):
        month = now.month - offset
        year  = now.year
        while month <= 0:
            month += 12
            year  -= 1
        targets.add((year, month))

    cur = ms_conn.cursor()
    cur.execute("""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name LIKE 'sqlt_data_%'
        ORDER BY t.name
    """)
    rows = cur.fetchall()
    cur.close()

    found = []
    for row in rows:
        tname  = row["table_name"]
        schema = row["schema_name"]
        m = SQLT_TABLE_PATTERN.match(tname)
        if not m:
            continue
        year  = int(m.group(2))
        month = int(m.group(3))
        if (year, month) not in targets:
            continue

        found.append({
            "mssql_table":    tname,
            "pg_schema":      schema,
            "pg_table":       tname.lower(),
            "watermark_col":  "t_stamp",
            "watermark_type": "epoch_ms",
            "pk_cols":        ["tagid", "t_stamp"],
            "columns":        SQLT_COLUMNS,
            "lookback_ms":    60_000,
            "sync_mode":      "incremental",
            "col_map":        {},
        })
        log.info(f"[discovery] Found sqlt table: {schema}.{tname}")

    if not found:
        log.info("[discovery] No sqlt_data tables found for target months")

    return found


def ensure_sqlt_pg_table(pg_conn, schema: str, table: str):
    sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
            tagid         integer          NOT NULL,
            intvalue      bigint,
            floatvalue    double precision,
            stringvalue   text,
            datevalue     timestamptz,
            dataintegrity integer,
            t_stamp       bigint           NOT NULL,
            PRIMARY KEY (tagid, t_stamp)
        );
    """
    cur = pg_conn.cursor()
    cur.execute(sql)
    pg_conn.commit()
    cur.close()
    log.info(f"[pg] Ensured table {schema}.{table} exists")


# ---------------------------------------------------------------------------
# Watermark persistence
# ---------------------------------------------------------------------------

def load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE) as f:
            return json.load(f)
    return {}


def save_watermarks(wm: dict):
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(wm, f, indent=2)


def get_default_watermark(table: dict) -> int | str:
    """
    Return starting watermark for a table with no prior run.
    INITIAL_LOAD=true  → epoch start (pulls everything)
    INITIAL_LOAD=false → 2 hours ago (normal incremental default)
    """
    if table["watermark_type"] == "epoch_ms":
        if INITIAL_LOAD:
            return 0
        return int(time.time() * 1000) - (2 * 3600 * 1000)
    else:
        if INITIAL_LOAD:
            return "1970-01-01T00:00:00"
        dt = datetime.now(timezone.utc).replace(microsecond=0)
        return (dt.replace(hour=0, minute=0, second=0)
                  .isoformat()
                  .replace("+00:00", ""))


# ---------------------------------------------------------------------------
# MSSQL helpers
# ---------------------------------------------------------------------------

def mssql_connect():
    return pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASS,
        database=MSSQL_DB,
        tds_version="7.4",
        as_dict=True,
    )


def build_mssql_query(table: dict, since) -> str:
    # Always use original MSSQL column names (keys of col_map, or col name if no mapping)
    mssql_cols, _ = resolve_columns(table)
    cols = ", ".join(f"[{c}]" for c in mssql_cols)
    wm   = table["watermark_col"]
    t    = table["mssql_table"]

    if table["watermark_type"] == "epoch_ms":
        threshold = int(since) - table["lookback_ms"]
        where = f"[{wm}] >= {threshold}"
    else:
        try:
            since_dt = datetime.fromisoformat(since)
            since_dt = since_dt - timedelta(seconds=DATETIME_LOOKBACK_SECONDS)
            # Truncate to milliseconds — Azure SQL rejects microsecond precision
            since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{since_dt.microsecond // 1000:03d}"
        except (ValueError, TypeError):
            since_str = since
        where = f"[{wm}] >= '{since_str}'"

    return f"SELECT {cols} FROM [{t}] WHERE {where} ORDER BY [{wm}] ASC"


def normalize_row(row: dict, col_map: dict) -> dict:
    """Return a copy of the row dict with:
    - all keys lowercased (pymssql returns original DB casing)
    - UUID types converted to strings
    - column names remapped via col_map (mssql_name -> pg_name)
    """
    # Build a lowercase version of col_map keys for case-insensitive matching
    lower_col_map = {k.lower(): v for k, v in col_map.items()}
    result = {}
    for k, v in row.items():
        if isinstance(v, uuid.UUID):
            v = str(v)
        lower_k = k.lower()
        # Apply col_map if present, otherwise keep lowercase key
        mapped_k = lower_col_map.get(lower_k, lower_k)
        result[mapped_k] = v
    return result


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
    )


def build_upsert_sql(table: dict) -> str:
    schema  = table["pg_schema"]
    tname   = table["pg_table"]
    pk_cols = table["pk_cols"]

    # Use pg column names for the INSERT
    _, pg_cols = resolve_columns(table)

    # Quote all column names to handle PostgreSQL reserved words
    col_list      = ", ".join(f'"{c}"' for c in pg_cols)
    placeholder   = ", ".join(["%s"] * len(pg_cols))
    conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
    update_cols   = [c for c in pg_cols if c not in pk_cols]

    if update_cols:
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        on_conflict = f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
    else:
        on_conflict = f"ON CONFLICT ({conflict_cols}) DO NOTHING"

    return (
        f'INSERT INTO "{schema}"."{tname}" ({col_list}) '
        f"VALUES ({placeholder}) "
        f"{on_conflict}"
    )


# ---------------------------------------------------------------------------
# Core sync logic
# ---------------------------------------------------------------------------

def sync_table_incremental(table: dict, watermarks: dict, ms_conn, pg_conn) -> int:
    name    = table["mssql_table"]
    wm_key  = name
    col_map = table.get("col_map", {})

    since = watermarks.get(wm_key, get_default_watermark(table))
    query = build_mssql_query(table, since)
    upsert_sql = build_upsert_sql(table)

    log.info(f"[{name}] Querying since {since} ...")

    ms_cur = ms_conn.cursor()
    ms_cur.execute(query)

    pg_cur = pg_conn.cursor()
    total     = 0
    latest_wm = since

    _, pg_cols = resolve_columns(table)

    while True:
        rows = ms_cur.fetchmany(CHUNK_SIZE)
        if not rows:
            break

        rows = [normalize_row(r, col_map) for r in rows]
        data = [tuple(row[c] for c in pg_cols) for row in rows]
        psycopg2.extras.execute_batch(pg_cur, upsert_sql, data, page_size=CHUNK_SIZE)
        pg_conn.commit()

        total += len(rows)

        wm_col   = table["watermark_col"]
        last_val = rows[-1][wm_col]
        if last_val is not None:
            if table["watermark_type"] == "epoch_ms":
                latest_wm = int(last_val)
            else:
                if hasattr(last_val, "isoformat"):
                    # Truncate to milliseconds — Azure SQL rejects microsecond precision
                    latest_wm = last_val.strftime("%Y-%m-%dT%H:%M:%S.") + f"{last_val.microsecond // 1000:03d}"
                else:
                    latest_wm = str(last_val)

        log.info(f"[{name}] ... {total} rows inserted/updated")

    if total > 0:
        watermarks[wm_key] = latest_wm
        log.info(f"[{name}] Done. {total} rows. Watermark advanced to {latest_wm}")
    else:
        log.info(f"[{name}] No new rows.")

    ms_cur.close()
    pg_cur.close()
    return total


def sync_table_full_replace(table: dict, ms_conn, pg_conn) -> int:
    """Truncate the PG table and reload all rows from MSSQL.
    Used for tables with no reliable watermark or duplicate PKs."""
    name    = table["mssql_table"]
    schema  = table["pg_schema"]
    tname   = table["pg_table"]
    col_map = table.get("col_map", {})

    # MSSQL side: original column names in square brackets
    mssql_cols, pg_cols = resolve_columns(table)
    cols  = ", ".join(f"[{c}]" for c in mssql_cols)
    query = f"SELECT {cols} FROM [{name}]"

    log.info(f"[{name}] Full replace mode — truncating {schema}.{tname} ...")

    ms_cur = ms_conn.cursor()
    ms_cur.execute(query)

    pg_cur = pg_conn.cursor()
    pg_cur.execute(f'TRUNCATE TABLE "{schema}"."{tname}"')

    # PostgreSQL side: double-quote pg column names to handle reserved words
    col_list   = ", ".join(f'"{c}"' for c in pg_cols)
    upsert_sql = (
        f'INSERT INTO "{schema}"."{tname}" ({col_list}) '
        f'VALUES ({", ".join(["%s"] * len(pg_cols))})'
    )

    total = 0
    while True:
        rows = ms_cur.fetchmany(CHUNK_SIZE)
        if not rows:
            break
        rows = [normalize_row(r, col_map) for r in rows]
        data = [tuple(row[c] for c in pg_cols) for row in rows]
        psycopg2.extras.execute_batch(pg_cur, upsert_sql, data, page_size=CHUNK_SIZE)
        total += len(rows)
        log.info(f"[{name}] ... {total} rows loaded")

    pg_conn.commit()
    ms_cur.close()
    pg_cur.close()
    log.info(f"[{name}] Full replace done. {total} rows.")
    return total


def sync_table(table: dict, watermarks: dict, ms_conn, pg_conn) -> int:
    if table.get("sync_mode") == "full_replace":
        return sync_table_full_replace(table, ms_conn, pg_conn)
    return sync_table_incremental(table, watermarks, ms_conn, pg_conn)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    validate_config()
    log.info(f"=== Ignition Sync Starting === DB: {MSSQL_DB} -> {PG_DB}")
    if INITIAL_LOAD:
        log.info("*** INITIAL_LOAD=true — pulling all historical data ***")
    watermarks = load_watermarks()

    try:
        ms_conn = mssql_connect()
        log.info("Connected to MSSQL")
    except Exception as e:
        log.error(f"MSSQL connection failed: {e}")
        raise

    try:
        pg_conn = pg_connect()
        log.info("Connected to PostgreSQL")
    except Exception as e:
        log.error(f"PostgreSQL connection failed: {e}")
        ms_conn.close()
        raise

    sqlt_tables = discover_sqlt_tables(ms_conn)
    for t in sqlt_tables:
        ensure_sqlt_pg_table(pg_conn, t["pg_schema"], t["pg_table"])

    static_tables = load_tables_config()
    all_tables = sqlt_tables + static_tables

    if not all_tables:
        log.warning("No tables to sync — check SQLT_DISCOVERY and TABLES_CONFIG")
        ms_conn.close()
        pg_conn.close()
        return

    total_rows = 0
    start = time.time()

    for table in all_tables:
        t0 = time.time()
        try:
            rows = sync_table(table, watermarks, ms_conn, pg_conn)
            total_rows += rows
            elapsed = time.time() - t0
            log.info(f"[{table['mssql_table']}] Completed in {elapsed:.1f}s")
        except Exception as e:
            log.error(f"[{table['mssql_table']}] FAILED: {e}")
            pg_conn.rollback()

    save_watermarks(watermarks)

    ms_conn.close()
    pg_conn.close()

    elapsed = time.time() - start
    log.info(f"=== Sync Complete: {total_rows} total rows in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
