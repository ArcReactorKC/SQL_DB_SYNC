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
# MSSQL -> PostgreSQL type mapping
# ---------------------------------------------------------------------------
MSSQL_TO_PG_TYPE = {
    "int":              "integer",
    "bigint":           "bigint",
    "smallint":         "smallint",
    "tinyint":          "smallint",
    "bit":              "boolean",
    "float":            "double precision",
    "real":             "real",
    "decimal":          "numeric",
    "numeric":          "numeric",
    "money":            "numeric(19,4)",
    "smallmoney":       "numeric(10,4)",
    "datetime":         "timestamptz",
    "datetime2":        "timestamptz",
    "smalldatetime":    "timestamptz",
    "date":             "date",
    "time":             "time",
    "char":             "text",
    "varchar":          "text",
    "nchar":            "text",
    "nvarchar":         "text",
    "text":             "text",
    "ntext":            "text",
    "uniqueidentifier": "uuid",
    "binary":           "bytea",
    "varbinary":        "bytea",
    "xml":              "text",
}


def mssql_type_to_pg(mssql_type: str) -> str:
    return MSSQL_TO_PG_TYPE.get(mssql_type.lower(), "text")


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
            t["col_map"] = {}

    return tables

# ---------------------------------------------------------------------------
# col_map helpers
# ---------------------------------------------------------------------------

def resolve_columns(table: dict):
    """Return two parallel lists:
    - mssql_cols: column names to use in the MSSQL SELECT (original names)
    - pg_cols:    column names to use in the Postgres INSERT (mapped names)
    """
    col_map = table.get("col_map", {})
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
# Static table schema management — create and evolve PG tables automatically
# ---------------------------------------------------------------------------

def ensure_pg_schema(pg_conn, schema: str):
    """
    Create the target schema if it does not already exist.
    Called once at startup before any table operations so fresh databases
    (e.g. newly created PG DBs with only the default 'public' schema) work
    without manual intervention.
    """
    cur = pg_conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    pg_conn.commit()
    cur.close()
    log.info(f"[pg] Ensured schema {schema} exists")


def get_mssql_column_types(ms_conn, mssql_table: str, columns: list[str]) -> dict[str, str]:
    """
    Query INFORMATION_SCHEMA.COLUMNS on MSSQL for the given table and columns.
    Returns a dict of {original_column_name: mssql_data_type}.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
          AND COLUMN_NAME IN ({placeholders})
    """
    cur = ms_conn.cursor()
    cur.execute(sql, [mssql_table] + list(columns))
    rows = cur.fetchall()
    cur.close()
    return {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in rows}


def get_pg_existing_columns(pg_conn, schema: str, table: str) -> set[str]:
    """
    Return the set of column names already present in the PG table (lowercased).
    Returns empty set if the table does not exist yet.
    """
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """
    cur = pg_conn.cursor()
    cur.execute(sql, [schema, table])
    rows = cur.fetchall()
    cur.close()
    return {row[0].lower() for row in rows}


def ensure_static_pg_table(table: dict, ms_conn, pg_conn):
    """
    Ensure the PG target table exists and contains all columns defined in
    tables.json. If the table is missing it is created. If it exists but is
    missing columns (schema drift) the missing columns are added via ALTER TABLE.

    Column types are resolved by querying MSSQL INFORMATION_SCHEMA — no
    hardcoded type assumptions. col_map renaming is applied so PG always
    receives the mapped column name.

    PKs are set only on CREATE; ALTER TABLE never drops or recreates PKs.
    """
    mssql_table = table["mssql_table"]
    schema      = table["pg_schema"]
    pg_table    = table["pg_table"]
    pk_cols     = table["pk_cols"]
    col_map     = table.get("col_map", {})

    mssql_cols, pg_cols = resolve_columns(table)

    # --- fetch MSSQL types for all columns in this table ---
    try:
        mssql_types = get_mssql_column_types(ms_conn, mssql_table, mssql_cols)
    except Exception as e:
        log.warning(f"[schema] Could not fetch MSSQL column types for {mssql_table}: {e}")
        mssql_types = {}

    # Build ordered list of (pg_col_name, pg_type) pairs
    col_defs = []
    for mssql_col, pg_col in zip(mssql_cols, pg_cols):
        mssql_type = mssql_types.get(mssql_col, "nvarchar")
        pg_type    = mssql_type_to_pg(mssql_type)
        col_defs.append((pg_col, pg_type))

    existing_cols = get_pg_existing_columns(pg_conn, schema, pg_table)

    cur = pg_conn.cursor()

    if not existing_cols:
        # Table does not exist — CREATE from scratch
        col_sql_parts = []
        for pg_col, pg_type in col_defs:
            col_sql_parts.append(f'    "{pg_col}" {pg_type}')

        pk_constraint = ", ".join(f'"{c}"' for c in pk_cols)
        col_sql_parts.append(f"    PRIMARY KEY ({pk_constraint})")

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{pg_table}" (\n'
            + ",\n".join(col_sql_parts)
            + "\n);"
        )
        log.info(f"[schema] Creating table {schema}.{pg_table}")
        cur.execute(create_sql)
        pg_conn.commit()
        log.info(f"[schema] Created {schema}.{pg_table} with {len(col_defs)} columns")

    else:
        # Table exists — check for missing columns and ALTER TABLE to add them
        added = []
        for pg_col, pg_type in col_defs:
            if pg_col.lower() not in existing_cols:
                alter_sql = f'ALTER TABLE "{schema}"."{pg_table}" ADD COLUMN "{pg_col}" {pg_type};'
                log.info(f"[schema] Adding missing column: {schema}.{pg_table}.{pg_col} ({pg_type})")
                cur.execute(alter_sql)
                added.append(pg_col)

        if added:
            pg_conn.commit()
            log.info(f"[schema] Schema drift fixed for {schema}.{pg_table} — added: {', '.join(added)}")
        else:
            log.info(f"[pg] Ensured table {schema}.{pg_table} exists (no drift)")

    cur.close()


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

    watermark_type  | INITIAL_LOAD=true       | INITIAL_LOAD=false
    ----------------|-------------------------|---------------------------
    epoch_ms        | 0 (Unix epoch ms)       | now - 2h (ms)
    epoch_ms_datetime | 0 (Unix epoch ms)     | now - 2h (ms)
    datetime        | "1970-01-01T00:00:00"   | today midnight UTC
    integer         | 0                       | 0 (always full for int PKs)
    string          | ""                      | ""
    """
    wt = table["watermark_type"]
    if wt in ("epoch_ms", "epoch_ms_datetime"):
        if INITIAL_LOAD:
            return 0
        return int(time.time() * 1000) - (2 * 3600 * 1000)
    elif wt == "integer":
        return 0
    elif wt == "string":
        return ""
    else:  # datetime
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
    """
    Build the MSSQL SELECT query based on watermark type.

    epoch_ms          — t_stamp is a bigint (Unix ms). WHERE t_stamp >= threshold_ms
    epoch_ms_datetime — t_stamp is a datetime column storing Unix ms as a number.
                        Convert bigint watermark to MSSQL datetime via DATEADD.
    datetime          — real datetime column. WHERE col >= 'YYYY-MM-DDTHH:MM:SS.mmm'
    integer           — integer PK. WHERE col >= since
    string            — not used for incremental; full_replace tables only.
    """
    mssql_cols, _ = resolve_columns(table)
    cols = ", ".join(f"[{c}]" for c in mssql_cols)
    wm   = table["watermark_col"]
    t    = table["mssql_table"]
    wt   = table["watermark_type"]

    if wt == "epoch_ms":
        threshold = int(since) - table["lookback_ms"]
        where = f"[{wm}] >= {threshold}"

    elif wt == "epoch_ms_datetime":
        threshold_ms = int(since) - table["lookback_ms"]
        days = threshold_ms // 86_400_000
        ms   = threshold_ms %  86_400_000
        where = (
            f"[{wm}] >= DATEADD(ms, {ms}, DATEADD(day, {days}, '19700101'))"
        )

    elif wt == "integer":
        where = f"[{wm}] >= {int(since)}"

    else:  # datetime
        try:
            since_dt = datetime.fromisoformat(since)
            since_dt = since_dt - timedelta(seconds=DATETIME_LOOKBACK_SECONDS)
            since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{since_dt.microsecond // 1000:03d}"
        except (ValueError, TypeError):
            since_str = since
        where = f"[{wm}] >= '{since_str}'"

    return f"SELECT {cols} FROM [{t}] WHERE {where} ORDER BY [{wm}] ASC"


def normalize_row(row: dict, col_map: dict) -> dict:
    """Return a copy of the row dict with:
    - all keys lowercased
    - UUID types converted to strings
    - column names remapped via col_map (mssql_name -> pg_name)
    """
    lower_col_map = {k.lower(): v for k, v in col_map.items()}
    result = {}
    for k, v in row.items():
        if isinstance(v, uuid.UUID):
            v = str(v)
        lower_k = k.lower()
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

    _, pg_cols = resolve_columns(table)

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
    wt      = table["watermark_type"]

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

        wm_col = table["watermark_col"]
        # Resolve watermark col through col_map to match normalized row keys
        col_map_lower = {k.lower(): v for k, v in col_map.items()}
        wm_col_pg = col_map_lower.get(wm_col.lower(), wm_col.lower())
        last_val = rows[-1].get(wm_col_pg)

        if last_val is not None:
            if wt in ("epoch_ms", "epoch_ms_datetime"):
                if hasattr(last_val, "timestamp"):
                    latest_wm = int(last_val.timestamp() * 1000)
                else:
                    latest_wm = int(last_val)
            elif wt == "integer":
                latest_wm = int(last_val)
            else:
                if hasattr(last_val, "isoformat"):
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
    Used for tables with no reliable watermark or duplicate PKs.
    Uses TRUNCATE ... CASCADE to handle any foreign key dependencies."""
    name    = table["mssql_table"]
    schema  = table["pg_schema"]
    tname   = table["pg_table"]
    col_map = table.get("col_map", {})

    mssql_cols, pg_cols = resolve_columns(table)
    cols  = ", ".join(f"[{c}]" for c in mssql_cols)
    query = f"SELECT {cols} FROM [{name}]"

    log.info(f"[{name}] Full replace mode — truncating {schema}.{tname} ...")

    ms_cur = ms_conn.cursor()
    ms_cur.execute(query)

    pg_cur = pg_conn.cursor()
    # CASCADE ensures dependent tables are also truncated, preventing
    # duplicate key errors when foreign key constraints exist.
    # Commit immediately so the TRUNCATE cannot be rolled back by a
    # subsequent INSERT failure, which would leave stale data in place.
    pg_cur.execute(f'TRUNCATE TABLE "{schema}"."{tname}" CASCADE')
    pg_conn.commit()

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

    # Collect all schemas needed and ensure they exist on PG before anything else
    all_schemas = {"dbo"}  # dbo is always needed for sqlt tables
    static_tables = load_tables_config()
    for t in static_tables:
        all_schemas.add(t["pg_schema"])
    for schema in sorted(all_schemas):
        try:
            ensure_pg_schema(pg_conn, schema)
        except Exception as e:
            log.error(f"[pg] Failed to ensure schema {schema}: {e}")
            pg_conn.rollback()

    for t in sqlt_tables:
        ensure_sqlt_pg_table(pg_conn, t["pg_schema"], t["pg_table"])

    # Ensure all static PG tables exist and are schema-current before syncing
    for t in static_tables:
        try:
            ensure_static_pg_table(t, ms_conn, pg_conn)
        except Exception as e:
            log.error(f"[schema] Failed to ensure table {t['pg_table']}: {e}")
            pg_conn.rollback()

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
