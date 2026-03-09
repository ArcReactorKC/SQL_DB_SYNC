# ignition-sync / Energy_Projects_syncer

Incremental sync of Ignition historian tables from Azure SQL (MSSQL) to a local PostgreSQL instance. A **single Docker image** handles any number of databases — each database gets its own container instance configured entirely via environment variables and an optional `tables.json` file.

---

## How it works

1. Reads the last known watermark per table from `/data/watermarks.json`
2. Auto-discovers `sqlt_data_*` tables for the current and previous month
3. Loads optional static table definitions from `/config/tables.json`
4. Streams rows in 5,000-row chunks directly into PostgreSQL
5. Advances watermarks only after successful inserts
6. Exits — triggered by cron, not a long-running daemon

---

## Directory structure per database on Unraid

```
/mnt/user/appdata/<DbName>/
    data/
        watermarks.json     ← auto-created on first run
    tables.json             ← defines custom tables (optional)
```

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MSSQL_HOST` | no | `ally-sql-01.database.windows.net` | Azure SQL hostname |
| `MSSQL_PORT` | no | `1433` | Azure SQL port |
| `MSSQL_USER` | no | `pgloader_user` | MSSQL username |
| `MSSQL_PASS` | **yes** | — | MSSQL password |
| `MSSQL_DB` | **yes** | — | Source database name |
| `PG_HOST` | no | `10.20.1.25` | PostgreSQL host |
| `PG_PORT` | no | `5432` | PostgreSQL port |
| `PG_USER` | no | `postgres` | PostgreSQL username |
| `PG_PASS` | **yes** | — | PostgreSQL password |
| `PG_DB` | **yes** | — | Destination database name |
| `CHUNK_SIZE` | no | `5000` | Rows per insert batch |
| `SQLT_LOOKBACK_MONTHS` | no | `1` | Extra months of sqlt_data to include |
| `SQLT_DISCOVERY` | no | `true` | Set to `false` for non-historian DBs |
| `TABLES_CONFIG` | no | `/config/tables.json` | Path to static tables definition |
| `WATERMARK_FILE` | no | `/data/watermarks.json` | Path to watermark persistence file |

---

## tables.json format

Only needed if the database has custom tables beyond `sqlt_data_*`.
Copy `tables.example.json` as a starting point.

```json
[
  {
    "mssql_table": "mytable",
    "pg_schema": "dbo",
    "pg_table": "mytable",
    "watermark_col": "t_stamp",
    "watermark_type": "datetime",
    "pk_cols": ["mytable_ndx"],
    "columns": ["mytable_ndx", "value", "t_stamp"]
  }
]
```

`watermark_type` is either `epoch_ms` (bigint) or `datetime`.

---

## Adding a new database on Unraid

1. Create the appdata folder:
```bash
mkdir -p /mnt/user/appdata/MyNewDb/data
```

2. Copy and edit the tables config if needed:
```bash
cp /mnt/user/appdata/Energy_Projects/tables.json \
   /mnt/user/appdata/MyNewDb/tables.json
# edit as needed
```

3. Add a new container in Unraid Docker pointing to the same image:
```
ghcr.io/arcreactorkc/energy_projects_syncer:latest
```

With volumes:
```
/mnt/user/appdata/MyNewDb/data  → /data
/mnt/user/appdata/MyNewDb       → /config
```

And env vars:
```
MSSQL_DB=mynewdb
PG_DB=mynewdb
MSSQL_PASS=...
PG_PASS=...
```

4. Add a User Script to run it on a schedule.

---

## Databases with no custom tables (historian only)

If the database only has `sqlt_data_*` tables, skip the `tables.json` entirely
and just set `SQLT_DISCOVERY=true` (the default). No config file needed at all.

---

## Updating

Push a commit to `main` → GitHub Actions rebuilds the image automatically.
In Unraid, **Force Update** any container using the image to pull the latest version.
All containers across all databases update from the same single image.

