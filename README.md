# ETL

ETL pipeline for AIS trajectory processing with support for both PostgreSQL/PostGIS and DuckDB.

## What changed since last semester

- Added DuckDB backend support alongside PostgreSQL.
- Reorganized shared logic into `src/core/` modules used by both backend implementations.
- Refactored backend-specific setup and execution into `src/db_setup/duckdb/` and `src/db_setup/postgresql/`.
- Added step-level confirmations and environment-based step toggles in `src/main.py`.
- Added schema split support so source data and CellString outputs can be stored separately.
- Added performance-focused batching/parallel processing paths and Arrow-based DuckDB inserts for CellString transforms.

## Repository structure

```text
ETL/
├── .github/
│   ├── copilot-instructions.md
│   └── workflows/
│       └── python-app.yml
├── ais_data/
├── geojson/
├── src/
│   ├── main.py
│   ├── duckdb_construct_trajs_stops.py
│   ├── duckdb_transform_ls_to_cs.py
│   ├── pg_construct_trajs_stops.py
│   ├── pg_transform_ls_to_cs.py
│   ├── convert_area_geojson.py
│   ├── convert_area_polygon.py
│   ├── convert_area_polygons_to_cellstring.py
│   ├── convert_crossing_linestring.py
│   ├── core/
│   │   ├── cellstring_utils.py
│   │   ├── ls_poly_to_cs.py
│   │   ├── points_to_ls_poly.py
│   │   └── utils.py
│   └── db_setup/
│       ├── duckdb/
│       │   ├── create_duckdb_points.py
│       │   ├── create_duckdb_tables.py
│       │   ├── drop_duckdb_tables.py
│       │   └── pyarrow_schemas.py
│       ├── postgresql/
│       │   ├── create_postgresql_tables.py
│       │   ├── create_ls_traj_stop_tables.py
│       │   ├── create_cs_traj_stop_tables.py
│       │   ├── create_area_tables.py
│       │   ├── create_crossing_tables.py
│       │   ├── mat_points_view.py
│       │   └── drop_postgresql_tables.py
│       └── utils/
│           ├── connect.py
│           └── db_utils.py
├── tests/
│   ├── test_connect.py
│   ├── test_linecover_same_cell.py
│   └── test_transform_ls_to_cs.py
├── requirements.txt
└── README.md
```

## Prerequisites

1. Python 3.11 or 3.12
2. Create and activate virtual environment
   - Windows: `python -m venv .venv` and `.\.venv\Scripts\Activate.ps1`
   - macOS/Linux: `python3 -m venv .venv` and `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`

Backend-specific prerequisites:

- PostgreSQL backend (`DB_BACKEND=postgresql`):
  - PostgreSQL + PostGIS available
  - On Windows, PostgreSQL `bin` (contains `libpq.dll`) must be in `PATH`
- DuckDB backend (`DB_BACKEND=duckdb`):
  - No database server required
  - DuckDB spatial extension is installed/loaded by the pipeline

## Configuration

Copy `.env.example` to `.env` and configure values.

Required core settings:

- `DB_BACKEND`: `duckdb` or `postgresql`
- `DUCKDB_PATH` when using DuckDB
- `POSTGRESQL_URL` when using PostgreSQL

Schema settings:

- Base (backward-compatible): `DUCKDB_SCHEMA`, `POSTGRESQL_SCHEMA`
- Optional split schema setup:
  - `DUCKDB_SOURCE_SCHEMA`, `POSTGRESQL_SOURCE_SCHEMA` for `points`, `trajectory_ls`, `stop_poly`
  - `DUCKDB_CS_SCHEMA`, `POSTGRESQL_CS_SCHEMA` for `trajectory_cs`, `stop_cs`

If split schema vars are not set, they fall back to backend base schema vars.

This supports creating multiple CellString variants in separate schemas without re-running construction of LS trajectories/stops.

## Run ETL

- Windows: `python ./src/main.py`
- macOS/Linux: `python3 ./src/main.py`

Execution is step-driven with confirm/skip prompts:

1. Drop source/LS tables
2. Drop CellString tables
3. Create schema(s)
4. Create tables
5. Create points table/materialized view
6. Construct trajectories and stops
7. Transform trajectories/stops to CellStrings

## Optional non-interactive step toggles

Set these env vars to bypass prompts for specific steps:

- `ETL_DROP` (legacy fallback; applies to both drop prompts)
- `ETL_DROP_LS`
- `ETL_DROP_CS`
- `ETL_CREATE_SCHEMA`
- `ETL_CREATE_TABLES`
- `ETL_CREATE_POINTS`
- `ETL_CONSTRUCT`
- `ETL_TRANSFORM`

Accepted values: `y`, `yes`, `1`, `true`, `n`, `no`, `0`, `false`.

## Validation

Run the same checks as CI:

- `flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics`
- `flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics`
- `python -m unittest discover -s tests`

## Draw areas and crossings

Use https://geojson.io/#map=6.47/55.777/10.723 to create polygon/linestring inputs.

Relevant scripts:

- `src/convert_area_geojson.py`
- `src/convert_area_polygon.py`
- `src/convert_area_polygons_to_cellstring.py`
- `src/convert_crossing_linestring.py`
