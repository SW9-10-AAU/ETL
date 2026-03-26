# ETL Repository - Copilot Agent Instructions

## Repository Summary

This repository contains an ETL pipeline for AIS (Automatic Identification System) trajectory processing with two backend implementations:

- PostgreSQL/PostGIS backend
- DuckDB backend

The pipeline:

- Builds/refreshes source points datasets
- Constructs LineString trajectories and Polygon stops from AIS points
- Transforms LineString/Polygon outputs into CellString representations
- Persists outputs to backend-specific schemas/tables

## Major architecture updates

Compared to earlier versions, this repository now includes:

1. DuckDB support in parallel with PostgreSQL
2. Shared core logic moved to `src/core/`
3. Backend-specific orchestration split into dedicated modules
4. Step-level execution controls (confirm/skip or env-driven)
5. Source-schema vs CellString-schema routing for reusable construct outputs
6. Performance-oriented batching and multiprocessing across construct/transform flows

## Current project layout

```text
ETL/
├── .github/
│   ├── copilot-instructions.md
│   └── workflows/python-app.yml
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
│   │   ├── points_to_ls_poly.py
│   │   ├── ls_poly_to_cs.py
│   │   ├── cellstring_utils.py
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

## Build and validation workflow

### Environment setup

1. Create and activate a virtual environment
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install lint tool used by CI:

```bash
pip install flake8
```

### Validation commands

Run these before proposing completion:

```bash
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
python -m unittest discover -s tests
```

### CI workflow reference

Workflow file: `.github/workflows/python-app.yml`

CI currently:

- Uses Python 3.11
- Runs both flake8 stages
- Runs unittest discovery in `tests/`

## Runtime configuration model

Configuration is loaded from `.env` via `python-dotenv`.

Core selectors:

- `DB_BACKEND=duckdb|postgresql`
- `DUCKDB_PATH` for DuckDB
- `POSTGRESQL_URL` for PostgreSQL

Schema routing:

- Base schemas: `DUCKDB_SCHEMA`, `POSTGRESQL_SCHEMA`
- Optional source schemas: `DUCKDB_SOURCE_SCHEMA`, `POSTGRESQL_SOURCE_SCHEMA`
- Optional CellString schemas: `DUCKDB_CS_SCHEMA`, `POSTGRESQL_CS_SCHEMA`

Source schema stores:

- `points`
- `trajectory_ls`
- `stop_poly`

CellString schema stores:

- `trajectory_cs`
- `stop_cs`

If source/CS schema variables are unset, code falls back to base schema variables.

## Step execution controls

`src/main.py` now supports per-step execution decisions:

1. Drop tables
2. Create schema(s)
3. Create tables
4. Create points
5. Construct trajectories/stops
6. Transform trajectories/stops to CellStrings

Control methods:

- Interactive prompt per step
- Optional env overrides:
  - `ETL_DROP`
  - `ETL_CREATE_SCHEMA`
  - `ETL_CREATE_TABLES`
  - `ETL_CREATE_POINTS`
  - `ETL_CONSTRUCT`
  - `ETL_TRANSFORM`

Accepted boolean values:

- True: `y`, `yes`, `1`, `true`
- False: `n`, `no`, `0`, `false`

## Backend implementation boundaries

### Shared logic (backend-agnostic)

`src/core/` contains reusable processing logic for:

- trajectory/stop construction rules from point streams
- LS/Polygon to CellString conversion utilities
- shared helper functions and types

### DuckDB-specific

- construct: `src/duckdb_construct_trajs_stops.py`
- transform: `src/duckdb_transform_ls_to_cs.py`
- setup: `src/db_setup/duckdb/*`

Notes:

- Uses DuckDB spatial extension
- Uses Arrow tables (`pyarrow`) for efficient CellString inserts

### PostgreSQL-specific

- construct: `src/pg_construct_trajs_stops.py`
- transform: `src/pg_transform_ls_to_cs.py`
- setup: `src/db_setup/postgresql/*`

Notes:

- Uses PostGIS geometry types and SQL functions
- Uses server-side schema/table/materialized-view creation scripts

## Performance characteristics to preserve

When editing construct/transform logic, preserve:

- batch-oriented processing
- multiprocessing via `ProcessPoolExecutor`
- dedupe/skip semantics based on existing target rows
- backend-specific optimized insertion strategy (Arrow for DuckDB)

Avoid regressions that force row-by-row processing unless explicitly requested.

## Common change guidance

1. Keep shared logic in `src/core` when backend-neutral
2. Keep backend-specific SQL and DDL in corresponding backend modules
3. Do not reintroduce old `src/tables` paths; they are obsolete
4. Keep schema routing explicit (source vs CS)
5. If modifying orchestration, keep per-step controls consistent across both backends

## Testing guidance for agents

After changes:

1. Run critical flake8 check
2. Run tests in `tests/`
3. If touching backend-specific SQL paths, ensure no syntax/identifier drift
4. If modifying schema routing, verify references still point to correct source or CS schema role

## Trust these instructions

These instructions are intended to reflect the current repository structure and execution model. If implementation and docs diverge, prioritize the actual code and update these instructions in the same PR.
