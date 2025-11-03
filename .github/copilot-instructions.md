# ETL Repository - Copilot Agent Instructions

## Repository Summary

This is an **ETL (Extract, Transform, Load) pipeline** for processing AIS (Automatic Identification System) trajectory data. The repository contains Python scripts that:

- Extract ship trajectory data from a PostgreSQL/PostGIS database
- Transform LineString geometries to Cell-String representations using map tiles (MVT/Mercantile at zoom level 21)
- Construct trajectories and stops from raw AIS point data using parallel processing
- Load transformed data back into PostgreSQL tables

**Repository Size:** ~350KB | **Language:** Python 3.11-3.12 | **Files:** 10 Python files (~755 lines of code)

**Key Technologies:**

- PostgreSQL with PostGIS extension for spatial data
- Python libraries: psycopg (3.2.10), shapely (2.1.1), mercantile (1.2.1), geopy (2.4.1), numpy (2.3.3)
- Unit testing with Python's unittest framework
- Linting with flake8

## Build & Validation Process

### Prerequisites (Critical - ALWAYS Required)

1. **PostgreSQL Installation Required**: This project requires PostgreSQL with PostGIS extension for database operations

   - On Windows: Add PostgreSQL bin folder (containing libpq.dll) to system PATH
   - Database connection string must be configured in `.env` file

2. **Python Version**: Python 3.11 or 3.12 (CI uses Python 3.11)

### Environment Setup (ALWAYS Follow This Order)

**Step 1: Install Dependencies** (ALWAYS run this first)

```bash
pip install -r requirements.txt
```

- This installs all required packages including psycopg, shapely, mercantile, geopy, numpy, python-dotenv
- Typical installation time: 10-30 seconds
- Must be run before ANY other commands

**Step 2: Install Development Tools** (Required for linting/CI validation)

```bash
pip install flake8
```

**Step 3: Configure Database Connection** (Required for running the application)

- Copy `.env.example` to `.env`
- Update `DATABASE_URL` with actual PostgreSQL connection string:
  ```
  DATABASE_URL=postgresql://{username}:{password}@{serverip}:{port}/{dbname}
  ```
- The application will exit with error "DATABASE_URL not defined in .env file" if this is missing

### Linting (ALWAYS Run Before Committing)

The CI pipeline runs flake8 in two stages:

**Stage 1: Critical Syntax Errors** (Build fails if this fails)

```bash
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
```

- Checks for Python syntax errors and undefined names
- Exit code MUST be 0 (no errors)

**Stage 2: Code Quality Warnings** (Non-blocking, exit-zero)

```bash
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
```

- Max line length: 127 characters
- Max complexity: 10
- Does not fail the build, but warnings should be addressed

**Known Linting Issues** (Pre-existing, not blockers):

- ~189 total warnings in existing code (mostly E231, W293, E302, E261)
- These are informational and don't block CI

### Testing (ALWAYS Run Before Committing)

**Run All Tests:**

```bash
python -m unittest discover -s tests
```

- Expected output: "Ran 11 tests in ~0.007s" with "OK"
- Tests must pass for CI to succeed
- Test files location: `tests/` directory

**Test Files:**

- `tests/test_connect.py` - Database connection tests (mocked)
- `tests/test_transform_ls_to_cs.py` - Geometry transformation tests

**Important**: Tests use mocking for database connections, so they can run without an actual database

### Running the Application

**Main Entry Point:**

```bash
python src/main.py
# or on some systems
python3 src/main.py
```

**Prerequisites for Running:**

- Valid `.env` file with `DATABASE_URL` configured
- PostgreSQL database must be accessible
- Database schema `prototype2` should exist

**Expected Behavior:**

- Creates database tables for trajectories and stops
- Processes AIS point data in parallel (uses CPU cores, max 12)
- Inserts transformed data into PostgreSQL tables

## Project Architecture & Layout

### Directory Structure

```
/home/runner/work/ETL/ETL/
├── .github/
│   └── workflows/
│       └── python-app.yml          # CI/CD workflow (Python 3.11)
├── src/                             # Main source code
│   ├── main.py                      # Entry point - orchestrates entire ETL pipeline
│   ├── connect.py                   # Database connection utility
│   ├── construct_trajs_stops.py     # Parallel processing of trajectories/stops (268 lines)
│   ├── transform_ls_to_cs.py        # LineString to CellString transformation (126 lines)
│   └── tables/                      # Database table creation scripts
│       ├── create_ls_traj_stop_tables.py   # LineString tables
│       ├── create_cs_traj_stop_tables.py   # CellString tables
│       ├── mat_points_view.py              # Materialized view creation
│       └── drop_all_tables.py              # Table cleanup utility
├── tests/                           # Unit tests (unittest framework)
│   ├── test_connect.py              # Database connection tests
│   └── test_transform_ls_to_cs.py   # Transformation logic tests
├── .env.example                     # Template for database configuration
├── .gitignore                       # Standard Python gitignore
├── requirements.txt                 # Python dependencies (10 packages)
├── README.md                        # User documentation
└── LICENSE                          # Project license
```

### Key Source Files

**src/main.py** - Main Orchestrator (42 lines)

- Loads environment variables with `python-dotenv`
- Validates DATABASE_URL exists
- Calls ETL steps in sequence:
  1. `create_ls_traj_stop_tables()` - Create LineString tables
  2. `create_cs_traj_stop_tables()` - Create CellString tables
  3. `construct_trajectories_and_stops()` - Process data (parallel, CPU-bound)
  4. Optional steps commented out: `drop_all_tables()`, `mat_points_view()`, transform functions

**src/connect.py** - Database Connection (11 lines)

- Single function: `connect_to_db()` returns psycopg.Connection
- Reads DATABASE_URL from environment
- Exits with error message if DATABASE_URL not set

**src/construct_trajs_stops.py** - Core Processing Logic (268 lines)

- Implements trajectory and stop detection algorithm from research paper
- Uses multiprocessing (ProcessPoolExecutor) for parallel MMSI processing
- Constants (threshold values): STOP_SOG_THRESHOLD=1.0, STOP_DISTANCE_THRESHOLD=250m, etc.
- Key functions:
  - `construct_trajectories_and_stops()` - Main parallel processing function
  - `process_single_mmsi()` - Processes single ship's trajectory
  - `insert_trajectory()`, `insert_stop()` - Database insertion
  - `get_mmsis()` - Queries distinct MMSIs from database

**src/transform_ls_to_cs.py** - Geometry Transformation (126 lines)

- Converts LineString/Polygon geometries to CellString (map tile IDs)
- Uses Mercantile for MVT tile calculations at zoom level 21
- Implements Bresenham's line algorithm for tile interpolation
- Key functions:
  - `encode_lonlat_to_cellid()` - Converts lat/lon to cell ID
  - `convert_linestring_to_cellstring()` - LineString → cell array
  - `convert_polygon_to_cellstring()` - Polygon → cell array
  - `is_unique_cells()` - Checks if all cells in array are unique

### Database Schema

**Schema:** `prototype2` (all tables/views in this schema)

**Tables Created:**

- `trajectory_ls` - LineString trajectories (geometry column: LINESTRINGM)
- `stop_poly` - Stop polygons (geometry column: POLYGON)
- `trajectory_cs` - CellString trajectories (cellstring: bigint ARRAY)
- `stop_cs` - CellString stops (cellstring: bigint ARRAY)
- `points` - Materialized view of AIS points (POINTM with MMSI)

**Important:** All tables have indexes on mmsi, timestamps, and geometry/cellstring columns

### GitHub Actions CI Pipeline

**Workflow:** `.github/workflows/python-app.yml`

- **Trigger:** Pull requests to `main` branch
- **Runner:** ubuntu-latest
- **Python Version:** 3.11 (specific version)

**CI Steps (in order):**

1. Checkout code
2. Set up Python 3.11
3. Install dependencies: `pip install --upgrade pip && pip install flake8 && pip install -r requirements.txt`
4. Lint with flake8 (critical errors only): `flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics`
5. Lint with flake8 (all warnings): `flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics`
6. Run tests: `python -m unittest discover -s tests || exit 1`

**CI Success Criteria:**

- No syntax errors (E9,F63,F7,F82)
- All 11 unit tests pass
- Warnings are informational only (exit-zero)

## Common Workflows & Pitfalls

### Making Code Changes

1. **ALWAYS** install dependencies first: `pip install -r requirements.txt && pip install flake8`
2. Make your code changes
3. Run syntax check: `flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics`
4. Run all tests: `python -m unittest discover -s tests`
5. Check full linting: `flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics`
6. Commit changes

### Adding New Dependencies

1. Update `requirements.txt` with version pinning (e.g., `package==X.Y.Z`)
2. Run `pip install -r requirements.txt` to verify installation
3. Ensure CI pipeline still passes

### Database-Related Changes

- **Never run** the application in CI - it requires a real PostgreSQL database
- Tests use `unittest.mock` to mock database connections
- When modifying database queries, update corresponding mocks in tests
- The application requires the `prototype2` schema to exist in PostgreSQL

### File Naming & Import Conventions

- All imports use relative paths from the src/ directory
- Tests add `sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))` to import src modules
- Module imports in src/ assume running from repository root

### Parallel Processing Notes

- `construct_trajectories_and_stops()` uses `ProcessPoolExecutor` with max workers = min(CPU count, 12)
- Each worker process creates its own database connection
- Database connection string passed as parameter (not shared connection objects)
- Progress tracking with ETA calculation based on completed MMSI count

## Validation Checklist

Before submitting a PR, ensure:

- [ ] `pip install -r requirements.txt` completes successfully
- [ ] `pip install flake8` completes successfully
- [ ] `flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics` returns 0 errors
- [ ] `python -m unittest discover -s tests` shows "Ran 11 tests" with "OK"
- [ ] No new syntax errors or undefined names introduced
- [ ] Code follows max line length of 127 characters where practical
- [ ] If modifying database queries, corresponding tests are updated

## Trust These Instructions

These instructions have been validated by running all commands in a clean environment. If you encounter issues not documented here, it likely indicates a new problem that should be investigated. Only perform additional exploration if these instructions are incomplete or found to be incorrect.
