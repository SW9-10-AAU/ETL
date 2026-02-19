# ETL

ETL for trajectories

## Prerequisites

1. Install PostgreSQL
   - Win: You need to add the PostgreSQL bin folder (which contains libpq.dll) to your system's PATH
2. Create a virtual environment: `python -m venv .venv`
3. Activate environment
   - Win: `.\.venv\Scripts\Activate.ps1`
   - Mac: `source .venv/bin/activate`
4. Install requirements: `pip install -r requirements.txt`

## Run

1. Activate environment
   - Win: `.\.venv\Scripts\Activate.ps1`
   - Mac: `source .venv/bin/activate`
2. Run script:
   - Win: `python ./src/main.py`
   - Mac: `python3 ./src/main.py`

## Functions

- [`main.py`](/src/main.py): Main script to run all steps
- [`drop_all_tables.py`](/src/tables/drop_all_tables.py): Drops all tables in the schema
- [`mat_points_view.py`](/src/tables/mat_points_view.py): Creates a Materialized View for points
- [`create_cs_traj_stop_tables.py`](/src/tables/create_cs_traj_stop_tables.py): Creates CS tables for trajectory with stops

### [`main.py`](/src/main.py)

Main script to run all steps in order: drop tables, create materialized view, create trajectory and stop tables.

#### Run

- Win: `python ./src/main.py`
- Mac: `python3 src/main.py`

### [`drop_all_tables.py`](/src/tables/drop_all_tables.py)

Drops all tables in the PostgreSQL database (dw/{schema_name}/trajectory_cs, dw/{schema_name}/stop_cs, dw/{schema_name}/trajectory_ls, dw/{schema_name}/stop_poly, dw/{schema_name}/points).

### [`mat_points_view.py`](/src/tables/mat_points_view.py)

Creates a materialized view named `points` in the PostgreSQL database (dw/{schema_name}/points). This view aggregates AIS points from a single MMSI taken from the `dw.fact.ais_point_fact` table. The resulting view contains six columns: `mmsi`, `geom` containing x, y, timestamp, `sog`, `cog`, `delta_sog`, `delta_depth_draught`.

This prepares points that can then be used for trajectory generation with stops.

### [`create_cs_traj_stop_tables.py`](/src/tables/create_cs_traj_stop_tables.py)

Creates one table for trajectory and one table for stops in the PostgreSQL database (dw/{schema_name}/trajectory_cs).

## Draw areas and crossings

Use this tool to draw an area (Polygon) or a crossing (LineString): https://geojson.io/#map=6.47/55.777/10.723

### [`convert_area_polygon_to_cs.py`](/src/tables/convert_area_polygon_to_cs.py)

Converts an area (Polygon) to a CellString and uploads both Polygon and CellString to the PostgreSQL database (benchmark.area_poly, benchmark.area_cs).

### [`convert_crossing_linestring.py`](/src/convert_crossing_linestring.py)

Converts a crossing (LineString) to a CellString and uploads both LineString and CellString to the PostgreSQL database (benchmark.crossing_ls, benchmark.crossing_cs).
