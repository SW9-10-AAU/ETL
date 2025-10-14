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
Drops all tables in the PostgreSQL database (dw/prototype1/trajectory_cs, dw/prototype1/stop_cs, dw/prototype1/trajectory_ls, dw/prototype1/stop_poly, dw/prototype1/points).

### [`mat_points_view.py`](/src/tables/mat_points_view.py)
Creates a materialized view named `points` in the PostgreSQL database (dw/prototype1/points). This view aggregates AIS points from a single MMSI taken from the `dw.fact.ais_point_fact` table. The resulting view contains six columns: `mmsi`, `geom` containing x, y, timestamp, `sog`, `cog`, `delta_sog`, `delta_depth_draught`.

This prepares points that can then be used for trajectory generation with stops.

### [`create_cs_traj_stop_tables.py`](/src/tables/create_cs_traj_stop_tables.py)
Creates one table for trajectory and one table for stops in the PostgreSQL database (dw/prototype1/trajectory_cs).
