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
- `mat_points_view.py`: Create a Materialized View for points

### `mat_points_view.py`
Creates a materialized view named `points` in the PostgreSQL database (dw/ls_experiment/points). This view aggregates AIS points from a single MMSI taken from the `dw.fact.ais_point_fact` table. The resulting view contains six columns: `mmsi`, `geom` containing x, y, timestamp, `sog`, `cog`, `delta_sog`, `delta_depth_draught`.

This prepares points that can then be used for trajectory generation with stops.
### Run
- Win: `python ./src/mat_points_view.py`
- Mac: `python3 src/mat_points_view.py`

## Tests
-  `python -m unittest discover -s tests`