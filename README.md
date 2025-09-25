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
