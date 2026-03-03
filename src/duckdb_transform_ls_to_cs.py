from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import duckdb
import pyarrow as pa
from core.ls_poly_to_cs import ProcessResultStop, ProcessResultTraj, Row, process_stop_row, process_trajectory_row
from db_setup.duckdb.pyarrow_schemas import STOP_CS_SCHEMA, TRAJ_CS_SCHEMA

FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

BATCH_SIZE = 5000
MAX_WORKERS = 4

def transform_ls_trajectories_to_cs(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = MAX_WORKERS,
                                            batch_size: int = BATCH_SIZE):
    print(f"--- Processing trajectories with (using {max_workers} workers) ---")
    total_processed = 0

    conn.execute("LOAD spatial")
    print(f"Processing trajectories in batches of {batch_size}...")
    
    # Use last processed trajectory as last_id
    max_id = conn.execute(f"""SELECT MAX(trajectory_id) FROM {db_schema}.trajectory_cs""").fetchone()
    last_id: int = max_id[0] if max_id and max_id[0] is not None else 0
    
    while True:
        print(f"Fetching batch of {batch_size} trajectories with trajectory_id > {last_id}...")
        batch: list[Row] = [
            (int(tid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
            for tid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsWKB(geom)
                FROM {db_schema}.trajectory_ls
                WHERE trajectory_id > ?
                ORDER BY trajectory_id
                LIMIT ?
            """, [last_id, batch_size]).fetchall()
        ]
        if not batch:
            break

        print(f"Processing batch of {len(batch)} trajectories (total processed so far: {total_processed:,})...")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row) for row in batch]
            results: list[ProcessResultTraj] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

        print(f"Processed batch of {len(results)} trajectories, inserting into the database...")

        arrow_table = pa.table({
            "trajectory_id": pa.array([r[0] for r in results], type=pa.int32()),
            "mmsi":          pa.array([r[1] for r in results], type=pa.int64()),
            "ts_start":      pa.array([r[2] for r in results], type=pa.timestamp("us", tz="UTC")),
            "ts_end":        pa.array([r[3] for r in results], type=pa.timestamp("us", tz="UTC")),
            "cellstring_z13": pa.array([r[4] for r in results], type=pa.list_(pa.int32())),
            "cellstring_z17": pa.array([r[5] for r in results], type=pa.list_(pa.int64())),
            "cellstring_z21": pa.array([r[6] for r in results], type=pa.list_(pa.int64())),
        }, schema=TRAJ_CS_SCHEMA)
        conn.execute(f"INSERT INTO {db_schema}.trajectory_cs SELECT * FROM arrow_table")

        last_id = batch[-1][0]
        total_processed += len(results)
        print(f"Inserted: {total_processed:,} trajectories in total")

    print(f"Finished processing all trajectories ({total_processed:,} total)")


def transform_poly_stops_to_cs(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = MAX_WORKERS,
                                     batch_size: int = BATCH_SIZE):
    print(f"--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0

    conn.execute("LOAD spatial")
    print(f"Processing stops in batches of {batch_size}...")
    
    # Use last processed stop as last_id
    max_id = conn.execute(f"""SELECT MAX(stop_id) FROM {db_schema}.stop_cs""").fetchone()
    last_id: int = max_id[0] if max_id and max_id[0] is not None else 0
    
    while True:
        print(f"Fetching batch of {batch_size} stops with stop_id > {last_id}...")
        batch: list[Row] = [
            (int(sid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
            for sid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
                SELECT stop_id, mmsi, ts_start, ts_end, ST_AsWKB(geom)
                FROM {db_schema}.stop_poly
                WHERE stop_id > ?
                ORDER BY stop_id
                LIMIT ?
            """, [last_id, batch_size]).fetchall()
        ]
        if not batch:
            break

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
            results: list[ProcessResultStop] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

        arrow_table = pa.table({
            "stop_id":        pa.array([r[0] for r in results], type=pa.int32()),
            "mmsi":           pa.array([r[1] for r in results], type=pa.int64()),
            "ts_start":       pa.array([r[2] for r in results], type=pa.timestamp("us", tz="UTC")),
            "ts_end":         pa.array([r[3] for r in results], type=pa.timestamp("us", tz="UTC")),
            "cellstring_z13": pa.array([r[4] for r in results], type=pa.list_(pa.int32())),
            "cellstring_z17": pa.array([r[5] for r in results], type=pa.list_(pa.int64())),
            "cellstring_z21": pa.array([r[6] for r in results], type=pa.list_(pa.int64())),
        }, schema=STOP_CS_SCHEMA)
        conn.execute(f"INSERT INTO {db_schema}.stop_cs SELECT * FROM arrow_table")

        last_id = batch[-1][0]
        total_processed += len(results)
        print(f"Inserted: {total_processed:,} stops in total")

    print(f"Finished processing all stops ({total_processed:,} total)")
