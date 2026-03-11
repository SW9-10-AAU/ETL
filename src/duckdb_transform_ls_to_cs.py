from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import duckdb
import pyarrow as pa
from ukc_core.quadkey_utils import quadkey_to_int
from core.ls_poly_to_cs import ProcessResultStop, ProcessResultTraj, StopRow, TrajRow, process_stop_row, process_trajectory_row
from db_setup.duckdb.pyarrow_schemas import STOP_CS_SCHEMA, TRAJ_CS_SCHEMA

FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

BATCH_SIZE = 5000
MAX_WORKERS = 4

def transform_ls_trajectories_to_cs(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = MAX_WORKERS,
                                            batch_size: int = BATCH_SIZE):
    print(f"--- Processing trajectories with (using {max_workers} workers) ---")
    total_processed = 0
    total_cells_inserted = 0

    conn.execute("LOAD spatial")
    print(f"Processing trajectories in batches of {batch_size}...")
    
    # Use last processed trajectory as last_id
    max_id = conn.execute(f"""SELECT MAX(trajectory_id) FROM {db_schema}.trajectory_cs""").fetchone()
    last_id: int = max_id[0] if max_id and max_id[0] is not None else 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while True:
            print(f"Fetching batch of {batch_size} trajectories with trajectory_id > {last_id}...")
            batch: list[TrajRow] = [
                (int(tid), int(mmsi),  bytes(geom_wkb))
                for tid, mmsi, geom_wkb in conn.execute(f"""
                    SELECT trajectory_id, mmsi, ST_AsWKB(geom)
                    FROM {db_schema}.trajectory_ls
                    WHERE trajectory_id > ?
                    ORDER BY trajectory_id
                    LIMIT ?
                """, [last_id, batch_size]).fetchall()
            ]
            if not batch:
                break

            print(f"Processing batch of {len(batch)} trajectories (total processed so far: {total_processed:,})...")

            futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row) for row in batch]
            results: list[ProcessResultTraj] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

            print(f"Processed batch of {len(results)} trajectories, inserting into the database...")

            # Flatten: one row per cell
            trajectory_ids: list[int] = []
            mmsis: list[int] = []
            timestamps: list[int] = []
            cells: list[int] = []

            for trajectory_id, mmsi, cells_with_ts in results:
                for cell, ts in cells_with_ts:
                    trajectory_ids.append(trajectory_id)
                    mmsis.append(mmsi)
                    timestamps.append(ts) #seconds
                    cells.append(cell)
            if cells:
                arrow_table = pa.table({
                    "trajectory_id": pa.array(trajectory_ids, type=pa.int32()),
                    "mmsi":          pa.array(mmsis, type=pa.int64()),
                    "ts":            pa.array(timestamps, type=pa.timestamp("s", tz="UTC")),
                    "cell_z21":      pa.array(cells, type=pa.uint64()),
                }, schema=TRAJ_CS_SCHEMA)
                conn.execute(f"INSERT INTO {db_schema}.trajectory_cs SELECT * FROM arrow_table")
                total_cells_inserted += len(cells)

            last_id = batch[-1][0]
            total_processed += len(results)
            print(f"Inserted: {total_processed:,} trajectories ({total_cells_inserted:,} cells)")

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
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while True:
            print(f"Fetching batch of {batch_size} stops with stop_id > {last_id}...")
            batch: list[StopRow] = [
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

            futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
            results: list[ProcessResultStop] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")
                    
            # Flatten: one row per cell
            stop_ids: list[int] = []
            mmsis: list[int] = []
            ts_starts: list[int] = []
            ts_ends: list[int] = []
            cells: list[int] = []

            for stop_id, mmsi, ts_start, ts_end, cell_list in results:
                for cell in cell_list:
                    stop_ids.append(stop_id)
                    mmsis.append(mmsi)
                    ts_starts.append(ts_start)
                    ts_ends.append(ts_end)
                    cells.append(cell)
                    
            if cells:
                arrow_table = pa.table({
                    "stop_id":        pa.array(stop_ids, type=pa.int32()),
                    "mmsi":           pa.array(mmsis, type=pa.int64()),
                    "ts_start":       pa.array(ts_starts, type=pa.timestamp("s", tz="UTC")),
                    "ts_end":         pa.array(ts_ends, type=pa.timestamp("s", tz="UTC")),
                    "cell_z21":       pa.array(cells, type=pa.uint64()),
                }, schema=STOP_CS_SCHEMA)
                conn.execute(f"INSERT INTO {db_schema}.stop_cs SELECT * FROM arrow_table")

            last_id = batch[-1][0]
            total_processed += len(results)
            print(f"Inserted: {total_processed:,} stops in total")

    print(f"Finished processing all stops ({total_processed:,} total)")
