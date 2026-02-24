from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import duckdb
from core.ls_poly_to_cs import ProcessResultStop, ProcessResultTraj, Row, process_stop_row, process_trajectory_row

FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

BATCH_SIZE = 5000
MAX_WORKERS = 4

def transform_ls_trajectories_to_cs(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = MAX_WORKERS,
                                            batch_size: int = BATCH_SIZE):
    print(f"--- Processing trajectories with (using {max_workers} workers) ---")
    total_processed = 0
    insert_query = f"""
        INSERT INTO {db_schema}.trajectory_cs (trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    conn.execute("LOAD spatial")
    print("Fetching all trajectory rows...")
    all_rows : list[Row] = [
        (int(tid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
        for tid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
            SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsWKB(geom)
            FROM {db_schema}.trajectory_ls
            ORDER BY trajectory_id
        """).fetchall()
    ]
    print(f"Fetched {len(all_rows):,} rows, processing in batches of {batch_size}...")

    for i in range(0, len(all_rows), batch_size):
        batch = all_rows[i:i + batch_size]

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row) for row in batch]
            results: list[ProcessResultTraj] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

        conn.executemany(insert_query,
                         [(trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21)
                          for (trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21)
                          in results])

        total_processed += len(results)
        print(f"Processed total: {total_processed:,} trajectories")

    print(f"Finished processing all trajectories ({total_processed:,} total)")


def transform_poly_stops_to_cs(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = MAX_WORKERS,
                                     batch_size: int = BATCH_SIZE):
    print(f"--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0
    insert_query = f"""
        INSERT INTO {db_schema}.stop_cs (stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    conn.execute("LOAD spatial")
    print("Fetching all stop rows...")
    all_rows : list[Row] = [
        (int(sid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
        for sid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
            SELECT stop_id, mmsi, ts_start, ts_end, ST_AsWKB(geom)
            FROM {db_schema}.stop_poly
            ORDER BY stop_id
        """).fetchall()
    ]
    print(f"Fetched {len(all_rows):,} rows, processing in batches of {batch_size}...")

    for i in range(0, len(all_rows), batch_size):
        batch = all_rows[i:i + batch_size]

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
            results: list[ProcessResultStop] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

        conn.executemany(insert_query,
                         [(stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21)
                          for (stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21)
                          in results])

        total_processed += len(results)
        print(f"Processed total: {total_processed:,} stops")

    print(f"Finished processing all stops ({total_processed:,} total)")
