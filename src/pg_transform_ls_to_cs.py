from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from psycopg import Connection, Cursor
from psycopg import sql
from psycopg.abc import Query
from core.ls_poly_to_cs import ProcessResultStop, ProcessResultTraj, process_stop_row, process_trajectory_row

FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

BATCH_SIZE = 5000
MAX_WORKERS = 4

# --- Batch Helper ---

def get_batches(cur: Cursor, query: Query, batch_size: int):
    """Generator that yields rows in batches."""
    print(f"Fetching rows...")
    cur.execute(query)
    print(f"Fetched rows, processing in batches of {batch_size}...")
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows

# --- Main Transformation Functions ---

def transform_ls_trajectories_to_cs(connection: Connection, db_schema: str, max_workers: int = MAX_WORKERS,
                                    batch_size: int = BATCH_SIZE):
    print(f"--- Processing trajectories (using {max_workers} workers) ---")
    total_processed = 0
    insert_traj_query = sql.SQL("""
                INSERT INTO {db_schema}.trajectory_cs (trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """).format(db_schema=sql.Identifier(db_schema))
                
    with connection.cursor() as cur:
        get_trajs_query = sql.SQL("""
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM {db_schema}.trajectory_ls
                ORDER BY trajectory_id;
                """).format(db_schema=sql.Identifier(db_schema))

        for batch in get_batches(cur, get_trajs_query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row) for row in batch]
                results: list[ProcessResultTraj] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_traj_query,
                                       [(trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) for
                                        (trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) in
                                        results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} trajectories")

    print(f"Finished processing all trajectories ({total_processed:,} total)")

def transform_poly_stops_to_cs(connection: Connection, db_schema: str, max_workers: int = MAX_WORKERS, batch_size: int = BATCH_SIZE):
    print(f"--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0
    insert_stop_query = sql.SQL("""
                   INSERT INTO {db_schema}.stop_cs (stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """).format(db_schema=sql.Identifier(db_schema))

    with connection.cursor() as cur:
        get_stops_query = sql.SQL("""
                SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM {db_schema}.stop_poly
                ORDER BY stop_id;
                """).format(db_schema=sql.Identifier(db_schema))

        for batch in get_batches(cur, get_stops_query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
                results: list[ProcessResultStop] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_stop_query, [(stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) for
                                                      (stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) in results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} stops")

    print(f"Finished processing all stops ({total_processed:,} total)")