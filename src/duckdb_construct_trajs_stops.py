import time
from collections import defaultdict
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import cast
import duckdb
from shapely import LineString, from_wkt, to_wkb

from core.points_to_ls_poly import DictAISPointWKB, ProcessResult, Stop, Traj, process_single_mmsi


def linestring_to_wkb_linestring_m(ls: LineString) -> bytes:
    """Rebuild a LineString (XYZ) as LineStringM, treating the third coordinate as M (epoch timestamp). Returns the LineStringM as WKB."""
    coords_m = " , ".join(f"{x} {y} {int(z)}" for x, y, z in ls.coords)
    
    return to_wkb(from_wkt(f"LINESTRING M ({coords_m})"))

BATCH_SIZE = 100 # Number of MMSIs to process in parallel
FutureResult = Future[ProcessResult] # Future returning ProcessResult

def get_mmsis_duckdb(conn: duckdb.DuckDBPyConnection, db_schema: str) -> list[int]:
    """Fetch MMSIs that still need processing, ordered by number of points (descending)."""
    rows = conn.execute(f"""
        SELECT p.mmsi, COUNT(*) AS num_points
        FROM {db_schema}.points p
        WHERE p.mmsi NOT IN (
            SELECT mmsi FROM {db_schema}.stop_poly
            UNION
            SELECT mmsi FROM {db_schema}.trajectory_ls
        )
        GROUP BY p.mmsi
        ORDER BY num_points DESC;
    """).fetchall()
    return [mmsi for mmsi, _ in rows]


def get_points_for_mmsis_in_batch_duckdb(conn: duckdb.DuckDBPyConnection, db_schema: str, mmsis: list[int]) -> DictAISPointWKB:
    """Fetch all points for multiple MMSIs, construct WKB PointM (with M=epoch_ts), grouped by MMSI."""
    placeholders = ','.join(['?'] * len(mmsis))
    rows = conn.execute(f"""
        SELECT mmsi, lon, lat, sog, epoch_ts
        FROM {db_schema}.points
        WHERE mmsi IN ({placeholders})
        ORDER BY mmsi, epoch_ts;
    """, mmsis).fetchall()

    grouped: DictAISPointWKB = defaultdict(list)
    for mmsi, lon, lat, sog, epoch_ts in rows:
        if mmsi is None or lon is None or lat is None or epoch_ts is None:
            continue
        pt = from_wkt(f"POINT M ({lon} {lat} {int(epoch_ts)})")
        grouped[int(mmsi)].append((pt.wkb, float(sog) if sog is not None else None))
    return grouped


def construct_trajectories_and_stops(conn: duckdb.DuckDBPyConnection, db_schema: str, max_workers: int = 4, batch_size: int = BATCH_SIZE):
    """Construct trajectories and stops for all MMSIs in DuckDB. Processes MMSIs in batches."""
    all_mmsis = get_mmsis_duckdb(conn, db_schema)
    
    insert_traj_query = f"""
        INSERT INTO {db_schema}.trajectory_ls (mmsi, ts_start, ts_end, geom)
        VALUES (?, to_timestamp(?), to_timestamp(?), ST_GeomFromWKB(?))
    """

    insert_stop_query = f"""
        INSERT INTO {db_schema}.stop_poly (mmsi, ts_start, ts_end, geom)
        VALUES (?, to_timestamp(?), to_timestamp(?), ST_GeomFromWKB(?))
    """

    num_mmsis = len(all_mmsis)
    if num_mmsis == 0:
        print("No MMSIs to process.")
        return

    start_time = time.perf_counter()
    print(f"Processing {num_mmsis} MMSIs in batches of {batch_size} MMSIs using {max_workers} workers.")

    for batch_start in range(0, num_mmsis, batch_size):
        mmsis_in_batch = all_mmsis[batch_start : batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        num_batches = (num_mmsis + batch_size - 1) // batch_size
        batch_start_time = time.perf_counter()
        print(f"\n--- Processing batch {batch_num} of {num_batches} ({len(mmsis_in_batch)} MMSIs: {mmsis_in_batch[0]} to {mmsis_in_batch[-1]}) ---")

        # Retrieve points for all MMSIs in the batch
        print(f"Fetching points for MMSIs in batch {batch_num}...")
        points = get_points_for_mmsis_in_batch_duckdb(conn, db_schema, mmsis_in_batch)
        print(f"{sum(len(pts) for pts in points.values()):,} points fetched.")

        trajs_to_insert: list[Traj] = []
        stops_to_insert: list[Stop] = []

        # Parallel processing of the batch of MMSIs
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[FutureResult, int] = {
                executor.submit(process_single_mmsi, mmsi, points[mmsi]): mmsi
                for mmsi in mmsis_in_batch
            }

            for future in as_completed(futures):
                mmsi = futures[future]
                try:
                    (mmsi, trajs, stops) = future.result()
                    trajs_to_insert.extend(trajs)
                    stops_to_insert.extend(stops)
                except Exception as e:
                    print(f"Error processing MMSI {mmsi}: {e}")
                    continue

        # Batch insert trajectories and stops
        if trajs_to_insert:
            conn.executemany(
                insert_traj_query,
                [(mmsi, ts_start, ts_end, linestring_to_wkb_linestring_m(geom)) for (mmsi, ts_start, ts_end, geom) in trajs_to_insert]
            )

        if stops_to_insert:
            conn.executemany(
                insert_stop_query,
                [(mmsi, ts_start, ts_end, geom.wkb) for (mmsi, ts_start, ts_end, geom) in stops_to_insert]
            )

        print(f"Batch {batch_num} inserted: {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops.")
        elapsed_time = time.perf_counter() - start_time
        batch_time = time.perf_counter() - batch_start_time
        print(f"Progress: {batch_num/num_batches*100:.2f}% | Elapsed time: {elapsed_time:.2f}s | Batch time: {batch_time:.2f}s | Avg per MMSI: {batch_time/len(mmsis_in_batch):.2f}s")

    total_time = time.perf_counter() - start_time
    print(f"\nAll MMSIs processed.")
    print(f"Total time: {total_time/60:.2f} min | Avg per MMSI: {total_time/num_mmsis:.2f}s")
