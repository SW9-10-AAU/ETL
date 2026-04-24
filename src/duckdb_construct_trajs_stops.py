import time
from collections import defaultdict
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from datetime import date

import duckdb

from core.points_to_ls_poly import (
    DictInputPoint,
    ProcessResult,
    Stop,
    Traj,
    process_single_mmsi,
)

FutureResult = Future[ProcessResult]  # Future returning ProcessResult


def get_latest_constructed_ts_duckdb(
    conn: duckdb.DuckDBPyConnection, output_schema: str
):
    """Fetch latest constructed ts_end across stop_poly and trajectory_ls."""
    row = conn.execute(
        f"""
        SELECT GREATEST(
            COALESCE((SELECT MAX(ts_end) FROM {output_schema}.stop_poly), TIMESTAMP '1970-01-01'),
            COALESCE((SELECT MAX(ts_end) FROM {output_schema}.trajectory_ls), TIMESTAMP '1970-01-01')
        ) AS latest_ts;
    """
    ).fetchone()
    return row[0] if row else None


def get_processing_days_duckdb(
    conn: duckdb.DuckDBPyConnection,
    points_schema: str,
    latest_ts,
) -> list[date]:
    """Fetch days that contain points newer than global latest constructed timestamp."""
    rows = conn.execute(
        f"""
        SELECT DISTINCT DATE(p.timestamp) AS point_day
        FROM {points_schema}.points p
        WHERE p.timestamp > ?
        ORDER BY point_day;
    """,
        [latest_ts],
    ).fetchall()
    return [point_day for (point_day,) in rows]


def get_mmsis_duckdb(
    conn: duckdb.DuckDBPyConnection,
    points_schema: str,
    point_day: date,
    latest_ts,
) -> list[int]:
    """Fetch MMSIs with unprocessed points for one day, ordered by number of points (descending)."""
    rows = conn.execute(
        f"""
        SELECT p.mmsi, COUNT(*) AS num_points
        FROM {points_schema}.points p
        WHERE DATE(p.timestamp) = ?
          AND p.timestamp > ?
        GROUP BY p.mmsi
        ORDER BY num_points DESC;
    """,
        [point_day, latest_ts],
    ).fetchall()
    return [int(mmsi) for mmsi, _ in rows]


def get_points_for_mmsis_in_batch_duckdb(
    conn: duckdb.DuckDBPyConnection,
    points_schema: str,
    point_day: date,
    mmsis: list[int],
    latest_ts,
) -> DictInputPoint:
    """Fetch one-day incremental points for MMSIs grouped by MMSI, ordered by time."""
    if not mmsis:
        return defaultdict(list)

    placeholders = ",".join(["?"] * len(mmsis))
    rows = conn.execute(
        f"""
        SELECT mmsi, lon, lat, sog, epoch_ts
        FROM {points_schema}.points
        WHERE mmsi IN ({placeholders})
          AND DATE(timestamp) = ?
          AND timestamp > ?
        ORDER BY mmsi, epoch_ts;
    """,
        [*mmsis, point_day, latest_ts],
    ).fetchall()

    grouped: DictInputPoint = defaultdict(list)
    for mmsi, lon, lat, sog, epoch_ts in rows:
        if mmsi is None or lon is None or lat is None or epoch_ts is None:
            continue
        grouped[int(mmsi)].append((lon, lat, sog, epoch_ts))
    return grouped


def construct_trajectories_and_stops(
    conn: duckdb.DuckDBPyConnection,
    points_schema: str,
    output_schema: str,
    max_workers: int = 4,
):
    """Construct trajectories and stops per day using global latest constructed timestamp."""
    latest_ts = get_latest_constructed_ts_duckdb(conn, output_schema)
    processing_days = get_processing_days_duckdb(conn, points_schema, latest_ts)

    insert_traj_query = f"""
        INSERT INTO {output_schema}.trajectory_ls (mmsi, ts_start, ts_end, geom)
        VALUES (?, to_timestamp(?), to_timestamp(?), ST_GeomFromWKB(?))
    """

    insert_stop_query = f"""
        INSERT INTO {output_schema}.stop_poly (mmsi, ts_start, ts_end, geom)
        VALUES (?, to_timestamp(?), to_timestamp(?), ST_GeomFromWKB(?))
    """

    if not processing_days:
        print("No days with unprocessed points.")
        return

    start_time = time.perf_counter()
    print(
        f"Processing {len(processing_days)} day(s) newer than global latest ts ({latest_ts}) using {max_workers} workers."
    )
    print(
        """
--------------------------------- Phases of processing each MMSI --------------------------------------------
[1] Parse input points into AISPoints (Point, SOG) tuples
[2] Iterate through points to construct candidate trajectories and stops
[3] Merge nearby candidate stops into merged stops
[4] Validate merged stops and add valid ones to final list of stops (with fallback to merge with trajs)
[4.1] Compute concave hull for merged stops
[4.2] Merge invalid stops with trajectories
[5] Validate candidate trajectories and add valid ones to final list of trajs
-------------------------------------------------------------------------------------------------------------"""
    )

    total_mmsis_processed = 0
    for day_num, point_day in enumerate(processing_days, start=1):
        day_start_time = time.perf_counter()
        day_mmsis = get_mmsis_duckdb(conn, points_schema, point_day, latest_ts)
        if not day_mmsis:
            continue

        print(
            f"\n=== Procesing batch {day_num}/{len(processing_days)}: {point_day.isoformat()} ({len(day_mmsis)} MMSIs: {day_mmsis[0]} to {day_mmsis[-1]}) ==="
        )

        batch_start_time = time.perf_counter()
        print(f"Fetching points for day {point_day.isoformat()}...")

        points = get_points_for_mmsis_in_batch_duckdb(
            conn,
            points_schema,
            point_day,
            day_mmsis,
            latest_ts,
        )

        point_count = sum(len(pts) for pts in points.values())
        print(
            f"{point_count:,} points fetched in {time.perf_counter() - batch_start_time:.2f}s."
        )

        trajs_to_insert: list[Traj] = []
        stops_to_insert: list[Stop] = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[FutureResult, int] = {
                executor.submit(process_single_mmsi, mmsi, points.get(mmsi, [])): mmsi
                for mmsi in day_mmsis
                if points.get(mmsi)
            }

            for future in as_completed(futures):
                mmsi = futures[future]
                try:
                    mmsi, trajs, stops = future.result()
                    trajs_to_insert.extend(trajs)
                    stops_to_insert.extend(stops)
                except Exception as e:
                    print(f"Error processing MMSI {mmsi}: {e}")
                    continue

        print(
            f"Processed batch {day_num}/{len(processing_days)}: {point_day.isoformat()} ({len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops). Inserting into database..."
        )

        if trajs_to_insert:
            conn.executemany(
                insert_traj_query,
                [
                    (mmsi, ts_start, ts_end, geom_wkb)
                    for (mmsi, ts_start, ts_end, geom_wkb) in trajs_to_insert
                ],
            )

        if stops_to_insert:
            conn.executemany(
                insert_stop_query,
                [
                    (mmsi, ts_start, ts_end, geom_wkb)
                    for (mmsi, ts_start, ts_end, geom_wkb) in stops_to_insert
                ],
            )

        total_mmsis_processed += len(day_mmsis)
        elapsed_time = time.perf_counter() - start_time
        batch_time = time.perf_counter() - batch_start_time
        print(
            f"Inserted batch results for day {point_day.isoformat()} | Elapsed: {elapsed_time:.2f}s | Batch time: {batch_time:.2f}s"
        )

        day_elapsed = time.perf_counter() - day_start_time
        print(
            f"Completed day {point_day.isoformat()} in {day_elapsed:.2f}s ({len(day_mmsis)} MMSIs)."
        )

    total_time = time.perf_counter() - start_time
    print("\nAll MMSIs processed.")
    print(
        f"Total time: {total_time/60:.2f} min | Avg per MMSI: {total_time/max(total_mmsis_processed, 1):.2f}s"
    )
