from collections import defaultdict
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from psycopg import Connection, Cursor
from psycopg import sql
from core.points_to_ls_poly import (
    AISPointRow,
    DictInputPoint,
    InputPoint,
    ProcessResult,
    Stop,
    Traj,
    process_single_mmsi,
)

BATCH_SIZE = 50  # Number of MMSIs to process in parallel
FutureResult = Future[ProcessResult]  # Future returning ProcessResult


def construct_trajectories_and_stops(
    conn: Connection,
    points_schema: str,
    output_schema: str,
    max_workers: int = 4,
    batch_size: int = BATCH_SIZE,
):
    """Construct trajectories and stops for all MMSIs in the database. Processes MMSIs in batches."""
    cur = conn.cursor()
    all_mmsis = get_mmsis(cur, points_schema, output_schema)
    cur.close()

    insert_traj_query = sql.SQL(
        """
            INSERT INTO {db_schema}.trajectory_ls (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
        """
    ).format(db_schema=sql.Identifier(output_schema))

    insert_stop_query = sql.SQL(
        """
            INSERT INTO {db_schema}.stop_poly (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
        """
    ).format(db_schema=sql.Identifier(output_schema))

    num_mmsis = len(all_mmsis)
    if num_mmsis == 0:
        print("No MMSIs to process.")
        return

    start_time = time.perf_counter()
    print(
        f"Processing {num_mmsis} MMSIs in batches of {batch_size} MMSIs using {max_workers} workers."
    )

    with conn.cursor() as read_cur:
        # Iterate in batches
        for batch_start in range(0, num_mmsis, batch_size):
            mmsis_in_batch = all_mmsis[batch_start : batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            num_batches = (num_mmsis + batch_size - 1) // batch_size
            batch_start_time = time.perf_counter()
            print(
                f"\n--- Processing batch {batch_num} of {num_batches} ({len(mmsis_in_batch)} MMSIs: {mmsis_in_batch[0]} to {mmsis_in_batch[-1]}) ---"
            )

            # Retrieve points for all MMSIs in the batch
            print(f"Fetching points for MMSIs in batch {batch_num}...")
            points: DictInputPoint = get_points_for_mmsis_in_batch(
                read_cur, points_schema, mmsis_in_batch
            )
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

            print(
                f"Batch {batch_num} processed: {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops. Inserting into database..."
            )

            # Batch insert trajectories and stops into the database
            with conn.cursor() as insert_cur:
                if trajs_to_insert:
                    insert_cur.executemany(
                        insert_traj_query,
                        [
                            (mmsi, ts_start, ts_end, geom_wkb)
                            for (mmsi, ts_start, ts_end, geom_wkb) in trajs_to_insert
                        ],
                    )

                if stops_to_insert:
                    insert_cur.executemany(
                        insert_stop_query,
                        [
                            (mmsi, ts_start, ts_end, geom_wkb)
                            for (mmsi, ts_start, ts_end, geom_wkb) in stops_to_insert
                        ],
                    )

            conn.commit()

            print(
                f"Batch {batch_num} inserted: {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops."
            )
            elapsed_time = time.perf_counter() - start_time
            batch_time = time.perf_counter() - batch_start_time
            print(
                f"Progress: {batch_num/num_batches*100:.2f}% | Elapsed time: {elapsed_time:.2f}s | Batch time: {batch_time:.2f}s | Avg per MMSI: {batch_time/batch_size:.2f}s"
            )

    total_time = time.perf_counter() - start_time
    print(f"\nAll MMSIs processed.")
    print(
        f"Total time: {total_time/60:.2f} min | Avg per MMSI: {total_time/num_mmsis:.2f}s"
    )


# ----------------------------------------------------------------------


def get_mmsis(cur: Cursor, points_schema: str, output_schema: str) -> list[int]:
    """Fetch MMSIs that still need processing, ordered by number of points (descending)."""
    query = sql.SQL(
        """
            SELECT p.mmsi, COUNT(*) AS num_points
            FROM {points_schema}.points p
            WHERE p.mmsi NOT IN (
                SELECT mmsi FROM {output_schema}.stop_poly
                UNION
                SELECT mmsi FROM {output_schema}.trajectory_ls
            )
            GROUP BY p.mmsi
            ORDER BY num_points DESC;
        """
    ).format(
        points_schema=sql.Identifier(points_schema),
        output_schema=sql.Identifier(output_schema),
    )

    cur.execute(query)

    rows: list[tuple[int, int]] = cur.fetchall()
    return [mmsi for mmsi, _ in rows]


def get_points_for_mmsis_in_batch(
    cur: Cursor, db_schema: str, mmsis: list[int]
) -> DictInputPoint:
    """Fetch all points for multiple MMSIs grouped by MMSI, ordered by time."""

    query = sql.SQL(
        """
            SELECT mmsi, ST_AsBinary(geom), sog
            FROM {db_schema}.points
            WHERE mmsi = ANY(%s)
            ORDER BY mmsi, ST_M(geom);
        """
    ).format(db_schema=sql.Identifier(db_schema))

    cur.execute(query, (mmsis,))

    rows: list[AISPointRow] = cur.fetchall()

    # Group points by MMSI
    grouped_points: DictInputPoint = defaultdict(list)

    for mmsi, geom_wkb, sog in rows:
        if mmsi is None or geom_wkb is None:
            continue

        grouped_points[mmsi].append((geom_wkb, sog))

    return grouped_points
