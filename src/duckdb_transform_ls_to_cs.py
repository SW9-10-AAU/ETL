from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import duckdb
import pyarrow as pa
from core.ls_poly_to_cs import (
    ProcessResultStop,
    ProcessResultTraj,
    StopRow,
    TrajRow,
    process_stop_row,
    process_trajectory_row,
)
from db_setup.duckdb.pyarrow_schemas import STOP_CS_SCHEMA, TRAJ_CS_SCHEMA

FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

BATCH_SIZE = 5000
MAX_WORKERS = 4


def calculate_exit_timestamps(
    cells_with_ts: list[tuple[int, int]], ts_end: int
) -> list[int]:
    if not cells_with_ts:
        return []

    exit_timestamps: list[int] = []
    for idx, (_, entry_ts) in enumerate(cells_with_ts):
        if idx + 1 < len(cells_with_ts):
            exit_ts = cells_with_ts[idx + 1][1]
        else:
            exit_ts = ts_end  # Use trajectory end time for the last cell

        # Ensure exit_ts >= entry_ts
        exit_timestamps.append(max(int(entry_ts), int(exit_ts)))

    return exit_timestamps


def transform_ls_trajectories_to_cs(
    conn: duckdb.DuckDBPyConnection,
    input_schema: str,
    output_schema: str,
    max_workers: int = MAX_WORKERS,
    batch_size: int = BATCH_SIZE,
):
    print(f"\n--- Processing trajectories with (using {max_workers} workers) ---")
    total_processed = 0
    total_cells_inserted = 0

    conn.execute("LOAD spatial")
    print(f"Processing trajectories in batches of {batch_size}...")

    traj_ids_to_process: list[int] = [row[0] for row in conn.execute(f"""
                SELECT trajectory_id FROM {input_schema}.trajectory_ls
                EXCEPT
                SELECT trajectory_id FROM {output_schema}.trajectory_cs
                ORDER BY trajectory_id;""").fetchall()]

    print(
        f"Found {len(traj_ids_to_process)} LineString trajectories to convert to CellString. Starting processing..."
    )
    next_index = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while next_index < len(traj_ids_to_process):
            batch_ids = traj_ids_to_process[next_index : next_index + batch_size]
            next_index += len(batch_ids)

            if not batch_ids:
                break

            print(f"Fetching batch of {len(batch_ids)} LineString trajectories...")
            batch: list[TrajRow] = [
                (int(tid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
                for tid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
                    SELECT trajectory_id, mmsi, EXTRACT(EPOCH FROM ts_start) AS ts_start, EXTRACT(EPOCH FROM ts_end) AS ts_end, ST_AsWKB(geom)
                    FROM {input_schema}.trajectory_ls
                    WHERE trajectory_id IN ({','.join(map(str, batch_ids))})
                    ORDER BY trajectory_id;
                """).fetchall()
            ]
            if not batch:
                break

            trajectory_end_by_id: dict[int, int] = {
                trajectory_id: ts_end for trajectory_id, _, _, ts_end, _ in batch
            }

            print(
                f"Processing batch of {len(batch)} trajectories (total processed so far: {total_processed:,})..."
            )

            futures: list[FutureResultTraj] = [
                executor.submit(process_trajectory_row, row) for row in batch
            ]
            results: list[ProcessResultTraj] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

            print(
                f"Processed batch of {len(results)} trajectories, inserting into the database..."
            )

            # Flatten: one row per cell
            cell_rows = [
                (trajectory_id, mmsi, ts_entry, ts_exit, cell)
                for trajectory_id, mmsi, cells_with_ts in results
                for (cell, ts_entry), ts_exit in zip(
                    cells_with_ts,
                    calculate_exit_timestamps(
                        cells_with_ts,
                        trajectory_end_by_id.get(
                            trajectory_id,
                            cells_with_ts[-1][1] if cells_with_ts else 0,
                        ),
                    ),
                )
            ]

            trajectory_ids: list[int] = []
            mmsis: list[int] = []
            ts_entries: list[int] = []
            ts_exits: list[int] = []
            cells: list[int] = []

            if cell_rows:
                trajectory_ids, mmsis, ts_entries, ts_exits, cells = map(
                    list, zip(*cell_rows)
                )
            if cells:
                traj_arrow_table = pa.table(
                    {
                        "trajectory_id": pa.array(trajectory_ids, type=pa.int32()),
                        "mmsi": pa.array(mmsis, type=pa.int64()),
                        "ts_entry": pa.array(
                            ts_entries, type=pa.timestamp("s", tz="UTC")
                        ),
                        "ts_exit": pa.array(ts_exits, type=pa.timestamp("s", tz="UTC")),
                        "cell_z21": pa.array(cells, type=pa.uint64()),
                    },
                    schema=TRAJ_CS_SCHEMA,
                )
                conn.execute(
                    f"INSERT INTO {output_schema}.trajectory_cs SELECT * FROM traj_arrow_table"
                )
                print(
                    f"Inserted batch of {len(results)} trajectories ({len(cells):,} cells)."
                )
                total_cells_inserted += len(cells)

            total_processed += len(results)
            print(
                f"Progress ({total_processed/len(traj_ids_to_process):.2%}): {total_processed:,} of {len(traj_ids_to_process):,} trajectories ({total_cells_inserted:,} cells)"
            )

    print(
        f"Finished processing all trajectories ({total_processed:,} trajectories, {total_cells_inserted:,} cells)"
    )


def transform_poly_stops_to_cs(
    conn: duckdb.DuckDBPyConnection,
    input_schema: str,
    output_schema: str,
    max_workers: int = MAX_WORKERS,
    batch_size: int = BATCH_SIZE,
):
    print(f"\n--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0
    total_cells_inserted = 0

    conn.execute("LOAD spatial")
    print(f"Processing stops in batches of {batch_size}...")

    stop_ids_to_process: list[int] = [row[0] for row in conn.execute(f"""
                SELECT stop_id FROM {input_schema}.stop_poly
                EXCEPT
                SELECT stop_id FROM {output_schema}.stop_cs
                ORDER BY stop_id;""").fetchall()]

    print(
        f"Found {len(stop_ids_to_process)} Polygon stops to convert to CellString. Starting processing..."
    )
    next_index = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        while next_index < len(stop_ids_to_process):
            batch_ids = stop_ids_to_process[next_index : next_index + batch_size]
            next_index += len(batch_ids)

            if not batch_ids:
                break

            print(f"Fetching batch of {len(batch_ids)} Polygon stops...")
            batch: list[StopRow] = [
                (int(sid), int(mmsi), ts_start, ts_end, bytes(geom_wkb))
                for sid, mmsi, ts_start, ts_end, geom_wkb in conn.execute(f"""
                    SELECT stop_id, mmsi, EXTRACT(EPOCH FROM ts_start) AS ts_start, EXTRACT(EPOCH FROM ts_end) AS ts_end, ST_AsWKB(geom)
                    FROM {input_schema}.stop_poly
                    WHERE stop_id IN ({','.join(map(str, batch_ids))})
                    ORDER BY stop_id;
                """).fetchall()
            ]
            if not batch:
                break

            futures: list[FutureResultStop] = [
                executor.submit(process_stop_row, row) for row in batch
            ]
            results: list[ProcessResultStop] = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")

            # Flatten: one row per cell
            stop_rows = [
                (stop_id, mmsi, ts_start, ts_end, cell)
                for stop_id, mmsi, ts_start, ts_end, cell_list in results
                for cell in cell_list
            ]

            stop_ids: list[int] = []
            mmsis: list[int] = []
            ts_starts: list[int] = []
            ts_ends: list[int] = []
            cells: list[int] = []

            if stop_rows:
                stop_ids, mmsis, ts_starts, ts_ends, cells = map(list, zip(*stop_rows))

            if cells:
                stop_arrow_table = pa.table(
                    {
                        "stop_id": pa.array(stop_ids, type=pa.int32()),
                        "mmsi": pa.array(mmsis, type=pa.int64()),
                        "ts_start": pa.array(
                            ts_starts, type=pa.timestamp("s", tz="UTC")
                        ),
                        "ts_end": pa.array(ts_ends, type=pa.timestamp("s", tz="UTC")),
                        "cell_z21": pa.array(cells, type=pa.uint64()),
                    },
                    schema=STOP_CS_SCHEMA,
                )
                conn.execute(
                    f"INSERT INTO {output_schema}.stop_cs SELECT * FROM stop_arrow_table"
                )
                print(f"Inserted batch of {len(results)} stops ({len(cells):,} cells).")
                total_cells_inserted += len(cells)

            total_processed += len(results)
            print(
                f"Progress ({total_processed/len(stop_ids_to_process):.2%}): {total_processed:,} of {len(stop_ids_to_process):,} stops ({total_cells_inserted:,} cells)"
            )

    print(
        f"Finished processing all stops ({total_processed:,} stops, {total_cells_inserted:,} cells)"
    )
