import re
import time
from datetime import date, datetime
from pathlib import Path

import duckdb

from db_setup.utils.db_utils import get_ais_data_path, get_ais_default_period
from prompt_utils import prompt_optional_date_range

AIS_FILE_PATTERN = re.compile(r"^aisdk-(\d{4}-\d{2}-\d{2})\.pq$")
TEMP_AIS_STAGE = "_selected_ais_data_tmp"


def parse_ais_file_date(file_name: str) -> date | None:
    match = AIS_FILE_PATTERN.match(file_name)
    if not match:
        return None
    return date.fromisoformat(match.group(1))


def discover_ais_parquet_files(ais_data_path: str) -> list[tuple[str, str, date]]:
    base_dir = Path(ais_data_path)
    if not base_dir.exists() or not base_dir.is_dir():
        raise ValueError(
            f"AIS_DATA_PATH '{ais_data_path}' does not exist or is not a directory."
        )

    discovered: list[tuple[str, str, date]] = []
    for file_path in sorted(base_dir.rglob("*.pq")):
        parsed_date = parse_ais_file_date(file_path.name)
        if parsed_date is None:
            continue
        discovered.append((str(file_path), file_path.name, parsed_date))

    discovered.sort(key=lambda item: item[2])
    return discovered


def filter_files_by_watermark_and_period(
    files: list[tuple[str, str, date]],
    watermark_date: date | None,
    start_date: date | None,
    end_date: date | None,
) -> list[tuple[str, str, date]]:
    selected: list[tuple[str, str, date]] = []
    for file_path, file_name, file_date in files:
        if watermark_date is not None and file_date <= watermark_date:
            continue
        if start_date is not None and file_date < start_date:
            continue
        if end_date is not None and file_date > end_date:
            continue
        selected.append((file_path, file_name, file_date))
    return selected


def _ensure_points_table(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.points (
            mmsi BIGINT NOT NULL,
            lat DOUBLE NOT NULL,
            lon DOUBLE NOT NULL,
            sog DOUBLE,
            timestamp TIMESTAMP NOT NULL,
            epoch_ts DOUBLE NOT NULL
        );
    """
    )


def _ensure_ingestion_log_table(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.points_ingestion_log (
            file_name TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            file_date DATE NOT NULL,
            min_ts TIMESTAMP,
            max_ts TIMESTAMP,
            loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """
    )


def _get_ingestion_watermark(
    conn: duckdb.DuckDBPyConnection, db_schema: str
) -> datetime | None:
    max_logged_row = conn.execute(
        f"SELECT MAX(max_ts) FROM {db_schema}.points_ingestion_log"
    ).fetchone()
    max_logged_ts = max_logged_row[0] if max_logged_row else None
    if max_logged_ts is not None:
        return max_logged_ts

    max_points_row = conn.execute(
        f"SELECT MAX(timestamp) FROM {db_schema}.points"
    ).fetchone()
    max_points_ts = max_points_row[0] if max_points_row else None
    return max_points_ts


def _create_staging_table(
    conn: duckdb.DuckDBPyConnection, selected_files: list[tuple[str, str, date]]
):
    conn.execute(f"DROP TABLE IF EXISTS {TEMP_AIS_STAGE};")
    first_path = selected_files[0][0]
    conn.execute(
        f"CREATE TEMP TABLE {TEMP_AIS_STAGE} AS SELECT * FROM read_parquet(?) LIMIT 0",
        [first_path],
    )
    for file_path, _, _ in selected_files:
        conn.execute(
            f"INSERT INTO {TEMP_AIS_STAGE} SELECT * FROM read_parquet(?)",
            [file_path],
        )


def _insert_incremental_points(conn: duckdb.DuckDBPyConnection, db_schema: str) -> int:
    before_count_row = conn.execute(
        f"SELECT COUNT(*) FROM {db_schema}.points"
    ).fetchone()
    before_count = before_count_row[0] if before_count_row else 0

    conn.execute(
        f"""
        INSERT INTO {db_schema}.points (mmsi, lat, lon, sog, timestamp, epoch_ts)
        WITH valid_mmsi AS (
            SELECT mmsi
            FROM {TEMP_AIS_STAGE}
            WHERE lat != 91
              AND LENGTH(CAST(mmsi AS VARCHAR)) = 9
              AND CAST(mmsi AS VARCHAR)[1] BETWEEN '2' AND '7'
              AND transponder_type = 'class a'
            GROUP BY mmsi
            HAVING COUNT(*) >= 10
        ),
        dedup AS (
            SELECT DISTINCT ON (a.mmsi, a.lat, a.lon, a.timestamp)
                CAST(a.mmsi AS BIGINT) AS mmsi,
                CAST(a.lat AS DOUBLE) AS lat,
                CAST(a.lon AS DOUBLE) AS lon,
                CAST(a.sog AS DOUBLE) AS sog,
                CAST(a.timestamp AS TIMESTAMP) AS timestamp,
                EPOCH(CAST(a.timestamp AS TIMESTAMP)) AS epoch_ts
            FROM {TEMP_AIS_STAGE} a
            JOIN valid_mmsi v ON a.mmsi = v.mmsi
            WHERE a.lat != 91
            ORDER BY a.mmsi, a.timestamp, a.lat, a.lon
        ),
        unseen AS (
            SELECT d.*
            FROM dedup d
            LEFT JOIN {db_schema}.points p
                ON p.mmsi = d.mmsi
               AND p.lat = d.lat
               AND p.lon = d.lon
               AND p.timestamp = d.timestamp
            WHERE p.mmsi IS NULL
        )
        SELECT mmsi, lat, lon, sog, timestamp, epoch_ts
        FROM unseen
        ORDER BY mmsi, epoch_ts;
    """
    )

    after_count_row = conn.execute(
        f"SELECT COUNT(*) FROM {db_schema}.points"
    ).fetchone()
    after_count = after_count_row[0] if after_count_row else 0
    return int(after_count - before_count)


def _log_loaded_files(
    conn: duckdb.DuckDBPyConnection,
    db_schema: str,
    selected_files: list[tuple[str, str, date]],
):
    for file_path, file_name, file_date in selected_files:
        min_max_row = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM read_parquet(?)",
            [file_path],
        ).fetchone()
        min_ts = min_max_row[0] if min_max_row else None
        max_ts = min_max_row[1] if min_max_row else None

        conn.execute(
            f"DELETE FROM {db_schema}.points_ingestion_log WHERE file_name = ?",
            [file_name],
        )
        conn.execute(
            f"""
            INSERT INTO {db_schema}.points_ingestion_log
                (file_name, file_path, file_date, min_ts, max_ts, loaded_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
            [file_name, file_path, file_date.isoformat(), min_ts, max_ts],
        )


def create_duckdb_points(
    conn: duckdb.DuckDBPyConnection,
    db_schema: str,
    ais_data_path: str | None = None,
):
    print("Loading AIS parquet files into DuckDB points incrementally...")
    start_time = time.perf_counter()

    _ensure_points_table(conn, db_schema)
    _ensure_ingestion_log_table(conn, db_schema)

    resolved_ais_data_path = ais_data_path or get_ais_data_path()
    discovered_files = discover_ais_parquet_files(resolved_ais_data_path)
    if not discovered_files:
        print(f"No AIS parquet files found in '{resolved_ais_data_path}'.")
        return

    watermark_ts = _get_ingestion_watermark(conn, db_schema)
    watermark_date = watermark_ts.date() if watermark_ts is not None else None

    default_start, default_end = get_ais_default_period()
    selected_start, selected_end = prompt_optional_date_range(
        "Select optional AIS ingestion period",
        default_start=default_start,
        default_end=default_end,
    )

    selected_files = filter_files_by_watermark_and_period(
        discovered_files,
        watermark_date,
        selected_start,
        selected_end,
    )

    print(
        f"Discovered {len(discovered_files)} AIS parquet files. Watermark: {watermark_ts if watermark_ts else 'None'}."
    )
    if selected_start or selected_end:
        print(
            f"Applying period filter: {selected_start.isoformat() if selected_start else '*'} to {selected_end.isoformat() if selected_end else '*'}"
        )

    if not selected_files:
        print("No new parquet files matched current watermark and date filter.")
        return

    print(
        f"Selected {len(selected_files)} parquet files from {selected_files[0][2].isoformat()} to {selected_files[-1][2].isoformat()}."
    )

    print(f"Loading {len(selected_files)} parquet files into DuckDB...")
    _create_staging_table(conn, selected_files)

    print(f"Loaded {len(selected_files)} files. Inserting into points table...")
    inserted_points = _insert_incremental_points(conn, db_schema)

    _log_loaded_files(conn, db_schema, selected_files)
    conn.execute(f"DROP TABLE IF EXISTS {TEMP_AIS_STAGE};")

    print(
        f"Incremental points load completed: {inserted_points:,} new points inserted from {len(selected_files)} files in {time.perf_counter() - start_time:.2f}s."
    )
