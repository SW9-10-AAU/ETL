import pyarrow as pa

TRAJ_CS_SCHEMA = pa.schema(
    [
        pa.field("trajectory_id", pa.int32()),
        pa.field("mmsi", pa.int64()),
        pa.field("ts", pa.timestamp("s", tz="UTC")),
        pa.field("occupation_seconds", pa.int32()),
        pa.field("cell_z21", pa.uint64()),
    ]
)

STOP_CS_SCHEMA = pa.schema(
    [
        pa.field("stop_id", pa.int32()),
        pa.field("mmsi", pa.int64()),
        pa.field("ts_start", pa.timestamp("s", tz="UTC")),
        pa.field("ts_end", pa.timestamp("s", tz="UTC")),
        pa.field("cell_z21", pa.uint64()),
    ]
)

REGION_CS_SCHEMA = pa.schema(
    [
        pa.field("region_id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("cell_z21", pa.uint64()),
    ]
)

PASSAGE_CS_SCHEMA = pa.schema(
    [
        pa.field("passage_id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("cell_z21", pa.uint64()),
    ]
)
