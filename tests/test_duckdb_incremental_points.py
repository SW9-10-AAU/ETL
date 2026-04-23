import os
import sys
import tempfile
import unittest
from datetime import date

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from db_setup.duckdb.create_duckdb_points import (  # noqa: E402
    discover_ais_parquet_files,
    filter_files_by_watermark_and_period,
    parse_ais_file_date,
)


class TestDuckdbIncrementalPointsHelpers(unittest.TestCase):

    def test_parse_ais_file_date_valid(self):
        self.assertEqual(parse_ais_file_date("aisdk-2025-12-01.pq"), date(2025, 12, 1))

    def test_parse_ais_file_date_invalid(self):
        self.assertIsNone(parse_ais_file_date("aisdk-2025-12-01.parquet"))
        self.assertIsNone(parse_ais_file_date("other-2025-12-01.pq"))

    def test_discover_ais_parquet_files_filters_and_sorts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            open(
                os.path.join(tmp_dir, "aisdk-2025-12-03.pq"), "w", encoding="utf-8"
            ).close()
            open(
                os.path.join(tmp_dir, "aisdk-2025-12-01.pq"), "w", encoding="utf-8"
            ).close()
            open(os.path.join(tmp_dir, "ignore-me.pq"), "w", encoding="utf-8").close()

            discovered = discover_ais_parquet_files(tmp_dir)
            self.assertEqual(len(discovered), 2)
            self.assertEqual(discovered[0][1], "aisdk-2025-12-01.pq")
            self.assertEqual(discovered[1][1], "aisdk-2025-12-03.pq")

    def test_filter_files_by_watermark_and_period(self):
        files = [
            ("a", "aisdk-2025-12-01.pq", date(2025, 12, 1)),
            ("b", "aisdk-2025-12-02.pq", date(2025, 12, 2)),
            ("c", "aisdk-2025-12-03.pq", date(2025, 12, 3)),
        ]

        filtered = filter_files_by_watermark_and_period(
            files,
            watermark_date=date(2025, 12, 1),
            start_date=date(2025, 12, 2),
            end_date=date(2025, 12, 3),
        )

        self.assertEqual(
            [name for _, name, _ in filtered],
            ["aisdk-2025-12-02.pq", "aisdk-2025-12-03.pq"],
        )


if __name__ == "__main__":
    unittest.main()
