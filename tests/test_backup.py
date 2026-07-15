import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from ambientweather2sqlite.database import (
    backup_database,
    create_database_if_not_exists,
    insert_observation,
)


class TestBackupDatabase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "source.db")
        self.dest_path = str(Path(self.temp_dir.name) / "backup.db")
        create_database_if_not_exists(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_backup_copies_all_rows(self):
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 00:00:00", "outTemp": 70.5},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 00:01:00", "outTemp": 71.0},
        )

        backup_size = backup_database(self.db_path, self.dest_path)

        self.assertGreater(backup_size, 0)
        self.assertTrue(Path(self.dest_path).exists())
        with closing(sqlite3.connect(self.dest_path)) as conn:
            rows = conn.execute(
                "SELECT ts, outTemp FROM observations ORDER BY ts",
            ).fetchall()
        self.assertEqual(
            rows,
            [("2026-01-01 00:00:00", 70.5), ("2026-01-01 00:01:00", 71.0)],
        )

    def test_backup_of_wal_database_with_open_writer(self):
        # Simulate the daemon holding an open WAL write transaction: the
        # backup must capture the committed snapshot, not the pending write.
        insert_observation(self.db_path, {"ts": "2026-01-01 00:00:00", "x": 1.0})
        with closing(sqlite3.connect(self.db_path)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute(
                "INSERT INTO observations (ts, x) VALUES ('2026-01-01 00:01:00', 2.0)",
            )  # uncommitted

            backup_size = backup_database(self.db_path, self.dest_path)

            writer.rollback()

        self.assertGreater(backup_size, 0)
        with closing(sqlite3.connect(self.dest_path)) as conn:
            rows = conn.execute(
                "SELECT ts FROM observations ORDER BY ts",
            ).fetchall()
        self.assertEqual(rows, [("2026-01-01 00:00:00",)])

    def test_failed_backup_cleans_up_partial_destination(self):
        with self.assertRaises(sqlite3.Error):
            backup_database(
                str(Path(self.temp_dir.name) / "missing.db"),
                self.dest_path,
            )

        self.assertFalse(Path(self.dest_path).exists())

    def test_backup_refuses_existing_destination(self):
        Path(self.dest_path).touch()

        with self.assertRaises(FileExistsError) as ctx:
            backup_database(self.db_path, self.dest_path)

        self.assertIn(self.dest_path, str(ctx.exception))

    def test_backup_preserves_sensor_name_mapping(self) -> None:
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 00:00:00", "soil-moisture.1": 42.0},
        )

        backup_database(self.db_path, self.dest_path)

        with closing(sqlite3.connect(self.dest_path)) as conn:
            mapping = conn.execute(
                "SELECT raw_name, column_name FROM aw2sqlite_sensor_columns",
            ).fetchall()
        self.assertEqual(mapping, [("soil-moisture.1", "soil_moisture_1")])

    @patch("ambientweather2sqlite.database.backup.os.fsync")
    def test_backup_flushes_file_and_parent_directory(
        self,
        mock_fsync: Mock,
    ) -> None:
        backup_database(self.db_path, self.dest_path)

        expected_calls = 1 if os.name == "nt" else 2
        self.assertEqual(mock_fsync.call_count, expected_calls)

    @unittest.skipIf(os.name == "nt", "Windows does not sync directories")
    @patch("ambientweather2sqlite.database.backup.os.fsync")
    def test_backup_ignores_unsupported_directory_sync(
        self,
        mock_fsync: Mock,
    ) -> None:
        mock_fsync.side_effect = [None, OSError("directory sync unsupported")]

        backup_size = backup_database(self.db_path, self.dest_path)

        self.assertGreater(backup_size, 0)
        self.assertTrue(Path(self.dest_path).exists())
        self.assertEqual(mock_fsync.call_count, 2)
