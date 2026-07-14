import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    inspect_database_migration,
    migrate_database,
)
from ambientweather2sqlite.database.identifiers import _column_name
from ambientweather2sqlite.database.writes import _insert_dict_row
from ambientweather2sqlite.exceptions import (
    InvalidTimestampError,
    MigrationRequiredError,
    NullTimestampMigrationError,
    UnsupportedSchemaVersionError,
)


class TestDatabaseUtilityFunctions(unittest.TestCase):
    def setUp(self) -> None:
        """Set up test fixtures with temporary database."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        Path(self.db_path).unlink(missing_ok=True)

    def tearDown(self) -> None:
        """Clean up after tests."""
        Path(self.db_path).unlink(missing_ok=True)

    def test_column_name_function(self) -> None:
        """Test _column_name function."""
        test_cases = [
            ("simple", "simple"),
            ("with spaces", "with_spaces"),
            ("with-dashes", "with_dashes"),
            ("with.dots", "with_dots"),
            ("with@symbols", "with_symbols"),
            ("MixedCase123", "MixedCase123"),
            ("", ""),
            ("123numbers", "123numbers"),
        ]

        for input_str, expected in test_cases:
            with self.subTest(input_str=input_str):
                result = _column_name(input_str)
                self.assertEqual(result, expected)

    def test_create_database_if_not_exists_new_db(self) -> None:
        """Test creating a new database."""
        was_created = create_database_if_not_exists(self.db_path)
        self.assertTrue(was_created)
        self.assertTrue(Path(self.db_path).exists())

        # Verify observations table was created
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='observations'",
            )
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], "observations")

    def test_create_database_if_not_exists_existing_db(self) -> None:
        """Test with existing database."""
        # Create database first
        create_database_if_not_exists(self.db_path)

        # Try to create again
        was_created = create_database_if_not_exists(self.db_path)
        self.assertFalse(was_created)

    def test_create_database_if_not_exists_initializes_empty_file(self) -> None:
        """Test with an existing but empty database file."""
        Path(self.db_path).touch()

        was_created = create_database_if_not_exists(self.db_path)

        self.assertTrue(was_created)
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='observations'",
            )
            result = cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], "observations")

    def test_create_database_refuses_nonempty_foreign_schema(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("CREATE TABLE sqliteX_foreign (id INTEGER)")
            conn.commit()

        with self.assertRaises(MigrationRequiredError):
            create_database_if_not_exists(self.db_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            objects = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'",
                ).fetchall()
            }
            version = conn.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(objects, {"sqliteX_foreign"})
        self.assertEqual(version, 0)

    @patch(
        "ambientweather2sqlite.database.schema._create_sensor_mapping_table",
        side_effect=sqlite3.OperationalError("schema failure"),
    )
    def test_create_database_rolls_back_partial_schema(self, mock_create: Mock) -> None:
        with self.assertRaises(sqlite3.OperationalError):
            create_database_if_not_exists(self.db_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            objects = conn.execute(
                "SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
            ).fetchall()
            version = conn.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(objects, [])
        self.assertEqual(version, 0)
        mock_create.assert_called_once()

    def test_create_database_refuses_newer_empty_schema(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("PRAGMA user_version = 2")

        with self.assertRaises(UnsupportedSchemaVersionError):
            create_database_if_not_exists(self.db_path)

    def test_observations_table_structure(self) -> None:
        """Test that observations table has correct structure."""
        create_database_if_not_exists(self.db_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            # Should have at least the ts column
            column_names = {col[1] for col in columns}
            self.assertIn("ts", column_names)
            ts_column = next(column for column in columns if column[1] == "ts")
            self.assertEqual(ts_column[3], 1)

    def test_sqlite_timestamp_default_uses_six_fractional_digits(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("INSERT INTO observations DEFAULT VALUES")
            timestamp = conn.execute("SELECT ts FROM observations").fetchone()[0]

        date_time, fractional = str(timestamp).split(".", maxsplit=1)
        self.assertEqual(len(date_time), 19)
        self.assertEqual(len(fractional), 6)
        self.assertTrue(fractional.isdigit())

    def test_insert_observation_basic(self) -> None:
        """Test basic observation insertion."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "outTemp": 75.5,
            "outHumi": 60.0,
            "windSpeed": 10.2,
        }

        insert_observation(self.db_path, test_data)

        # Verify data was inserted
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1)

    def test_insert_observation_creates_columns(self) -> None:
        """Test that insert_observation creates columns as needed."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "newColumn": 42.0,
            "anotherColumn": 33.3,
        }

        insert_observation(self.db_path, test_data)

        # Verify columns were created
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("newColumn", column_names)
            self.assertIn("anotherColumn", column_names)

    def test_insert_multiple_observations(self) -> None:
        """Test inserting multiple observations with distinct timestamps."""
        create_database_if_not_exists(self.db_path)

        test_data_list = [
            {"ts": "2026-01-01 12:00:00", "outTemp": 75.0, "outHumi": 60.0},
            {"ts": "2026-01-01 12:01:00", "outTemp": 77.0, "outHumi": 58.0},
            {"ts": "2026-01-01 12:02:00", "outTemp": 79.0, "outHumi": 55.0},
        ]

        for data in test_data_list:
            insert_observation(self.db_path, data)

        # Verify all data was inserted
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            self.assertEqual(count, len(test_data_list))

    def test_insert_observation_without_ts_uses_unique_precise_timestamps(self) -> None:
        create_database_if_not_exists(self.db_path)

        with patch(
            "ambientweather2sqlite.database.writes._current_observation_timestamp",
            side_effect=[
                "2026-01-01 12:00:00.000001",
                "2026-01-01 12:00:00.000002",
            ],
        ):
            insert_observation(self.db_path, {"outTemp": 75.0})
            insert_observation(self.db_path, {"outTemp": 76.0})

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ts FROM observations ORDER BY ts")
            timestamps = [row[0] for row in cursor.fetchall()]

        self.assertEqual(
            timestamps,
            [
                "2026-01-01 12:00:00.000001",
                "2026-01-01 12:00:00.000002",
            ],
        )

    def test_insert_observation_normalizes_invalid_ts_values(self) -> None:
        create_database_if_not_exists(self.db_path)

        with patch(
            "ambientweather2sqlite.database.writes._current_observation_timestamp",
            side_effect=[
                "2026-01-01 12:00:00.000010",
                "2026-01-01 12:00:00.000011",
            ],
        ):
            insert_observation(self.db_path, {"ts": None, "outTemp": 75.0})
            insert_observation(self.db_path, {"ts": "   ", "outTemp": 76.0})

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ts FROM observations ORDER BY ts")
            timestamps = [row[0] for row in cursor.fetchall()]

        self.assertEqual(
            timestamps,
            [
                "2026-01-01 12:00:00.000010",
                "2026-01-01 12:00:00.000011",
            ],
        )

    def test_legacy_database_requires_explicit_backed_up_migration(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE observations (ts TIMESTAMP, outTemp REAL)")
            cursor.execute("CREATE INDEX idx_out_temp ON observations(outTemp)")
            cursor.executemany(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                [
                    ("2026-01-01 12:00:00", 70.0),
                    ("2026-01-01 12:00:00", 72.0),
                ],
            )
            conn.commit()

        with self.assertRaises(MigrationRequiredError):
            create_database_if_not_exists(self.db_path)

        plan = inspect_database_migration(self.db_path)
        self.assertTrue(plan.required)
        self.assertFalse(plan.timestamp_not_null)
        self.assertEqual(plan.duplicate_rows_to_remove, 1)

        backup_path = f"{self.db_path}.pre-migration"
        result = migrate_database(self.db_path, backup_path=backup_path)

        self.assertEqual(result.removed_duplicate_rows, 1)
        self.assertEqual(result.backup_path, backup_path)
        self.assertTrue(Path(backup_path).exists())
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM observations")
            count = cursor.fetchone()[0]
            cursor.execute("SELECT outTemp FROM observations")
            remaining_temp = cursor.fetchone()[0]
            cursor.execute("PRAGMA index_list(observations)")
            indexes = cursor.fetchall()
            cursor.execute(
                "SELECT COUNT(*) FROM aw2sqlite_migration_conflicts",
            )
            audit_count = cursor.fetchone()[0]
            cursor.execute(
                "SELECT raw_name, column_name FROM aw2sqlite_sensor_columns",
            )
            sensor_mappings = cursor.fetchall()
            cursor.execute("PRAGMA table_info(observations)")
            timestamp_column = next(
                column for column in cursor.fetchall() if column[1] == "ts"
            )

        self.assertEqual(count, 1)
        self.assertEqual(remaining_temp, 72.0)
        self.assertTrue(any(index[2] for index in indexes))
        self.assertIn("idx_out_temp", {index[1] for index in indexes})
        self.assertEqual(audit_count, 2)
        self.assertEqual(sensor_mappings, [("outTemp", "outTemp")])
        self.assertEqual(timestamp_column[3], 1)
        migrated_plan = inspect_database_migration(self.db_path)
        self.assertFalse(migrated_plan.required)
        self.assertTrue(migrated_plan.timestamp_not_null)
        Path(backup_path).unlink()

    def test_migration_refuses_null_timestamps_after_creating_backup(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("CREATE TABLE observations (ts TIMESTAMP, outTemp REAL)")
            conn.execute("INSERT INTO observations (ts, outTemp) VALUES (NULL, 70.0)")
            conn.commit()

        backup_path = f"{self.db_path}.null-timestamp-backup"
        with self.assertRaises(NullTimestampMigrationError):
            migrate_database(self.db_path, backup_path=backup_path)

        self.assertTrue(Path(backup_path).exists())
        with closing(sqlite3.connect(self.db_path)) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
                1,
            )
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 0)
        Path(backup_path).unlink()

    def test_migration_does_not_map_case_variant_timestamp_as_sensor(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute('CREATE TABLE observations ("TS" TIMESTAMP, outTemp REAL)')
            conn.execute(
                'INSERT INTO observations ("TS", outTemp) VALUES (?, ?)',
                ("2026-01-01 12:00:00", 70.0),
            )
            conn.commit()

        backup_path = f"{self.db_path}.uppercase-ts-backup"
        migrate_database(self.db_path, backup_path=backup_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            mappings = conn.execute(
                "SELECT raw_name, column_name FROM aw2sqlite_sensor_columns",
            ).fetchall()
            timestamp_column = next(
                row
                for row in conn.execute("PRAGMA table_info(observations)").fetchall()
                if str(row[1]).casefold() == "ts"
            )

        self.assertEqual(mappings, [("outTemp", "outTemp")])
        self.assertEqual(timestamp_column[3], 1)
        self.assertFalse(inspect_database_migration(self.db_path).required)
        Path(backup_path).unlink()

    @patch("ambientweather2sqlite.database.migrations.backup_database")
    def test_migration_acquires_write_lock_before_backup(
        self,
        mock_backup: Mock,
    ) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("CREATE TABLE observations (ts TIMESTAMP, outTemp REAL)")
            conn.execute(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                ("2026-01-01 12:00:00", 70.0),
            )
            conn.commit()

        def assert_source_is_locked(source_path: str, _backup_path: str) -> int:
            with closing(sqlite3.connect(source_path, timeout=0.01)) as concurrent:
                with self.assertRaises(sqlite3.OperationalError):
                    concurrent.execute(
                        "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                        ("2026-01-01 13:00:00", 71.0),
                    )
            return 0

        mock_backup.side_effect = assert_source_is_locked
        migrate_database(self.db_path, backup_path=f"{self.db_path}.locked-backup")

        mock_backup.assert_called_once()

    def test_migration_refuses_newer_schema_versions(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("PRAGMA user_version = 2")

        with self.assertRaises(UnsupportedSchemaVersionError):
            inspect_database_migration(self.db_path)
        with self.assertRaises(UnsupportedSchemaVersionError):
            migrate_database(self.db_path)

    def test_migration_replaces_partial_timestamp_index(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("DROP INDEX idx_observations_ts")
            conn.execute(
                "CREATE UNIQUE INDEX idx_observations_ts ON observations(ts) "
                "WHERE ts IS NOT NULL",
            )
            conn.commit()

        plan = inspect_database_migration(self.db_path)
        self.assertFalse(plan.unique_timestamp_index)
        backup_path = f"{self.db_path}.partial-index-backup"
        migrate_database(self.db_path, backup_path=backup_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            timestamp_index = next(
                row
                for row in conn.execute("PRAGMA index_list(observations)").fetchall()
                if row[1] == "idx_observations_ts"
            )
        self.assertEqual(timestamp_index[4], 0)
        self.assertFalse(inspect_database_migration(self.db_path).required)
        Path(backup_path).unlink()

    def test_migration_repairs_malformed_sensor_mapping_table(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("DROP TABLE aw2sqlite_sensor_columns")
            conn.execute(
                "CREATE TABLE aw2sqlite_sensor_columns ("
                "raw_name TEXT PRIMARY KEY, "
                "column_name TEXT NOT NULL COLLATE NOCASE UNIQUE, "
                "label TEXT NOT NULL, unit TEXT NOT NULL)",
            )
            conn.execute(
                "INSERT INTO aw2sqlite_sensor_columns VALUES (?, ?, ?, ?)",
                ("legacy", "legacy", "Legacy Sensor", "percent"),
            )
            conn.commit()

        plan = inspect_database_migration(self.db_path)
        self.assertFalse(plan.sensor_mapping_table)
        backup_path = f"{self.db_path}.mapping-repair-backup"
        migrate_database(self.db_path, backup_path=backup_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            mapping = conn.execute(
                "SELECT raw_name, column_name, label, unit "
                "FROM aw2sqlite_sensor_columns",
            ).fetchone()
        self.assertEqual(mapping, ("legacy", "legacy", "Legacy Sensor", "percent"))
        self.assertFalse(inspect_database_migration(self.db_path).required)
        Path(backup_path).unlink()

    def test_migration_deduplicates_malformed_sensor_mappings(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("DROP TABLE aw2sqlite_sensor_columns")
            conn.execute(
                "CREATE TABLE aw2sqlite_sensor_columns ("
                "raw_name TEXT, column_name TEXT, label TEXT, unit TEXT)",
            )
            conn.executemany(
                "INSERT INTO aw2sqlite_sensor_columns VALUES (?, ?, ?, ?)",
                [
                    ("duplicate", "first", "First", "percent"),
                    ("duplicate", "second", "Second", "celsius"),
                    ("column-owner", "shared", "Owner", None),
                    ("column-loser", "SHARED", "Loser", None),
                ],
            )
            conn.commit()

        backup_path = f"{self.db_path}.mapping-deduplication-backup"
        migrate_database(self.db_path, backup_path=backup_path)

        with closing(sqlite3.connect(self.db_path)) as conn:
            mappings = conn.execute(
                "SELECT raw_name, column_name, label, unit "
                "FROM aw2sqlite_sensor_columns ORDER BY rowid",
            ).fetchall()
        self.assertEqual(
            mappings,
            [
                ("duplicate", "first", "First", "percent"),
                ("column-owner", "shared", "Owner", None),
            ],
        )
        self.assertFalse(inspect_database_migration(self.db_path).required)
        Path(backup_path).unlink()

    def test_migration_preserves_observation_triggers(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("CREATE TABLE observations (ts TIMESTAMP, outTemp REAL)")
            conn.execute("CREATE TABLE observation_audit (temperature REAL)")
            conn.execute(
                "CREATE TRIGGER audit_temperature AFTER INSERT ON observations "
                "BEGIN INSERT INTO observation_audit VALUES (NEW.outTemp); END",
            )
            conn.execute(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                ("2026-01-01 12:00:00", 70.0),
            )
            conn.execute("DELETE FROM observation_audit")
            conn.commit()

        backup_path = f"{self.db_path}.trigger-backup"
        migrate_database(self.db_path, backup_path=backup_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO observations (ts, outTemp) VALUES (?, ?)",
                ("2026-01-01 13:00:00", 71.0),
            )
            audit_rows = conn.execute(
                "SELECT temperature FROM observation_audit",
            ).fetchall()

        self.assertEqual(audit_rows, [(71.0,)])
        Path(backup_path).unlink()

    def test_insert_dict_row_returns_none_when_duplicate_is_ignored(self) -> None:
        create_database_if_not_exists(self.db_path)
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("ALTER TABLE observations ADD COLUMN outTemp REAL")
            first_rowid = _insert_dict_row(
                conn,
                "observations",
                {"ts": "2026-01-01 12:00:00", "outTemp": 70.0},
            )
            second_rowid = _insert_dict_row(
                conn,
                "observations",
                {"ts": "2026-01-01 12:00:00", "outTemp": 72.0},
            )

        self.assertIsNotNone(first_rowid)
        self.assertIsNone(second_rowid)

    def test_insert_observation_with_special_column_names(self) -> None:
        """Test insertion with column names requiring sanitization."""
        create_database_if_not_exists(self.db_path)

        test_data = {
            "column with spaces": 1.0,
            "column-with-dashes": 2.0,
            "column.with.dots": 3.0,
        }

        insert_observation(self.db_path, test_data)

        # Verify data was inserted and columns were sanitized
        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(observations)")
            columns = cursor.fetchall()

            column_names = {col[1] for col in columns}
            self.assertIn("column_with_spaces", column_names)
            self.assertIn("column_with_dashes", column_names)
            self.assertIn("column_with_dots", column_names)

    def test_repeated_insert_with_special_column_names_reuses_sanitized_columns(
        self,
    ) -> None:
        create_database_if_not_exists(self.db_path)

        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 12:00:00", "sensor-name": 1.0},
        )
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01 12:01:00", "sensor-name": 2.0},
        )

        with closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT sensor_name FROM observations ORDER BY ts")
            values = [row[0] for row in cursor.fetchall()]

        self.assertEqual(values, [1.0, 2.0])

    def test_unsafe_and_colliding_sensor_names_are_lossless(self) -> None:
        create_database_if_not_exists(self.db_path)

        added = insert_observation(
            self.db_path,
            {
                "ts": "2026-01-01T01:00:00+01:00",
                "select": 1.0,
                "123sensor": 2.0,
                "sensor-name": 3.0,
                "sensor.name": 4.0,
                "TS": 5.0,
                "": 6.0,
            },
        )

        self.assertEqual(
            added,
            ["select", "123sensor", "sensor-name", "sensor.name", "TS", ""],
        )
        with closing(sqlite3.connect(self.db_path)) as conn:
            mappings = dict(
                conn.execute(
                    "SELECT raw_name, column_name FROM aw2sqlite_sensor_columns",
                ).fetchall(),
            )
            physical_columns = [mappings[name] for name in added]
            selected = ", ".join(f'"{column}"' for column in physical_columns)
            query = f"SELECT ts, {selected} FROM observations"  # noqa: S608
            row = conn.execute(query).fetchone()

        self.assertEqual(len(set(physical_columns)), len(physical_columns))
        self.assertEqual(row, ("2026-01-01 00:00:00", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0))

    def test_duplicate_insert_rolls_back_new_sensor_schema(self) -> None:
        create_database_if_not_exists(self.db_path)
        timestamp = "2026-01-01 12:00:00"
        insert_observation(self.db_path, {"ts": timestamp, "outTemp": 70.0})

        added = insert_observation(
            self.db_path,
            {"ts": timestamp, "new-sensor": 1.0},
        )

        self.assertEqual(added, [])
        with closing(sqlite3.connect(self.db_path)) as conn:
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(observations)")
            }
            mapping = conn.execute(
                "SELECT 1 FROM aw2sqlite_sensor_columns WHERE raw_name = ?",
                ("new-sensor",),
            ).fetchone()
        self.assertNotIn("new_sensor", columns)
        self.assertIsNone(mapping)

    def test_caller_timestamp_is_normalized_and_invalid_value_is_rejected(
        self,
    ) -> None:
        create_database_if_not_exists(self.db_path)
        insert_observation(
            self.db_path,
            {"ts": "2026-01-01T01:30:00+01:00", "outTemp": 70.0},
        )

        with self.assertRaises(InvalidTimestampError):
            insert_observation(
                self.db_path,
                {"ts": "not-a-timestamp", "outTemp": 71.0},
            )

        with closing(sqlite3.connect(self.db_path)) as conn:
            timestamps = conn.execute("SELECT ts FROM observations").fetchall()
        self.assertEqual(timestamps, [("2026-01-01 00:30:00",)])


if __name__ == "__main__":
    unittest.main()
