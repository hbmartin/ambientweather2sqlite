import json
import stat
import tempfile
from http.client import HTTPException
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    persist_sensor_metadata,
)
from ambientweather2sqlite.metadata import (
    _write_metadata_file,
    create_metadata,
    metadata_path,
    write_metadata_from_database,
)
from ambientweather2sqlite.units_mapping import Units


class TestMetadata(TestCase):
    def test_atomic_metadata_write_preserves_sidecar_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            path = metadata_path(db_path)
            path.write_text("original", encoding="utf-8")
            path.chmod(0o640)

            _write_metadata_file(
                db_path,
                "http://127.0.0.1/livedata.htm",
                {"outTemp": "Outdoor Temperature"},
                {"outTemp": "fahrenheit"},
            )

            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)

    @patch("ambientweather2sqlite.metadata.extract_units", return_value={})
    @patch("ambientweather2sqlite.metadata.extract_labels", return_value={})
    @patch("ambientweather2sqlite.metadata.mureq.get", return_value="html")
    def test_empty_labels_preserve_existing_sidecar(
        self,
        mock_get: Mock,
        mock_extract_labels: Mock,
        mock_extract_units: Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            create_database_if_not_exists(db_path)
            path = metadata_path(db_path)
            path.write_text("original", encoding="utf-8")

            result = create_metadata(
                db_path,
                "http://127.0.0.1/livedata.htm",
            )

            self.assertEqual(result, ({}, {}))
            self.assertEqual(path.read_text(encoding="utf-8"), "original")
            mock_get.assert_called()
            mock_extract_labels.assert_called_once()
            mock_extract_units.assert_called_once()

    def test_metadata_write_failure_preserves_existing_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            path = metadata_path(db_path)
            path.write_text("original", encoding="utf-8")

            with (
                patch(
                    "ambientweather2sqlite.metadata.Path.replace",
                    side_effect=OSError("replace failed"),
                ),
                self.assertRaises(OSError),
            ):
                _write_metadata_file(
                    db_path,
                    "http://127.0.0.1/livedata.htm",
                    {"outTemp": "Outdoor Temperature"},
                    {"outTemp": "fahrenheit"},
                )

            self.assertEqual(path.read_text(encoding="utf-8"), "original")
            self.assertEqual(list(Path(temp_dir).glob(f".{path.name}.*")), [])

    def test_offline_refresh_preserves_sidecar_without_stored_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            create_database_if_not_exists(db_path)
            path = metadata_path(db_path)
            path.write_text("original", encoding="utf-8")

            result = write_metadata_from_database(
                db_path,
                "http://127.0.0.1/livedata.htm",
            )

            self.assertIsNone(result)
            self.assertEqual(path.read_text(encoding="utf-8"), "original")

    @patch("ambientweather2sqlite.metadata.mureq.get")
    def test_create_metadata_handles_network_failures(self, mock_get: Mock) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            create_database_if_not_exists(db_path)
            for error in (TimeoutError(), HTTPException("unavailable")):
                with self.subTest(error=type(error).__name__):
                    mock_get.side_effect = error
                    self.assertEqual(
                        create_metadata(
                            db_path,
                            "http://127.0.0.1/livedata.htm",
                        ),
                        ({}, {}),
                    )

    @patch("ambientweather2sqlite.metadata.extract_units")
    @patch("ambientweather2sqlite.metadata.extract_labels")
    @patch("ambientweather2sqlite.metadata.mureq.get")
    def test_create_metadata_skips_missing_units_without_crashing(
        self,
        mock_get: Mock,
        mock_extract_labels: Mock,
        mock_extract_units: Mock,
    ) -> None:
        mock_get.side_effect = ["live html", "station html"]
        mock_extract_labels.return_value = {
            "outTemp": "Outdoor Temperature",
            "outHumi": "Outdoor Humidity",
        }
        mock_extract_units.return_value = {
            Units.HUMIDITY: "%",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")

            labels, column_to_unit = create_metadata(
                database_path=db_path,
                live_data_url="http://127.0.0.1/livedata.htm",
            )

        self.assertEqual(labels["outTemp"], "Outdoor Temperature")
        self.assertEqual(labels["outHumi"], "Outdoor Humidity (%)")
        self.assertEqual(column_to_unit, {"outHumi": "percent"})

    @patch("ambientweather2sqlite.metadata.extract_units", return_value={})
    @patch(
        "ambientweather2sqlite.metadata.extract_labels",
        return_value={"newSensor": "New Sensor"},
    )
    @patch("ambientweather2sqlite.metadata.mureq.get", return_value="html")
    def test_online_refresh_keeps_previously_persisted_sensors(
        self,
        mock_get: Mock,
        mock_extract_labels: Mock,
        mock_extract_units: Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            create_database_if_not_exists(db_path)
            persist_sensor_metadata(
                db_path,
                {"oldSensor": "Old Sensor"},
                {"oldSensor": "celsius"},
            )

            create_metadata(db_path, "http://127.0.0.1/livedata.htm")

            sidecar = json.loads(
                metadata_path(db_path).read_text(encoding="utf-8"),
            )

        table_metadata = sidecar["databases"]["weather"]["tables"]["observations"]
        self.assertEqual(
            table_metadata["columns"],
            {"newSensor": "New Sensor", "oldSensor": "Old Sensor"},
        )
        self.assertEqual(table_metadata["units"], {"oldSensor": "celsius"})
        self.assertEqual(mock_get.call_count, 2)
        mock_extract_labels.assert_called_once()
        mock_extract_units.assert_called_once()

    @patch("ambientweather2sqlite.metadata.extract_units")
    @patch("ambientweather2sqlite.metadata.extract_labels")
    @patch("ambientweather2sqlite.metadata.mureq.get")
    def test_metadata_uses_physical_columns_and_regenerates_offline(
        self,
        mock_get: Mock,
        mock_extract_labels: Mock,
        mock_extract_units: Mock,
    ) -> None:
        mock_get.side_effect = ["live html", "station html"]
        mock_extract_labels.return_value = {
            "soil-moisture.1": "Soil Moisture",
        }
        mock_extract_units.return_value = {}

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "weather.db")
            create_database_if_not_exists(db_path)
            create_metadata(db_path, "http://127.0.0.1/livedata.htm")
            path = metadata_path(db_path)
            original = json.loads(path.read_text(encoding="utf-8"))
            path.unlink()

            regenerated_path = write_metadata_from_database(
                db_path,
                "http://127.0.0.1/livedata.htm",
            )
            if regenerated_path is None:
                self.fail("Expected stored metadata to regenerate the sidecar")
            regenerated = json.loads(regenerated_path.read_text(encoding="utf-8"))

        columns = original["databases"]["weather"]["tables"]["observations"]["columns"]
        self.assertEqual(columns, {"soil_moisture_1": "Soil Moisture"})
        self.assertEqual(regenerated, original)
