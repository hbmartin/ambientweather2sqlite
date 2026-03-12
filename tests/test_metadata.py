import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.metadata import create_metadata
from ambientweather2sqlite.units_mapping import Units


class TestMetadata(TestCase):
    @patch("ambientweather2sqlite.metadata.extract_units")
    @patch("ambientweather2sqlite.metadata.extract_labels")
    @patch("ambientweather2sqlite.metadata.mureq.get")
    def test_create_metadata_skips_missing_units_without_crashing(
        self,
        mock_get,
        mock_extract_labels,
        mock_extract_units,
    ):
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
