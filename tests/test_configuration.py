import tempfile
from pathlib import Path
from unittest import TestCase

from ambientweather2sqlite.configuration import load_config


class TestConfiguration(TestCase):
    def test_load_config_rejects_boolean_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "aw2sqlite.toml"
            config_path.write_text(
                'live_data_url = "http://127.0.0.1/livedata.htm"\n'
                'database_path = "/tmp/aw2sqlite.db"\n'
                "port = true\n",
                encoding="utf-8",
            )

            with self.assertRaises(TypeError):
                load_config(config_path)
