import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.systemd import generate_unit, install_systemd


class TestSystemd(TestCase):
    def test_generate_unit_contains_config_path(self):
        config_path = Path("/tmp/test-config.toml")

        unit = generate_unit(config_path)

        self.assertIn(str(config_path.resolve()), unit)
        self.assertIn("[Unit]", unit)
        self.assertIn("[Service]", unit)
        self.assertIn("[Install]", unit)
        self.assertIn("Restart=always", unit)
        self.assertIn("serve", unit)

    def test_generate_unit_falls_back_to_python_module(self):
        with (
            patch("ambientweather2sqlite.systemd.shutil.which", return_value=None),
            patch("ambientweather2sqlite.systemd.sys.executable", "/usr/bin/python3"),
        ):
            unit = generate_unit(Path("/tmp/test-config.toml"))

        self.assertIn("ExecStart=/usr/bin/python3 -m ambientweather2sqlite serve", unit)

    def test_generate_unit_quotes_paths_with_spaces(self):
        config_path = Path("/tmp/my configs/test.toml")
        with (
            patch("ambientweather2sqlite.systemd.shutil.which", return_value=None),
            patch("ambientweather2sqlite.systemd.sys.executable", "/usr/bin/python3"),
        ):
            unit = generate_unit(config_path)

        self.assertIn(f"'{config_path.resolve()}'", unit)

    def test_generate_unit_escapes_percent_specifiers(self):
        config_path = Path("/tmp/config%h.toml")
        with (
            patch("ambientweather2sqlite.systemd.shutil.which", return_value=None),
            patch("ambientweather2sqlite.systemd.sys.executable", "/tmp/%h/python3"),
        ):
            unit = generate_unit(config_path)

        self.assertIn("/tmp/%%h/python3", unit)
        self.assertIn(str(config_path.resolve()).replace("%", "%%"), unit)

    @patch("ambientweather2sqlite.systemd.Path.home")
    def test_install_systemd_writes_unit_file(self, mock_home):
        with tempfile.TemporaryDirectory() as tmp:
            mock_home.return_value = Path(tmp)
            config_path = Path(tmp) / "test-config.toml"
            config_path.write_text("", encoding="utf-8")

            result = install_systemd(config_path)

            self.assertTrue(result.exists())
            self.assertEqual(result.name, "ambientweather2sqlite.service")
            content = result.read_text(encoding="utf-8")
            self.assertIn("ExecStart=", content)
