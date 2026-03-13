import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ambientweather2sqlite.launchd import generate_plist, install_launchd


class TestLaunchd(TestCase):
    def test_generate_plist_contains_config_path(self):
        config_path = Path("/tmp/test-config.toml")

        plist = generate_plist(config_path)

        self.assertIn(str(config_path.resolve()), plist)
        self.assertIn("com.ambientweather2sqlite", plist)
        self.assertIn("<key>KeepAlive</key>", plist)
        self.assertIn("<key>RunAtLoad</key>", plist)
        self.assertIn("serve", plist)

    def test_generate_plist_is_valid_xml(self):
        import xml.etree.ElementTree as ET

        config_path = Path("/tmp/test-config.toml")

        plist = generate_plist(config_path)

        # Should parse as valid XML
        ET.fromstring(plist)

    def test_generate_plist_falls_back_to_python_module(self):
        with (
            patch("ambientweather2sqlite.launchd.shutil.which", return_value=None),
            patch("ambientweather2sqlite.launchd.sys.executable", "/usr/bin/python3"),
        ):
            plist = generate_plist(Path("/tmp/test-config.toml"))

        self.assertIn("/usr/bin/python3", plist)
        self.assertIn("<string>-m</string>", plist)
        self.assertIn("<string>ambientweather2sqlite</string>", plist)

    @patch("ambientweather2sqlite.launchd.Path.home")
    def test_install_launchd_writes_plist_file(self, mock_home):
        with tempfile.TemporaryDirectory() as tmp:
            mock_home.return_value = Path(tmp)
            config_path = Path(tmp) / "test-config.toml"
            config_path.write_text("", encoding="utf-8")

            result = install_launchd(config_path)

            self.assertTrue(result.exists())
            content = result.read_text(encoding="utf-8")
            self.assertIn("com.ambientweather2sqlite", content)
