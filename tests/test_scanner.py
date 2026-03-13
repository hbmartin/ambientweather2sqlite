from unittest import TestCase
from unittest.mock import MagicMock, patch

from ambientweather2sqlite.scanner import (
    detect_local_subnet,
    probe_weather_station,
    scan_for_stations,
    scan_port80,
)


class TestDetectLocalSubnet(TestCase):
    def test_returns_slash_24_subnet_when_prefix_unknown(self):
        with (
            patch(
                "ambientweather2sqlite.scanner._detect_prefix_length",
                return_value=None,
            ),
            patch(
                "ambientweather2sqlite.scanner._detect_local_ip",
                return_value="192.168.1.42",
            ),
        ):
            self.assertEqual(detect_local_subnet(), "192.168.1.0/24")

    def test_uses_detected_prefix_length(self):
        with (
            patch(
                "ambientweather2sqlite.scanner._detect_prefix_length",
                return_value=20,
            ),
            patch(
                "ambientweather2sqlite.scanner._detect_local_ip",
                return_value="192.168.16.42",
            ),
        ):
            self.assertEqual(detect_local_subnet(), "192.168.16.0/20")


class TestScanPort80(TestCase):
    @patch("ambientweather2sqlite.scanner.socket.socket")
    def test_finds_open_hosts(self, mock_socket_cls):
        mock_sock = MagicMock()
        mock_socket_cls.return_value.__enter__ = lambda _s: mock_sock
        mock_socket_cls.return_value.__exit__ = MagicMock(return_value=False)

        def connect_ex(addr) -> int:
            return 0 if addr[0] == "10.0.0.1" else 1

        mock_sock.connect_ex.side_effect = connect_ex

        result = scan_port80("10.0.0.0/30", timeout=0.1, workers=2)

        self.assertIn("10.0.0.1", result)

    @patch("ambientweather2sqlite.scanner.socket.socket")
    def test_returns_empty_when_none_open(self, mock_socket_cls):
        mock_sock = MagicMock()
        mock_socket_cls.return_value.__enter__ = lambda _s: mock_sock
        mock_socket_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_sock.connect_ex.return_value = 1

        result = scan_port80("10.0.0.0/30", timeout=0.1, workers=2)

        self.assertEqual(result, [])

    def test_rejects_ipv6_subnets(self):
        with self.assertRaisesRegex(TypeError, "Only IPv4 subnets are supported"):
            scan_port80("2001:db8::/126")


class TestProbeWeatherStation(TestCase):
    @patch("ambientweather2sqlite.scanner.extract_values")
    @patch("ambientweather2sqlite.scanner.mureq.get")
    def test_returns_url_when_station_found(self, mock_get, mock_extract):
        mock_get.return_value = "<html>...</html>"
        mock_extract.return_value = {"outTemp": 72.0}

        result = probe_weather_station("192.168.1.10")

        self.assertEqual(result, "http://192.168.1.10/livedata.htm")

    @patch("ambientweather2sqlite.scanner.extract_values")
    @patch("ambientweather2sqlite.scanner.mureq.get")
    def test_returns_none_when_no_data(self, mock_get, mock_extract):
        mock_get.return_value = "<html></html>"
        mock_extract.return_value = {}

        result = probe_weather_station("192.168.1.10")

        self.assertIsNone(result)

    @patch("ambientweather2sqlite.scanner.mureq.get")
    def test_returns_none_on_timeout(self, mock_get):
        mock_get.side_effect = TimeoutError

        result = probe_weather_station("192.168.1.10")

        self.assertIsNone(result)


class TestScanForStations(TestCase):
    @patch("ambientweather2sqlite.scanner.probe_weather_station")
    @patch("ambientweather2sqlite.scanner.scan_port80")
    def test_returns_found_stations(self, mock_scan, mock_probe):
        mock_scan.return_value = ["192.168.1.1", "192.168.1.2"]
        mock_probe.side_effect = lambda ip: (
            f"http://{ip}/livedata.htm" if ip == "192.168.1.1" else None
        )

        result = scan_for_stations("192.168.1.0/24")

        self.assertEqual(result, ["http://192.168.1.1/livedata.htm"])

    @patch("ambientweather2sqlite.scanner.scan_port80")
    def test_returns_empty_when_no_hosts_open(self, mock_scan):
        mock_scan.return_value = []

        result = scan_for_stations("192.168.1.0/24")

        self.assertEqual(result, [])

    @patch("ambientweather2sqlite.scanner.detect_local_subnet")
    def test_returns_empty_when_subnet_autodetect_fails(self, mock_detect):
        mock_detect.side_effect = OSError("no route")

        result = scan_for_stations()

        self.assertEqual(result, [])
