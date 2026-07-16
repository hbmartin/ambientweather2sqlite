from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.models import RediscoveryConfig
from ambientweather2sqlite.rediscovery import (
    StationRediscovery,
    _host_ip,
    find_station_by_mac,
)

_OLD_URL = "http://192.168.0.99/livedata.htm"
_NEW_URL = "http://192.168.0.150/livedata.htm"
_MAC = "aa:bb:cc:dd:ee:ff"


class TestHostIp(TestCase):
    def test_returns_ip_literal_host(self):
        self.assertEqual(_host_ip(_OLD_URL), "192.168.0.99")

    def test_returns_none_for_dns_hostname(self):
        self.assertIsNone(_host_ip("http://weatherstation.local/livedata.htm"))

    def test_returns_none_for_url_without_host(self):
        self.assertIsNone(_host_ip("not-a-url"))


@patch("ambientweather2sqlite.rediscovery.get_mac_address")
@patch("ambientweather2sqlite.rediscovery.probe_weather_station")
@patch("ambientweather2sqlite.rediscovery.scan_port80")
@patch(
    "ambientweather2sqlite.rediscovery.detect_local_subnet",
    return_value="192.168.0.0/24",
)
class TestFindStationByMac(TestCase):
    def test_matches_candidate_by_mac_among_multiple(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.return_value = ["192.168.0.150", "192.168.0.200"]
        mock_probe.side_effect = lambda ip: f"http://{ip}/livedata.htm"
        mock_mac.side_effect = {
            "192.168.0.150": _MAC,
            "192.168.0.200": "11:22:33:44:55:66",
        }.get

        self.assertEqual(find_station_by_mac(_MAC), (_NEW_URL, _MAC))

    def test_refuses_when_readable_macs_do_not_match(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.return_value = ["192.168.0.150"]
        mock_probe.side_effect = lambda ip: f"http://{ip}/livedata.htm"
        mock_mac.return_value = "11:22:33:44:55:66"

        self.assertIsNone(find_station_by_mac(_MAC))

    def test_single_candidate_fallback_without_stored_mac(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.return_value = ["192.168.0.150"]
        mock_probe.side_effect = lambda ip: f"http://{ip}/livedata.htm"
        mock_mac.return_value = None

        self.assertEqual(find_station_by_mac(None), (_NEW_URL, None))

    def test_single_candidate_fallback_when_mac_lookup_unavailable(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        # A stored MAC with no readable candidate MACs cannot disprove a
        # match, so the lone station is still accepted.
        mock_scan.return_value = ["192.168.0.150"]
        mock_probe.side_effect = lambda ip: f"http://{ip}/livedata.htm"
        mock_mac.return_value = None

        self.assertEqual(find_station_by_mac(_MAC), (_NEW_URL, None))

    def test_ambiguous_multiple_candidates_without_macs(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.return_value = ["192.168.0.150", "192.168.0.200"]
        mock_probe.side_effect = lambda ip: f"http://{ip}/livedata.htm"
        mock_mac.return_value = None

        self.assertIsNone(find_station_by_mac(None))

    def test_no_candidates_returns_none(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.return_value = ["192.168.0.150"]
        mock_probe.return_value = None

        self.assertIsNone(find_station_by_mac(_MAC))

    def test_subnet_detection_failure_returns_none(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_subnet.side_effect = OSError("no route")

        self.assertIsNone(find_station_by_mac(_MAC))
        mock_scan.assert_not_called()

    def test_ipv6_subnet_returns_none(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        mock_scan.side_effect = TypeError("Only IPv4 subnets are supported")

        self.assertIsNone(find_station_by_mac(_MAC, subnet="2001:db8::/126"))

    def test_oversized_subnet_is_refused_without_scanning(
        self,
        mock_subnet: Mock,
        mock_scan: Mock,
        mock_probe: Mock,
        mock_mac: Mock,
    ):
        self.assertIsNone(find_station_by_mac(_MAC, subnet="10.0.0.0/16"))
        mock_scan.assert_not_called()


class TestStationRediscovery(TestCase):
    def _tracker(
        self,
        config: RediscoveryConfig | None = None,
        mac_address: str | None = None,
    ) -> tuple[StationRediscovery, Mock]:
        logger = Mock()
        tracker = StationRediscovery(
            config or RediscoveryConfig(),
            _OLD_URL,
            logger,
            mac_address=mac_address,
        )
        return tracker, logger

    @patch("ambientweather2sqlite.rediscovery.find_station_by_mac")
    def test_no_scan_below_threshold(self, mock_find: Mock):
        tracker, _logger = self._tracker()

        self.assertIsNone(tracker.record_failure())

        mock_find.assert_not_called()
        self.assertEqual(tracker.consecutive_failures, 1)

    @patch("ambientweather2sqlite.rediscovery.find_station_by_mac")
    def test_disabled_never_scans(self, mock_find: Mock):
        tracker, _logger = self._tracker(RediscoveryConfig(enabled=False))

        for _ in range(5):
            self.assertIsNone(tracker.record_failure())

        mock_find.assert_not_called()

    @patch(
        "ambientweather2sqlite.rediscovery.find_station_by_mac",
        return_value=(_NEW_URL, _MAC),
    )
    def test_threshold_triggers_switch(self, mock_find: Mock):
        tracker, logger = self._tracker(mac_address=_MAC)

        self.assertIsNone(tracker.record_failure())
        self.assertEqual(tracker.record_failure(), _NEW_URL)

        mock_find.assert_called_once_with(_MAC)
        self.assertEqual(tracker.current_url, _NEW_URL)
        self.assertEqual(tracker.mac_address, _MAC)
        logger.warning.assert_called_once()
        warning_args = logger.warning.call_args.args
        self.assertIn("Weather station moved", warning_args[0])
        self.assertIn(_OLD_URL, warning_args)
        self.assertIn(_NEW_URL, warning_args)
        self.assertIn(f"MAC {_MAC}", warning_args)

    @patch(
        "ambientweather2sqlite.rediscovery.find_station_by_mac",
        return_value=(_NEW_URL, None),
    )
    def test_fallback_switch_is_labeled_and_clears_mac(self, mock_find: Mock):
        tracker, logger = self._tracker()

        tracker.record_failure()
        self.assertEqual(tracker.record_failure(), _NEW_URL)

        self.assertIsNone(tracker.mac_address)
        self.assertIn("single-station fallback", logger.warning.call_args.args)

    @patch("ambientweather2sqlite.rediscovery.find_station_by_mac", return_value=None)
    def test_failed_rescan_keeps_current_url(self, mock_find: Mock):
        tracker, logger = self._tracker()

        tracker.record_failure()
        self.assertIsNone(tracker.record_failure())

        mock_find.assert_called_once()
        self.assertEqual(tracker.current_url, _OLD_URL)
        logger.warning.assert_not_called()
        logger.info.assert_called_once()

    @patch(
        "ambientweather2sqlite.rediscovery.find_station_by_mac",
        return_value=(_OLD_URL, _MAC),
    )
    def test_same_url_result_is_not_a_switch(self, mock_find: Mock):
        tracker, logger = self._tracker(mac_address=_MAC)

        tracker.record_failure()
        self.assertIsNone(tracker.record_failure())

        self.assertEqual(tracker.current_url, _OLD_URL)
        logger.warning.assert_not_called()

    @patch("ambientweather2sqlite.rediscovery.time.monotonic")
    @patch("ambientweather2sqlite.rediscovery.find_station_by_mac", return_value=None)
    def test_cooldown_suppresses_rescans_until_expiry(
        self,
        mock_find: Mock,
        mock_monotonic: Mock,
    ):
        tracker, _logger = self._tracker()

        mock_monotonic.return_value = 1000.0
        tracker.record_failure()
        tracker.record_failure()  # threshold reached: first scan
        self.assertEqual(mock_find.call_count, 1)

        mock_monotonic.return_value = 1100.0
        tracker.record_failure()  # still cooling down
        self.assertEqual(mock_find.call_count, 1)

        mock_monotonic.return_value = 1300.0
        tracker.record_failure()  # cooldown expired: scan again
        self.assertEqual(mock_find.call_count, 2)

    @patch("ambientweather2sqlite.rediscovery.get_mac_address", return_value=_MAC)
    def test_success_resets_counter_and_captures_mac_once(self, mock_mac: Mock):
        tracker, logger = self._tracker()
        tracker.consecutive_failures = 1

        tracker.record_success()
        tracker.record_success()

        self.assertEqual(tracker.consecutive_failures, 0)
        self.assertEqual(tracker.mac_address, _MAC)
        mock_mac.assert_called_once_with("192.168.0.99")
        logger.info.assert_called_once()

    @patch("ambientweather2sqlite.rediscovery.get_mac_address")
    def test_success_skips_capture_for_dns_hostname(self, mock_mac: Mock):
        logger = Mock()
        tracker = StationRediscovery(
            RediscoveryConfig(),
            "http://weatherstation.local/livedata.htm",
            logger,
        )

        tracker.record_success()

        mock_mac.assert_not_called()
        self.assertIsNone(tracker.mac_address)

    @patch("ambientweather2sqlite.rediscovery.get_mac_address", return_value=None)
    def test_success_retries_capture_while_mac_unknown(self, mock_mac: Mock):
        tracker, logger = self._tracker()

        tracker.record_success()
        tracker.record_success()

        self.assertEqual(mock_mac.call_count, 2)
        self.assertIsNone(tracker.mac_address)
        logger.info.assert_not_called()
