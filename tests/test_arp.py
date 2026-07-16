import subprocess
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.arp import (
    _mac_from_output,
    _normalize_mac,
    get_mac_address,
)

_MACOS_ARP_OUTPUT = "? (192.168.0.99) at 0:11:22:3:44:55 on en0 ifscope [ethernet]\n"
_LINUX_IP_NEIGH_OUTPUT = "192.168.0.99 dev eth0 lladdr aa:bb:cc:dd:ee:ff REACHABLE\n"
_WINDOWS_ARP_OUTPUT = (
    "Interface: 192.168.0.10 --- 0x4\n"
    "  Internet Address      Physical Address      Type\n"
    "  192.168.0.99          aa-bb-cc-dd-ee-ff     dynamic\n"
)
_PROC_NET_ARP_CONTENT = (
    "IP address       HW type     Flags       HW address            Mask     Device\n"
    "192.168.0.99     0x1         0x2         aa:bb:cc:dd:ee:ff     *        eth0\n"
)


class TestNormalizeMac(TestCase):
    def test_lowercases_and_zero_pads_octets(self):
        self.assertEqual(_normalize_mac("0:11:22:3:44:55"), "00:11:22:03:44:55")
        self.assertEqual(_normalize_mac("AA:BB:CC:DD:EE:FF"), "aa:bb:cc:dd:ee:ff")

    def test_accepts_dash_separated_octets(self):
        self.assertEqual(_normalize_mac("aa-bb-cc-dd-ee-ff"), "aa:bb:cc:dd:ee:ff")

    def test_rejects_wrong_octet_count(self):
        self.assertIsNone(_normalize_mac("aa:bb:cc:dd:ee"))

    def test_rejects_non_hex_octets(self):
        self.assertIsNone(_normalize_mac("zz:bb:cc:dd:ee:ff"))


class TestMacFromOutput(TestCase):
    def test_rejects_all_zeros_mac(self):
        self.assertIsNone(
            _mac_from_output("192.168.0.99 at 00:00:00:00:00:00 on eth0"),
        )

    def test_rejects_broadcast_mac(self):
        self.assertIsNone(
            _mac_from_output("192.168.0.99 at ff:ff:ff:ff:ff:ff on eth0"),
        )

    def test_ignores_incomplete_entries(self):
        self.assertIsNone(_mac_from_output("? (192.168.0.99) at (incomplete) on en0"))

    def test_ignores_failed_neighbor_entries(self):
        self.assertIsNone(_mac_from_output("192.168.0.99 dev eth0 FAILED"))


class TestGetMacAddress(TestCase):
    def test_reads_linux_ip_neigh_output(self):
        with patch(
            "ambientweather2sqlite.arp.subprocess.run",
            return_value=Mock(stdout=_LINUX_IP_NEIGH_OUTPUT),
        ) as mock_run:
            mac = get_mac_address("192.168.0.99")

        self.assertEqual(mac, "aa:bb:cc:dd:ee:ff")
        mock_run.assert_called_once()
        self.assertEqual(
            mock_run.call_args.args[0],
            ["ip", "neigh", "show", "192.168.0.99"],
        )

    def test_normalizes_unpadded_macos_arp_output(self):
        # macOS has no `ip` binary; `arp -n` prints unpadded octets.
        with patch(
            "ambientweather2sqlite.arp.subprocess.run",
            side_effect=[FileNotFoundError, Mock(stdout=_MACOS_ARP_OUTPUT)],
        ):
            mac = get_mac_address("192.168.0.99")

        self.assertEqual(mac, "00:11:22:03:44:55")

    def test_reads_windows_dash_separated_output(self):
        with patch(
            "ambientweather2sqlite.arp.subprocess.run",
            side_effect=[
                FileNotFoundError,
                FileNotFoundError,
                Mock(stdout=_WINDOWS_ARP_OUTPUT),
            ],
        ):
            mac = get_mac_address("192.168.0.99")

        self.assertEqual(mac, "aa:bb:cc:dd:ee:ff")

    def test_falls_back_to_proc_net_arp_when_commands_fail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proc_net_arp = Path(temp_dir) / "arp"
            proc_net_arp.write_text(_PROC_NET_ARP_CONTENT, encoding="utf-8")
            with (
                patch(
                    "ambientweather2sqlite.arp.subprocess.run",
                    side_effect=[
                        FileNotFoundError,
                        subprocess.CalledProcessError(1, "arp"),
                        subprocess.TimeoutExpired("arp", 1),
                    ],
                ),
                patch("ambientweather2sqlite.arp._PROC_NET_ARP", proc_net_arp),
            ):
                mac = get_mac_address("192.168.0.99")

        self.assertEqual(mac, "aa:bb:cc:dd:ee:ff")

    def test_proc_net_arp_only_matches_requested_ip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proc_net_arp = Path(temp_dir) / "arp"
            proc_net_arp.write_text(_PROC_NET_ARP_CONTENT, encoding="utf-8")
            with (
                patch(
                    "ambientweather2sqlite.arp.subprocess.run",
                    side_effect=FileNotFoundError,
                ),
                patch("ambientweather2sqlite.arp._PROC_NET_ARP", proc_net_arp),
            ):
                mac = get_mac_address("192.168.0.50")

        self.assertIsNone(mac)

    def test_returns_none_when_every_source_fails(self):
        missing_proc_net_arp = Path("/nonexistent/proc/net/arp")
        with (
            patch(
                "ambientweather2sqlite.arp.subprocess.run",
                side_effect=FileNotFoundError,
            ),
            patch("ambientweather2sqlite.arp._PROC_NET_ARP", missing_proc_net_arp),
        ):
            self.assertIsNone(get_mac_address("192.168.0.99"))

    def test_skips_commands_without_a_usable_mac(self):
        # An unresolved neighbor entry in the first command's output must not
        # stop the later commands from being consulted.
        with patch(
            "ambientweather2sqlite.arp.subprocess.run",
            side_effect=[
                Mock(stdout="192.168.0.99 dev eth0 FAILED\n"),
                Mock(stdout=_MACOS_ARP_OUTPUT),
            ],
        ) as mock_run:
            mac = get_mac_address("192.168.0.99")

        self.assertEqual(mac, "00:11:22:03:44:55")
        self.assertEqual(mock_run.call_count, 2)
