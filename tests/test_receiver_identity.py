"""Receiver provenance, persistence, and verification across public entrypoints."""

import json
import os
import sqlite3
import stat
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, call, patch
from urllib.error import HTTPError
from urllib.request import urlopen

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.arp import normalize_mac
from ambientweather2sqlite.cli.commands import main
from ambientweather2sqlite.daemon import _collect_forever, _serve_station
from ambientweather2sqlite.database import (
    create_database_if_not_exists,
    insert_observation,
    query_sensor_metadata,
)
from ambientweather2sqlite.exceptions import ReceiverIdentityMismatch
from ambientweather2sqlite.file_io import atomic_write_json
from ambientweather2sqlite.metadata import create_metadata, metadata_path
from ambientweather2sqlite.models import AlertConfig, MqttConfig, RediscoveryConfig
from ambientweather2sqlite.receiver_state import (
    legacy_receiver_state_path,
    load_receiver_identity,
    receiver_state_path,
    reset_receiver_identity,
    save_receiver_identity,
)
from ambientweather2sqlite.rediscovery import StationRediscovery, create_rediscovery
from ambientweather2sqlite.server import Server
from ambientweather2sqlite.station_http import _receiver_lock, station_get

_A = "http://192.0.2.10/livedata.htm"
_B = "http://192.0.2.20/livedata.htm"
_MAC_A = "aa:bb:cc:dd:ee:01"
_MAC_B = "aa:bb:cc:dd:ee:02"
_PAGE = '<input name="outTemp" disabled value="72.5">'
_LABELED_PAGE = (
    '<table><tr><td>Temperature</td><td><input name="outTemp" '
    'disabled value="72.5"></td></tr></table>'
)


class TestReceiverState(TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.database = str(self.directory / "station.db")
        self.path = receiver_state_path(self.database)
        self.legacy = legacy_receiver_state_path(self.database)
        self.logger = Mock()

    def _load(self) -> str | None:
        return load_receiver_identity(
            self.path, _A, self.logger, legacy_path=self.legacy
        )

    def test_extensions_are_distinct_and_normalized_legacy_identity_is_migrated(self):
        self.assertNotEqual(
            self.path, receiver_state_path(str(self.directory / "station.sqlite"))
        )
        self.legacy.write_text(
            json.dumps({"configured_url": _A, "mac_address": _MAC_A.upper()})
        )
        self.assertEqual(self._load(), _MAC_A)
        self.assertFalse(self.legacy.exists())
        self.assertEqual(json.loads(self.path.read_text())["mac_address"], _MAC_A)
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(list(self.directory.iterdir()), [self.path])

    def test_current_file_is_authoritative_even_if_invalid_or_scoped_elsewhere(self):
        save_receiver_identity(self.legacy, _A, _MAC_A)
        for document in (
            "{",
            "[]",
            json.dumps({"configured_url": _A, "mac_address": "invalid"}),
            json.dumps({"configured_url": _B, "mac_address": _MAC_B}),
        ):
            with self.subTest(document=document):
                self.path.write_text(document)
                self.assertIsNone(self._load())
                self.assertTrue(self.legacy.exists())
        save_receiver_identity(self.path, _A, _MAC_B)
        self.assertEqual(self._load(), _MAC_B)

    def test_invalid_or_differently_scoped_legacy_state_is_not_migrated(self):
        for document in ("{", "[]", '{"configured_url":"other"}'):
            with self.subTest(document=document):
                self.legacy.write_text(document)
                self.assertIsNone(self._load())
                self.assertFalse(self.path.exists())
                self.assertTrue(self.legacy.exists())

    def test_failed_migration_keeps_legacy_and_uses_identity_in_memory(self):
        save_receiver_identity(self.legacy, _A, _MAC_A)
        with patch(
            "ambientweather2sqlite.receiver_state.save_receiver_identity",
            side_effect=OSError("read-only"),
        ):
            self.assertEqual(self._load(), _MAC_A)
        self.assertTrue(self.legacy.exists())
        self.assertFalse(self.path.exists())
        self.logger.warning.assert_called_once()

    def test_read_failure_does_not_resurrect_legacy(self):
        save_receiver_identity(self.legacy, _A, _MAC_A)
        self.path.mkdir()
        self.assertIsNone(self._load())
        self.assertTrue(self.legacy.exists())
        self.logger.warning.assert_called_once()

    def test_reset_is_idempotent_and_does_not_delete_another_urls_legacy(self):
        save_receiver_identity(self.path, _A, _MAC_A)
        save_receiver_identity(self.legacy, _A, _MAC_A)
        reset_receiver_identity(self.database, _A)
        reset_receiver_identity(self.database, _A)
        self.assertEqual(list(self.directory.iterdir()), [])
        save_receiver_identity(self.legacy, _B, _MAC_B)
        reset_receiver_identity(self.database, _A)
        self.assertTrue(self.legacy.exists())

    def test_reset_removes_legacy_before_current_and_propagates_failure(self):
        save_receiver_identity(self.path, _A, _MAC_A)
        save_receiver_identity(self.legacy, _A, _MAC_A)
        original_unlink = Path.unlink
        removals = []

        def unlink(path: Path, *, missing_ok: bool = False) -> None:
            removals.append(path)
            if path == self.legacy:
                raise PermissionError("busy")
            return original_unlink(path, missing_ok=missing_ok)

        with patch.object(Path, "unlink", unlink), self.assertRaises(PermissionError):
            reset_receiver_identity(self.database, _A)
        self.assertEqual(removals, [self.legacy])
        self.assertTrue(self.path.exists())

    def test_json_replacement_preserves_existing_file_on_interruption(self):
        atomic_write_json(self.path, {"original": True}, mode=0o600)
        for operation, error in (
            ("json.dump", OSError("full")),
            ("os.fsync", KeyboardInterrupt()),
        ):
            with (
                self.subTest(operation=operation),
                patch(f"ambientweather2sqlite.file_io.{operation}", side_effect=error),
                self.assertRaises(type(error)),
            ):
                atomic_write_json(self.path, {"replacement": True})
            self.assertEqual(json.loads(self.path.read_text()), {"original": True})
            self.assertEqual(list(self.directory.iterdir()), [self.path])

    def test_json_modes_and_parent_sync(self):
        with patch("ambientweather2sqlite.file_io.os.fsync") as sync:
            atomic_write_json(self.path, {"first": True})
        self.assertEqual(sync.call_count, 1 if os.name == "nt" else 2)
        if os.name != "nt":
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o644)
            self.path.chmod(0o640)
            atomic_write_json(self.path, {"second": True})
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o640)
            save_receiver_identity(self.path, _A, _MAC_A)
            self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    @given(st.lists(st.integers(min_value=0, max_value=255), min_size=6, max_size=6))
    def test_mac_spellings_round_trip(self, octets):
        expected = ":".join(f"{octet:02x}" for octet in octets)
        if expected in ("00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"):
            expected = None
        for separator in (":", "-"):
            self.assertEqual(
                normalize_mac(separator.join(f"{octet:X}" for octet in octets)),
                expected,
            )


class TestReceiverCollection(TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.database = str(self.directory / "station.db")
        create_database_if_not_exists(self.database)
        self.logger = Mock()
        self.mac = self.enterContext(
            patch(
                "ambientweather2sqlite.rediscovery.get_mac_address", return_value=None
            )
        )

    def _tracker(self, mac=None, threshold=1) -> StationRediscovery:
        return StationRediscovery(
            RediscoveryConfig(failure_threshold=threshold),
            _A,
            self.logger,
            mac_address=mac,
            state_path=receiver_state_path(self.database),
        )

    def _rows(self) -> list[tuple[float]]:
        with closing(sqlite3.connect(self.database)) as connection:
            return connection.execute("SELECT outTemp FROM observations").fetchall()

    def test_temporary_fallback_collects_b_but_restart_learns_a(self):
        tracker = self._tracker()
        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                side_effect=[TimeoutError, _PAGE],
            ),
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                return_value=(_B, _MAC_B),
            ),
            patch("ambientweather2sqlite.daemon._refresh_metadata", return_value=True),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, KeyboardInterrupt],
            ),
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.mac.return_value = _MAC_B
            _collect_forever(
                _A,
                self.database,
                {},
                {},
                self.logger,
                None,
                60,
                rediscovery=tracker,
                interactive=False,
            )
        self.assertEqual(self._rows(), [(72.5,)])
        self.assertEqual(tracker.mac_address, _MAC_B)
        self.assertFalse(receiver_state_path(self.database).exists())
        restored = self._tracker()
        self.assertIsNone(restored.mac_address)
        self.assertEqual(restored.current_url, _A)
        self.mac.return_value = _MAC_A
        restored.verify_identity(_A)
        restored.record_success()
        self.assertEqual(
            json.loads(receiver_state_path(self.database).read_text())["mac_address"],
            _MAC_A,
        )

    def test_later_mac_capture_and_relocations_stay_temporary(self):
        tracker = self._tracker()
        with (
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                side_effect=[(_B, None), ("http://192.0.2.30/livedata.htm", _MAC_B)],
            ) as find,
            patch(
                "ambientweather2sqlite.rediscovery.time.monotonic", side_effect=[0, 301]
            ),
        ):
            self.assertEqual(tracker.record_failure(), _B)
            self.mac.return_value = _MAC_B
            tracker.record_success()
            self.assertEqual(tracker.record_failure(), "http://192.0.2.30/livedata.htm")
        self.assertEqual(find.call_args_list, [call(None), call(_MAC_B)])
        self.assertFalse(receiver_state_path(self.database).exists())

    def test_mismatch_never_stores_publishes_or_records_success(self):
        tracker = self._tracker(_MAC_A, threshold=2)
        notifier = AlertNotifier(
            AlertConfig(webhook_url="http://hooks.invalid/"), _A, self.logger
        )
        self.mac.return_value = _MAC_B
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", return_value=_PAGE),
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                return_value=None,
            ) as find,
            patch("ambientweather2sqlite.daemon.time.monotonic", return_value=100),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, None, KeyboardInterrupt],
            ) as wait,
            patch("ambientweather2sqlite.daemon.insert_observation") as insert,
            patch("ambientweather2sqlite.daemon._publish_observation") as publish,
            patch("ambientweather2sqlite.daemon._emit_observation") as emit,
            patch.object(AlertNotifier, "_post") as post,
            patch("builtins.print") as output,
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _A,
                self.database,
                {},
                {},
                self.logger,
                None,
                60,
                notifier,
                tracker,
                interactive=False,
            )
        insert.assert_not_called()
        publish.assert_not_called()
        emit.assert_not_called()
        find.assert_called_once_with(_MAC_A)
        self.assertEqual(tracker.consecutive_failures, 3)
        self.assertEqual(notifier.consecutive_failures, 3)
        self.assertEqual(self.mac.call_count, 3)
        post.assert_called_once_with("station_offline", 3, reason="identity_mismatch")
        self.assertEqual(wait.call_args_list, [call(160, show_countdown=False)] * 3)
        self.assertEqual(
            sum("identity mismatch" in str(c) for c in output.call_args_list), 3
        )
        self.logger.warning.assert_called_once()

    def test_metadata_checks_both_responses_before_any_persistence(self):
        tracker = self._tracker(_MAC_A)
        self.mac.side_effect = [_MAC_A, _MAC_B]
        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                side_effect=[_LABELED_PAGE, "<html>units</html>"],
            ) as get,
            patch("builtins.print"),
        ):
            self.assertEqual(
                create_metadata(
                    self.database, _A, verify_identity=tracker.verify_identity
                ),
                ({}, {}),
            )
        self.assertEqual(get.call_count, 2)
        self.assertEqual(query_sensor_metadata(self.database), [])
        self.assertFalse(metadata_path(self.database).exists())
        self.assertFalse(receiver_state_path(self.database).exists())

    def test_unreadable_current_identity_preserves_successful_collection(self):
        tracker = self._tracker(_MAC_A)
        tracker.consecutive_failures = 1
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", return_value=_PAGE),
            patch("ambientweather2sqlite.daemon._refresh_metadata", return_value=True),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=KeyboardInterrupt,
            ),
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _A,
                self.database,
                {},
                {},
                self.logger,
                None,
                60,
                rediscovery=tracker,
                interactive=False,
            )
        self.assertEqual(self._rows(), [(72.5,)])
        self.assertEqual(tracker.mac_address, _MAC_A)
        self.assertEqual(tracker.consecutive_failures, 0)

    def test_metadata_and_proxy_verification_never_learn_identity(self):
        tracker = self._tracker()
        self.mac.return_value = _MAC_B
        with patch(
            "ambientweather2sqlite.station_http.mureq.get", return_value=_LABELED_PAGE
        ):
            self.assertEqual(
                station_get(_A, verify_identity=tracker.verify_identity), _LABELED_PAGE
            )
            create_metadata(self.database, _A, verify_identity=tracker.verify_identity)
        self.assertIsNone(tracker.mac_address)
        self.mac.assert_not_called()
        self.assertFalse(receiver_state_path(self.database).exists())

    def test_startup_restores_identity_before_metadata_or_discovery(self):
        save_receiver_identity(receiver_state_path(self.database), _A, _MAC_A)
        self.mac.return_value = _MAC_B
        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                return_value=_LABELED_PAGE,
            ),
            patch("ambientweather2sqlite.daemon._collect_forever") as collect,
            patch("ambientweather2sqlite.daemon.publish_message") as publish,
            patch("ambientweather2sqlite.daemon.Server") as server,
            patch("builtins.print"),
        ):
            _serve_station(
                _A,
                self.database,
                self.logger,
                None,
                port=8000,
                period_seconds=60,
                host="localhost",
                auth_token=None,
                mqtt_config=MqttConfig(host="broker", topic="obs", discovery=True),
                rediscovery_config=None,
                interactive=False,
            )
        self.assertEqual(query_sensor_metadata(self.database), [])
        self.assertFalse(metadata_path(self.database).exists())
        publish.assert_not_called()
        tracker = collect.call_args.args[8]
        self.assertEqual(tracker.mac_address, _MAC_A)
        self.assertEqual(
            server.call_args.kwargs["verify_identity"], tracker.verify_identity
        )

    def test_relocation_refreshes_same_schema_and_resets_stale_comparison(self):
        insert_observation(self.database, {"ts": "2000-01-01", "outTemp": 1})
        tracker = self._tracker(_MAC_A)
        self.mac.return_value = _MAC_A
        labels = {"outTemp": "Old label"}
        units = {"outTemp": "old"}
        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                side_effect=[_PAGE, TimeoutError, _PAGE],
            ),
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                return_value=(_B, _MAC_A),
            ),
            patch(
                "ambientweather2sqlite.daemon.create_metadata",
                return_value=({"outTemp": "New label"}, {"outTemp": "new"}),
            ) as metadata,
            patch("ambientweather2sqlite.daemon._publish_discovery") as discovery,
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None, None, KeyboardInterrupt],
            ),
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _A,
                self.database,
                labels,
                units,
                self.logger,
                None,
                60,
                rediscovery=tracker,
                interactive=False,
            )
        self.assertEqual(self._rows(), [(1.0,), (72.5,), (72.5,)])
        self.assertEqual(labels, {"outTemp": "New label"})
        self.assertEqual(units, {"outTemp": "new"})
        metadata.assert_called_once_with(
            self.database, _B, verify_identity=tracker.verify_identity
        )
        discovery.assert_called_once()

    def test_failed_relocation_metadata_retries_even_with_complete_labels_and_stale_pages(
        self,
    ):
        insert_observation(self.database, {"ts": "2000-01-01", "outTemp": 1})
        tracker = self._tracker(_MAC_A)
        self.mac.return_value = _MAC_A
        labels = {"outTemp": "Old label"}
        attempts = []
        expected_refreshes = 2

        def refresh(_db, url, names, units, *_args, **_kwargs: object) -> bool:
            attempts.append(url)
            if len(attempts) == 1:
                self.assertEqual(names, {})
                self.assertEqual(units, {})
            names["outTemp"] = "New label"
            return len(attempts) == expected_refreshes

        with (
            patch(
                "ambientweather2sqlite.station_http.mureq.get",
                side_effect=[_PAGE, TimeoutError] + [_PAGE] * 8,
            ),
            patch(
                "ambientweather2sqlite.rediscovery.find_station_by_mac",
                return_value=(_B, _MAC_A),
            ),
            patch(
                "ambientweather2sqlite.daemon._refresh_metadata", side_effect=refresh
            ),
            patch(
                "ambientweather2sqlite.daemon.wait_for_next_update",
                side_effect=[None] * 9 + [KeyboardInterrupt],
            ),
            patch("builtins.print"),
            self.assertRaises(KeyboardInterrupt),
        ):
            _collect_forever(
                _A,
                self.database,
                labels,
                {"outTemp": "old"},
                self.logger,
                None,
                60,
                rediscovery=tracker,
                interactive=False,
            )
        self.assertEqual(attempts, [_B, _B])
        self.assertEqual(len(self._rows()), 3)

    def test_verifier_checks_fetched_url_inside_http_lock_after_relocation(self):
        tracker = self._tracker(_MAC_A)
        checked = []

        def get(_url, **_kwargs: object) -> str:
            tracker.current_url = _B
            return _PAGE

        def mac(ip) -> str:
            checked.append(ip)
            lock = _receiver_lock(_A)
            acquired = lock.acquire(blocking=False)
            if acquired:
                lock.release()
            self.assertFalse(acquired)
            return _MAC_B if ip == "192.0.2.10" else _MAC_A

        self.mac.side_effect = mac
        with (
            patch("ambientweather2sqlite.station_http.mureq.get", side_effect=get),
            self.assertRaises(ReceiverIdentityMismatch) as error,
        ):
            station_get(_A, verify_identity=tracker.verify_identity)
        self.assertEqual(checked, ["192.0.2.10"])
        self.assertEqual(error.exception.url, _A)
        self.assertEqual(error.exception.expected_mac, _MAC_A)
        self.assertEqual(error.exception.observed_mac, _MAC_B)

    def test_warning_throttle_changes_and_verified_recovery(self):
        tracker = self._tracker(_MAC_A)
        self.mac.side_effect = [_MAC_B] * 4 + [_MAC_A, _MAC_B]
        with patch(
            "ambientweather2sqlite.rediscovery.time.monotonic",
            side_effect=[0, 10, 300, 301, 302],
        ):
            for url in (_A, _A, _A, _B):
                with self.assertRaises(ReceiverIdentityMismatch):
                    tracker.verify_identity(url)
            tracker.verify_identity(_A)
            with self.assertRaises(ReceiverIdentityMismatch):
                tracker.verify_identity(_A)
        self.assertEqual(self.logger.warning.call_count, 4)


class TestReceiverCommands(TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.database = str(self.directory / "station.db")
        self.config = self.directory / "config.toml"
        self.config.write_text(
            f'live_data_url = "{_A}"\ndatabase_path = "{self.database}"\n'
        )
        self.path = receiver_state_path(self.database)

    def test_reset_uses_existing_config_without_setup_and_is_idempotent(self):
        save_receiver_identity(self.path, _A, _MAC_A)
        with (
            patch("ambientweather2sqlite.cli.commands.create_config_file") as setup,
            patch("builtins.print"),
        ):
            main(["receiver", "reset", "--config", str(self.config)])
            main(["receiver", "reset", "--config", str(self.config)])
        self.assertFalse(self.path.exists())
        setup.assert_not_called()

    def test_reset_reports_missing_config_and_filesystem_failure(self):
        with (
            patch(
                "ambientweather2sqlite.cli.commands.get_config_path", return_value=None
            ),
            patch("builtins.print"),
            self.assertRaises(SystemExit) as error,
        ):
            main(["receiver", "reset"])
        self.assertEqual(error.exception.code, 1)
        self.path.mkdir()
        with patch("builtins.print"), self.assertRaises(SystemExit) as error:
            main(["receiver", "reset", "--config", str(self.config)])
        self.assertEqual(error.exception.code, 1)

    def test_once_and_online_metadata_honor_saved_identity(self):
        create_database_if_not_exists(self.database)
        save_receiver_identity(self.path, _A, _MAC_A)
        for command in ("once", "metadata"):
            with (
                self.subTest(command=command),
                patch(
                    "ambientweather2sqlite.rediscovery.get_mac_address",
                    return_value=_MAC_B,
                ),
                patch(
                    "ambientweather2sqlite.station_http.mureq.get",
                    return_value=_LABELED_PAGE,
                ),
                patch("builtins.print") as output,
                self.assertRaises(SystemExit) as error,
            ):
                main([command, "--config", str(self.config)])
            self.assertEqual(error.exception.code, 1)
            self.assertFalse(any("72.5" in str(c) for c in output.call_args_list))
        self.assertEqual(query_sensor_metadata(self.database), [])

    def test_disabled_rediscovery_does_not_read_or_migrate_identity(self):
        legacy = legacy_receiver_state_path(self.database)
        save_receiver_identity(legacy, _A, _MAC_A)
        self.assertIsNone(
            create_rediscovery(
                RediscoveryConfig(enabled=False), _A, self.database, Mock()
            )
        )
        self.assertTrue(legacy.exists())
        self.assertFalse(self.path.exists())


class TestReceiverProxy(TestCase):
    def test_live_mismatch_and_concurrent_relocation_leave_health_responsive(self):
        directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        database = str(directory / "station.db")
        create_database_if_not_exists(database)
        insert_observation(database, {"outTemp": 1})
        tracker = StationRediscovery(
            RediscoveryConfig(), _A, Mock(), mac_address=_MAC_A
        )
        server = Server(
            _A, database, 0, "127.0.0.1", verify_identity=tracker.verify_identity
        )
        self.addCleanup(server.shutdown)
        server.start()
        base = f"http://127.0.0.1:{server.httpd.server_port}"

        def request(path) -> tuple[int, dict[str, object]]:
            try:
                response = urlopen(base + path, timeout=4)
            except HTTPError as error:
                response = error
            with response:
                return response.status, json.loads(response.read())

        for _ in range(3):
            entered = threading.Event()
            release = threading.Event()
            server.update_live_data_url(_A)

            def get(
                _url, *, entered=entered, release=release, **_kwargs: object
            ) -> str:
                entered.set()
                if not release.wait(timeout=4):
                    raise TimeoutError
                return _LABELED_PAGE

            with (
                patch("ambientweather2sqlite.station_http.mureq.get", side_effect=get),
                patch(
                    "ambientweather2sqlite.rediscovery.get_mac_address",
                    side_effect=lambda ip: _MAC_B if ip == "192.0.2.10" else _MAC_A,
                ),
                ThreadPoolExecutor(max_workers=1) as pool,
            ):
                future = pool.submit(request, "/")
                try:
                    self.assertTrue(entered.wait(timeout=2))
                    self.assertEqual(request("/health")[0], 200)
                    tracker.current_url = _B
                    server.update_live_data_url(_B)
                finally:
                    release.set()
                status, payload = future.result(timeout=3)
                self.assertEqual(status, 503)
                self.assertEqual(set(payload), {"error"})
                self.assertIn(_A, payload["error"])
                self.assertEqual(request("/")[0], 200)
