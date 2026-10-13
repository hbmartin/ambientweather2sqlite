from http.client import HTTPException
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.models import AlertConfig

_CONFIG = AlertConfig(
    webhook_url="http://hooks.local/alert",
    failure_threshold=3,
    timeout_seconds=2.5,
)


def _make_notifier() -> tuple[AlertNotifier, Mock]:
    logger = Mock()
    return AlertNotifier(_CONFIG, "http://127.0.0.1/livedata.htm", logger), logger


class TestAlertNotifier(TestCase):
    @patch("ambientweather2sqlite.alerts.mureq.post")
    def test_collection_events_include_freshness_without_resetting_failures(
        self,
        mock_post: Mock,
    ) -> None:
        notifier, _ = _make_notifier()
        notifier.record_failure()
        for event in ("collection_stalled", "collection_resumed"):
            notifier.collection_event(event, "2026-10-12 16:00:00", 600.0)
            payload = mock_post.call_args.kwargs["json"]
            self.assertEqual(payload["event"], event)
            self.assertEqual(payload["last_observation_ts"], "2026-10-12 16:00:00")
            self.assertEqual(payload["age_seconds"], 600.0)
            self.assertEqual(payload["station_url"], notifier.station_url)
        self.assertEqual(notifier.consecutive_failures, 1)
        self.assertFalse(notifier.alert_sent)

    @patch("ambientweather2sqlite.alerts.mureq.post")
    def test_offline_fires_once_at_threshold(self, mock_post: Mock):
        notifier, _ = _make_notifier()

        notifier.record_failure()
        notifier.record_failure()
        mock_post.assert_not_called()

        notifier.record_failure()

        mock_post.assert_called_once()
        url = mock_post.call_args.args[0]
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(url, "http://hooks.local/alert")
        self.assertEqual(payload["event"], "station_offline")
        self.assertEqual(payload["station_url"], "http://127.0.0.1/livedata.htm")
        self.assertEqual(payload["consecutive_failures"], 3)
        self.assertIn("timestamp", payload)
        self.assertEqual(mock_post.call_args.kwargs["timeout"], 2.5)

    @patch("ambientweather2sqlite.alerts.mureq.post")
    def test_continued_failures_do_not_repost(self, mock_post: Mock):
        notifier, _ = _make_notifier()

        for _unused in range(10):
            notifier.record_failure()

        mock_post.assert_called_once()

    @patch("ambientweather2sqlite.alerts.mureq.post")
    def test_recovery_posts_online_and_resets(self, mock_post: Mock):
        notifier, _ = _make_notifier()
        for _unused in range(3):
            notifier.record_failure()

        notifier.record_success()

        self.assertEqual(mock_post.call_count, 2)
        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["event"], "station_online")
        self.assertEqual(payload["consecutive_failures"], 3)
        self.assertEqual(notifier.consecutive_failures, 0)
        self.assertFalse(notifier.alert_sent)

        # A fresh outage alerts again after the reset.
        for _unused in range(3):
            notifier.record_failure()
        self.assertEqual(mock_post.call_count, 3)

    @patch("ambientweather2sqlite.alerts.mureq.post")
    def test_success_without_prior_alert_posts_nothing(self, mock_post: Mock):
        notifier, _ = _make_notifier()
        notifier.record_failure()

        notifier.record_success()

        mock_post.assert_not_called()
        self.assertEqual(notifier.consecutive_failures, 0)

    def test_webhook_errors_are_logged_and_swallowed(self):
        for error in (
            HTTPException("boom"),
            OSError("net down"),
            TimeoutError(),
            ValueError("bad response"),
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid byte"),
        ):
            with self.subTest(error=type(error).__name__):
                notifier, logger = _make_notifier()
                with patch(
                    "ambientweather2sqlite.alerts.mureq.post",
                    side_effect=error,
                ):
                    for _unused in range(3):
                        notifier.record_failure()

                logger.warning.assert_called_once()
                self.assertIn(
                    "Alert webhook failed",
                    logger.warning.call_args.args[0],
                )

    def test_invalid_direct_webhook_urls_are_contained_for_every_event(self) -> None:
        for url in (
            "http://",
            "ftp://hooks.local/",
            "http://hooks.local:0/",
            "http://hooks local/",
            "http://sta\ttion.local/",
            "http://" + chr(0xD800) + ".invalid/",
        ):
            with self.subTest(url=repr(url)):
                logger = Mock()
                notifier = AlertNotifier(
                    AlertConfig(url, failure_threshold=1),
                    "http://station.local/",
                    logger,
                )
                with (
                    patch("socket.getaddrinfo") as dns,
                    patch("ambientweather2sqlite.mureq.post") as post,
                ):
                    notifier.record_failure()
                    self.assertTrue(notifier.alert_sent)
                    self.assertEqual(notifier.consecutive_failures, 1)
                    notifier.record_fetch_success()
                    notifier.record_success()
                    for event in ("collection_stalled", "collection_resumed"):
                        notifier.collection_event(event, None, 300.0)
                self.assertEqual(logger.warning.call_count, 4)
                self.assertFalse(notifier.alert_sent)
                self.assertEqual(notifier.consecutive_failures, 0)
                post.assert_not_called()
                dns.assert_not_called()

    def test_transport_value_errors_preserve_outage_counts_and_request_arguments(
        self,
    ) -> None:
        notifier, logger = _make_notifier()
        with patch(
            "ambientweather2sqlite.mureq.post",
            side_effect=ValueError("invalid response"),
        ) as post:
            for _ in range(4):
                notifier.record_failure()
            notifier.record_fetch_success()
            notifier.record_success()
            notifier.collection_event("collection_stalled", None, 300.0)
            notifier.collection_event("collection_resumed", "2026-10-12 16:00:00", 0)
            for _ in range(3):
                notifier.record_failure()
            notifier.record_success()
        self.assertEqual(logger.warning.call_count, 6)
        self.assertEqual(
            [
                (
                    call.kwargs["json"]["event"],
                    call.kwargs["json"]["consecutive_failures"],
                )
                for call in post.call_args_list
                if call.kwargs["json"]["event"].startswith("station_")
            ],
            [
                ("station_offline", 3),
                ("station_online", 4),
                ("station_offline", 3),
                ("station_online", 3),
            ],
        )
        for call in post.call_args_list:
            self.assertEqual(call.args, (_CONFIG.webhook_url,))
            self.assertEqual(call.kwargs["timeout"], _CONFIG.timeout_seconds)
        self.assertFalse(notifier.alert_sent)
        self.assertEqual(notifier.consecutive_failures, 0)
