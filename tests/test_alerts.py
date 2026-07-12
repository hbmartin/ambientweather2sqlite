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
        for error in (HTTPException("boom"), OSError("net down"), TimeoutError()):
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
