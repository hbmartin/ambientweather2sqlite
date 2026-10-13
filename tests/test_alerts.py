from concurrent.futures import Future
from http.client import HTTPException
from threading import Event, Thread
from unittest import TestCase
from unittest.mock import Mock, patch

from ambientweather2sqlite.alerts import AlertNotifier
from ambientweather2sqlite.http_validation import parse_http_url
from ambientweather2sqlite.models import AlertConfig
from tests.thread_helpers import start_worker

_CONFIG = AlertConfig(
    webhook_url="http://hooks.local/alert",
    failure_threshold=3,
    timeout_seconds=2.5,
)


def _make_notifier() -> tuple[AlertNotifier, Mock]:
    logger = Mock()
    return AlertNotifier(_CONFIG, "http://127.0.0.1/livedata.htm", logger), logger


def _start_collection_event(
    notifier: AlertNotifier, event: str, *, started: Event | None = None
) -> tuple[Thread, Future[None]]:
    def send() -> None:
        if started is not None:
            started.set()
        notifier.collection_event(event, None, 300)

    return start_worker(send)


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
                logger.warning.assert_called_once()
                self.assertFalse(notifier.alert_sent)
                self.assertEqual(notifier.consecutive_failures, 0)
                post.assert_not_called()
                dns.assert_not_called()

    def test_validation_is_cached_and_refreshes_when_the_url_changes(self) -> None:
        notifier = AlertNotifier(
            AlertConfig("http://", failure_threshold=1), "http://station.local/", Mock()
        )
        with (
            patch(
                "ambientweather2sqlite.alerts.parse_http_url", wraps=parse_http_url
            ) as validate,
            patch("ambientweather2sqlite.mureq.post") as post,
        ):
            notifier.record_failure()
            notifier.record_fetch_success()
            notifier.record_success()
            notifier.collection_event("collection_stalled", None, 300)
            self.assertEqual(validate.call_count, 1)
            notifier.logger.warning.assert_called_once()
            post.assert_not_called()

            notifier.config = AlertConfig("ftp://hooks.local/", failure_threshold=1)
            notifier.collection_event("collection_resumed", None, 0)
            self.assertEqual(validate.call_count, 2)
            self.assertEqual(notifier.logger.warning.call_count, 2)

            notifier.config = AlertConfig(
                "HTTP://hooks.local/alert", failure_threshold=1
            )
            notifier.record_failure()
            notifier.record_success()
            self.assertEqual(validate.call_count, 3)
            self.assertEqual(post.call_count, 2)
            self.assertEqual(
                [
                    (
                        call.kwargs["json"]["event"],
                        call.kwargs["json"]["consecutive_failures"],
                    )
                    for call in post.call_args_list
                ],
                [("station_offline", 1), ("station_online", 1)],
            )
            self.assertTrue(
                all(
                    call.args == ("HTTP://hooks.local/alert",)
                    for call in post.call_args_list
                )
            )
            notifier.config = AlertConfig("http://", failure_threshold=1)
            notifier.collection_event("collection_stalled", None, 300)
            self.assertEqual(validate.call_count, 4)
            self.assertEqual(notifier.logger.warning.call_count, 3)
        self.assertFalse(notifier.alert_sent)
        self.assertEqual(notifier.consecutive_failures, 0)

    def test_concurrent_invalid_events_validate_and_warn_once(self) -> None:
        entered = Event()
        second_started = Event()
        release = Event()
        logger = Mock()
        notifier = AlertNotifier(
            AlertConfig("http://"),
            "http://station.local/",
            logger,
            consecutive_failures=7,
            alert_sent=True,
        )
        workers: list[tuple[Thread, Future[None]]] = []

        def validate(url: str) -> None:
            entered.set()
            if not release.wait(timeout=2):
                raise AssertionError("Validation was not released")
            parse_http_url(url)

        def second_event() -> None:
            second_started.set()
            notifier.collection_event("collection_resumed", None, 0)

        with (
            patch(
                "ambientweather2sqlite.alerts.parse_http_url", side_effect=validate
            ) as parse,
            patch("ambientweather2sqlite.mureq.post") as post,
        ):
            try:
                workers.append(_start_collection_event(notifier, "collection_stalled"))
                self.assertTrue(entered.wait(timeout=2))
                workers.append(start_worker(second_event))
                self.assertTrue(second_started.wait(timeout=2))
                release.set()
                for _, result in workers:
                    result.result(timeout=2)
            finally:
                release.set()
                for thread, _ in workers:
                    thread.join(timeout=2)
        for thread, _ in workers:
            self.assertFalse(thread.is_alive())
        parse.assert_called_once_with("http://")
        logger.warning.assert_called_once()
        post.assert_not_called()
        self.assertTrue(notifier.alert_sent)
        self.assertEqual(notifier.consecutive_failures, 7)

    def test_concurrent_valid_events_wait_for_validation_and_both_deliver(self) -> None:
        entered = Event()
        second_started = Event()
        release = Event()
        notifier, logger = _make_notifier()
        workers: list[tuple[Thread, Future[None]]] = []

        def validate(url: str) -> None:
            entered.set()
            if not release.wait(timeout=2):
                raise AssertionError("Validation was not released")
            parse_http_url(url)

        with (
            patch(
                "ambientweather2sqlite.alerts.parse_http_url", side_effect=validate
            ) as parse,
            patch("ambientweather2sqlite.mureq.post") as post,
        ):
            try:
                workers.append(_start_collection_event(notifier, "collection_stalled"))
                self.assertTrue(entered.wait(timeout=2))
                workers.append(
                    _start_collection_event(
                        notifier, "collection_resumed", started=second_started
                    )
                )
                self.assertTrue(second_started.wait(timeout=2))
                with self.assertRaises(TimeoutError):
                    workers[1][1].result(timeout=0.05)
                release.set()
                for _, result in workers:
                    result.result(timeout=2)
            finally:
                release.set()
                for thread, _ in workers:
                    thread.join(timeout=2)
        for thread, _ in workers:
            self.assertFalse(thread.is_alive())
        parse.assert_called_once_with(_CONFIG.webhook_url)
        self.assertCountEqual(
            [call.kwargs["json"]["event"] for call in post.call_args_list],
            ["collection_stalled", "collection_resumed"],
        )
        for call in post.call_args_list:
            self.assertEqual(call.args, (_CONFIG.webhook_url,))
            self.assertEqual(call.kwargs["timeout"], _CONFIG.timeout_seconds)
        logger.warning.assert_not_called()

    def test_blocked_warning_allows_cached_events_and_url_replacement(self) -> None:
        entered = Event()
        release = Event()
        logger = Mock()
        notifier = AlertNotifier(
            AlertConfig("http://"), "http://station.local/", logger
        )
        workers: list[tuple[Thread, Future[None]]] = []

        def warning(_message: str, _error: ValueError) -> None:
            entered.set()
            if not release.wait(timeout=2):
                raise AssertionError("Warning was not released")

        logger.warning.side_effect = warning
        with (
            patch(
                "ambientweather2sqlite.alerts.parse_http_url", wraps=parse_http_url
            ) as parse,
            patch("ambientweather2sqlite.mureq.post") as post,
        ):
            try:
                workers.append(_start_collection_event(notifier, "collection_stalled"))
                self.assertTrue(entered.wait(timeout=2))
                workers.append(_start_collection_event(notifier, "collection_resumed"))
                workers[1][1].result(timeout=1)
                parse.assert_called_once_with("http://")
                logger.warning.assert_called_once()
                post.assert_not_called()
                notifier.config = _CONFIG
                workers.append(_start_collection_event(notifier, "collection_resumed"))
                workers[2][1].result(timeout=1)
                self.assertEqual(parse.call_count, 2)
                release.set()
                workers[0][1].result(timeout=2)
            finally:
                release.set()
                for thread, _ in workers:
                    thread.join(timeout=2)
        for thread, _ in workers:
            self.assertFalse(thread.is_alive())
        logger.warning.assert_called_once()
        post.assert_called_once()
        self.assertEqual(post.call_args.args, (_CONFIG.webhook_url,))
        self.assertEqual(post.call_args.kwargs["json"]["event"], "collection_resumed")
        self.assertEqual(notifier.consecutive_failures, 0)
        self.assertFalse(notifier.alert_sent)

    def test_validation_lock_does_not_serialize_deliveries(self) -> None:
        entered = Event()
        release = Event()
        notifier, _ = _make_notifier()
        workers: list[tuple[Thread, Future[None]]] = []

        def post(_url: str, *, json: dict[str, object], timeout: float) -> None:
            self.assertEqual(timeout, _CONFIG.timeout_seconds)
            if json["event"] == "collection_stalled":
                entered.set()
                if not release.wait(timeout=2):
                    raise AssertionError("Delivery was not released")

        with (
            patch("ambientweather2sqlite.mureq.post", side_effect=post),
            patch(
                "ambientweather2sqlite.alerts.parse_http_url", wraps=parse_http_url
            ) as validate,
        ):
            try:
                workers.append(_start_collection_event(notifier, "collection_stalled"))
                self.assertTrue(entered.wait(timeout=2))
                workers.append(
                    start_worker(
                        lambda: notifier.collection_event("collection_resumed", None, 0)
                    )
                )
                workers[1][1].result(timeout=2)
                release.set()
                workers[0][1].result(timeout=2)
            finally:
                release.set()
                for thread, _ in workers:
                    thread.join(timeout=2)
        for thread, _ in workers:
            self.assertFalse(thread.is_alive())
        validate.assert_called_once_with(_CONFIG.webhook_url)

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
