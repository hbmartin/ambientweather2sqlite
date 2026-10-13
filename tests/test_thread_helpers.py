"""Assertions and timeouts reach the runner without unbounded worker joins."""

from threading import Event, current_thread
from unittest import TestCase
from unittest.mock import patch

from tests.thread_helpers import run_worker, start_worker


class TestBoundedWorkers(TestCase):
    def test_started_worker_captures_original_base_exceptions(self) -> None:
        for error in (
            AssertionError("worker assertion"),
            SystemExit("worker exit"),
            KeyboardInterrupt("worker interrupt"),
        ):
            with self.subTest(error=type(error).__name__):

                def action(failure: BaseException = error) -> None:
                    self.assertTrue(current_thread().daemon)
                    raise failure

                with patch("threading.excepthook") as hook:
                    thread, result = start_worker(action)
                    try:
                        with self.assertRaises(type(error)) as raised:
                            result.result(timeout=2)
                        self.assertIs(raised.exception, error)
                    finally:
                        thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                hook.assert_not_called()

    def test_started_worker_has_bounded_wait_and_completes_after_release(self) -> None:
        entered = Event()
        release = Event()

        def action() -> None:
            self.assertTrue(current_thread().daemon)
            entered.set()
            if not release.wait(timeout=2):
                raise AssertionError("Worker was not released")

        thread, result = start_worker(action)
        try:
            self.assertTrue(entered.wait(timeout=2))
            with self.assertRaises(TimeoutError):
                result.result(timeout=0.05)
            self.assertTrue(thread.is_alive())
        finally:
            release.set()
            thread.join(timeout=2)
        self.assertIsNone(result.result(timeout=2))
        self.assertFalse(thread.is_alive())

    def test_base_exceptions_propagate_without_invoking_thread_excepthook(self) -> None:
        for error in (SystemExit("worker exit"), KeyboardInterrupt("worker interrupt")):
            with self.subTest(error=type(error).__name__):

                def action(failure: BaseException = error) -> None:
                    raise failure

                with (
                    patch("threading.excepthook") as hook,
                    self.assertRaises(type(error)) as raised,
                ):
                    run_worker(action)
                self.assertIs(raised.exception, error)
                hook.assert_not_called()

    def test_assertions_propagate_from_daemon_worker(self) -> None:
        def action() -> None:
            self.assertTrue(current_thread().daemon)
            self.assertEqual("actual", "expected")

        with self.assertRaisesRegex(AssertionError, "actual"):
            run_worker(action)

    def test_blocked_worker_times_out_before_it_is_released(self) -> None:
        entered = Event()
        release = Event()
        done = Event()

        def action() -> None:
            entered.set()
            try:
                release.wait(timeout=2)
            finally:
                done.set()

        try:
            with self.assertRaises(TimeoutError):
                run_worker(action, timeout=0.05)
            self.assertTrue(entered.wait(timeout=2))
            self.assertFalse(done.is_set())
        finally:
            release.set()
            self.assertTrue(done.wait(timeout=2))

    def test_successful_worker_finishes_before_returning(self) -> None:
        done = Event()
        run_worker(done.set)
        self.assertTrue(done.is_set())
