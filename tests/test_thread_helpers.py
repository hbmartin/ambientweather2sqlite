"""Assertions and timeouts reach the runner without unbounded worker joins."""

from threading import Event, Thread, current_thread
from typing import TYPE_CHECKING
from unittest import TestCase
from unittest.mock import Mock, patch

from tests.thread_helpers import join_workers, run_worker, start_worker

if TYPE_CHECKING:
    from concurrent.futures import Future


class TestBoundedWorkers(TestCase):
    def test_join_workers_handles_empty_successful_and_failed_workers(self) -> None:
        join_workers(())
        failure = AssertionError("original worker failure")

        def fail() -> None:
            raise failure

        first_thread, success = start_worker(lambda: None)
        second_thread, failed = start_worker(fail)
        join_workers(thread for thread in (first_thread, second_thread))
        self.assertIsNone(success.result(timeout=2))
        with self.assertRaises(AssertionError) as raised:
            failed.result(timeout=2)
        self.assertIs(raised.exception, failure)
        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())

    def test_join_workers_shares_one_budget_and_checks_all_workers(self) -> None:
        current = 10.0
        first = Mock(spec=Thread)
        second = Mock(spec=Thread)
        first.is_alive.return_value = True
        second.is_alive.return_value = False

        def consume_budget(*, timeout: float) -> None:
            nonlocal current
            current += timeout

        first.join.side_effect = consume_budget
        with (
            patch("tests.thread_helpers.monotonic", side_effect=lambda: current),
            self.assertRaisesRegex(TimeoutError, "did not stop before their deadline"),
        ):
            join_workers((first, second), timeout=1)
        first.join.assert_called_once_with(timeout=1)
        second.join.assert_called_once_with(timeout=0)

    def test_join_workers_times_out_and_workers_exit_after_release(self) -> None:
        release = Event()
        entered = (Event(), Event())
        workers: list[tuple[Thread, Future[None]]] = []

        def wait(started: Event) -> None:
            started.set()
            if not release.wait(timeout=2):
                raise AssertionError("Worker was not released")

        try:
            workers.extend(
                start_worker(lambda event=started: wait(event)) for started in entered
            )
            for started in entered:
                self.assertTrue(started.wait(timeout=2))
            with self.assertRaises(TimeoutError):
                join_workers((thread for thread, _ in workers), timeout=0.05)
            self.assertTrue(all(thread.is_alive() for thread, _ in workers))
        finally:
            release.set()
            join_workers(thread for thread, _ in workers)
        for _, result in workers:
            self.assertIsNone(result.result(timeout=2))

    def test_run_worker_preserves_original_exception_if_cleanup_times_out(self) -> None:
        failure = AssertionError("original worker failure")

        def fail() -> None:
            raise failure

        with (
            patch(
                "tests.thread_helpers.join_workers", side_effect=TimeoutError("cleanup")
            ),
            self.assertRaises(AssertionError) as raised,
        ):
            run_worker(fail)
        self.assertIs(raised.exception, failure)

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
