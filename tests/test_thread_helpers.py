"""Assertions and timeouts reach the runner without unbounded worker joins."""

from threading import Event, current_thread
from unittest import TestCase

from tests.thread_helpers import run_worker


class TestBoundedWorkers(TestCase):
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
