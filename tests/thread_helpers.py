"""Daemon workers with exception propagation and explicit bounded cleanup."""

from collections.abc import Callable, Iterable
from concurrent.futures import Future
from contextlib import suppress
from threading import Thread
from time import monotonic


def start_worker(action: Callable[[], None]) -> tuple[Thread, Future[None]]:
    """Start a daemon worker, returning immediately with its thread and future.

    Callers own bounded future waits and joins, including a liveness check.
    Release blocking events before calling join_workers, even after failures.
    """
    result: Future[None] = Future()

    def run() -> None:
        try:
            action()
        except (BaseException,) as error:  # noqa: BLE001, B013
            result.set_exception(error)
        else:
            result.set_result(None)

    thread = Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def join_workers(threads: Iterable[Thread], *, timeout: float = 2) -> None:
    """Join workers against one deadline, raising if any remain alive."""
    deadline = monotonic() + timeout
    workers = tuple(threads)
    for thread in workers:
        thread.join(timeout=max(0, deadline - monotonic()))
    if any(thread.is_alive() for thread in workers):
        msg = "Test workers did not stop before their deadline"
        raise TimeoutError(msg)


def run_worker(action: Callable[[], None], *, timeout: float = 2) -> None:
    deadline = monotonic() + timeout
    thread, result = start_worker(action)
    try:
        result.result(timeout=max(0, deadline - monotonic()))
    except (BaseException,):  # noqa: B013
        # Cleanup timeouts must not replace the worker's original exception.
        with suppress(TimeoutError):
            join_workers((thread,), timeout=max(0, deadline - monotonic()))
        raise
    else:
        join_workers((thread,), timeout=max(0, deadline - monotonic()))
