"""Bounded daemon workers that propagate assertions to the test thread."""

from collections.abc import Callable
from concurrent.futures import Future
from threading import Thread
from time import monotonic


def run_worker(action: Callable[[], None], *, timeout: float = 2) -> None:
    result: Future[None] = Future()

    def run() -> None:
        try:
            action()
        except (BaseException,) as error:  # noqa: BLE001, B013
            result.set_exception(error)
        else:
            result.set_result(None)

    thread = Thread(target=run, daemon=True)
    deadline = monotonic() + timeout
    thread.start()
    try:
        result.result(timeout=timeout)
    finally:
        thread.join(timeout=max(0, deadline - monotonic()))
    if thread.is_alive():
        msg = "Test worker did not stop before its deadline"
        raise TimeoutError(msg)
