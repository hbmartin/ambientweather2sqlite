import re
import sqlite3
from contextlib import closing

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.database.identifiers import (
    _encoded_column_name,
    _resolve_sensor_names,
)


@given(raw_name=st.text())
def test_encoded_column_name_round_trips(raw_name: str) -> None:
    encoded = _encoded_column_name(raw_name)

    decoded = bytes.fromhex(encoded.removeprefix("aw2_")).decode("utf-8")

    assert decoded == raw_name
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", encoded)


@given(raw_names=st.lists(st.text(), min_size=1, max_size=8, unique=True))
def test_resolved_sensor_names_are_injective(raw_names: list[str]) -> None:
    with closing(sqlite3.connect(":memory:")) as conn:
        resolved = _resolve_sensor_names(conn, raw_names)

    assert set(resolved) == set(raw_names)
    assert len({column.casefold() for column in resolved.values()}) == len(raw_names)
