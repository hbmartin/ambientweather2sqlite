"""Property-based tests for awparser.py using Hypothesis."""

from unittest import TestCase

from hypothesis import given, settings
from hypothesis import strategies as st

from ambientweather2sqlite.awparser import extract_labels, extract_values


def _make_disabled_input(name: str, value: str) -> str:
    return f'<input name="{name}" value="{value}" disabled>'


def _make_labeled_row(label: str, name: str, value: str) -> str:
    return (
        f"<tr><td>{label}</td><td>"
        f'<input name="{name}" value="{value}" disabled>'
        f"</td></tr>"
    )


# Strategy for generating valid sensor names (alphanumeric, no special chars)
_sensor_names = st.from_regex(r"[a-zA-Z][a-zA-Z0-9]{1,15}", fullmatch=True).filter(
    lambda s: "Batt" not in s and "Time" not in s and "ID" not in s,
)

_sensor_values = st.one_of(
    st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False),
    st.integers(min_value=-1000, max_value=1000),
)


class TestExtractValuesProperty(TestCase):
    @given(name=_sensor_names, value=_sensor_values)
    @settings(max_examples=100)
    def test_valid_input_always_parsed(self, name, value):
        html = _make_disabled_input(name, str(value))
        result = extract_values(html)

        self.assertIn(name, result)

    @given(data=st.text(min_size=0, max_size=500))
    @settings(max_examples=100)
    def test_never_crashes_on_arbitrary_text(self, data):
        # Should never raise, regardless of input
        result = extract_values(data)

        self.assertIsInstance(result, dict)

    @given(
        name=_sensor_names,
        value=st.text(
            alphabet=st.characters(categories=("L", "N", "P")),
            min_size=1,
            max_size=20,
        ),
    )
    @settings(max_examples=50)
    def test_non_numeric_values_return_none(self, name, value):
        # Filter out strings that happen to be valid floats
        try:
            float(value)
        except ValueError:
            pass
        else:
            return
        html = _make_disabled_input(name, value)
        result = extract_values(html)
        if name in result:
            self.assertIsNone(result[name])

    @given(
        name=st.from_regex(r"(Batt|Time|ID)[a-zA-Z0-9]{0,5}", fullmatch=True),
        value=_sensor_values,
    )
    @settings(max_examples=50)
    def test_filtered_fields_never_appear(self, name, value):
        html = _make_disabled_input(name, str(value))
        result = extract_values(html)

        self.assertNotIn(name, result)


class TestExtractLabelsProperty(TestCase):
    @given(
        label=st.text(
            alphabet=st.characters(categories=("L", "N", "Z"), max_codepoint=127),
            min_size=1,
            max_size=30,
        ).filter(lambda s: s.strip()),
        name=_sensor_names,
    )
    @settings(max_examples=100)
    def test_label_extraction_preserves_mapping(self, label, name):
        html = f"<table>{_make_labeled_row(label, name, '0.0')}</table>"
        result = extract_labels(html)

        self.assertIn(name, result)
        self.assertEqual(result[name], label.strip())

    @given(data=st.text(min_size=0, max_size=500))
    @settings(max_examples=100)
    def test_never_crashes_on_arbitrary_html(self, data):
        result = extract_labels(data)

        self.assertIsInstance(result, dict)

    @given(
        html=st.text(
            alphabet=st.characters(categories=("L", "N", "P", "S")),
            min_size=0,
            max_size=200,
        ),
    )
    @settings(max_examples=50)
    def test_malformed_html_returns_dict(self, html):
        # Wrap in some broken HTML structure
        broken = f"<table><tr><td>{html}</td><td><input></td></tr></table>"
        result = extract_labels(broken)

        self.assertIsInstance(result, dict)
