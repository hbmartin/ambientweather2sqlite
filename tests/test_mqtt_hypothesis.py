"""Property-based tests for MQTT wire encoders."""

from unittest import TestCase

from hypothesis import given
from hypothesis import strategies as st

from ambientweather2sqlite.mqtt import _encode_remaining_length, _encode_string


def _decode_remaining_length(encoded: bytes) -> int:
    multiplier = 1
    value = 0
    for digit in encoded:
        value += (digit & 0x7F) * multiplier
        multiplier *= 128
    return value


class TestMqttEncodingProperties(TestCase):
    @given(length=st.integers(min_value=0, max_value=268_435_455))
    def test_remaining_length_round_trips(self, length):
        encoded = _encode_remaining_length(length)

        self.assertEqual(_decode_remaining_length(encoded), length)
        self.assertLessEqual(len(encoded), 4)
        self.assertTrue(all(digit & 0x80 for digit in encoded[:-1]))
        self.assertFalse(encoded[-1] & 0x80)

    @given(value=st.text(max_size=1000))
    def test_utf8_string_length_prefix_matches_payload(self, value):
        encoded = _encode_string(value)
        payload_length = int.from_bytes(encoded[:2])

        self.assertEqual(payload_length, len(encoded[2:]))
        self.assertEqual(encoded[2:].decode(), value)
