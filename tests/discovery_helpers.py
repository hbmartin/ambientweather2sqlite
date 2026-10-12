from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from unittest import TestCase

    from ambientweather2sqlite.models import MqttConfig


def assert_sensor_identity(
    test: TestCase,
    column: str,
    topic: str,
    payload: Mapping[str, object],
    config: MqttConfig,
) -> None:
    node_id = f"aw2sqlite_{config.client_id}"
    test.assertEqual(
        topic, f"{config.discovery_prefix}/sensor/{node_id}/{column}/config"
    )
    test.assertEqual(payload["unique_id"], f"{node_id}_{column}")
    test.assertEqual(payload["state_topic"], config.topic)
    test.assertEqual(payload["value_template"], f"{{{{ value_json.{column} }}}}")
    test.assertEqual(payload["state_class"], "measurement")
