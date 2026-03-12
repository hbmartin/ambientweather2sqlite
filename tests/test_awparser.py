from unittest import TestCase

from ambientweather2sqlite.awparser import extract_labels, extract_units, extract_values
from ambientweather2sqlite.units_mapping import Units


class TestAwparser(TestCase):
    def test_extract_values_filters_disabled_inputs_and_casts_numbers(self):
        html = """
        <input name="tempf" value="72.5" disabled>
        <input name="rainrate" value="not-a-number" disabled>
        <input name="soilBatt1" value="1" disabled>
        <input name="stationTime" value="12:00" disabled>
        <input name="stationID" value="abc" disabled>
        <input name="ignored" value="99">
        <input name="missing-value" disabled>
        """

        self.assertEqual(
            extract_values(html),
            {
                "tempf": 72.5,
                "rainrate": None,
            },
        )

    def test_extract_labels_maps_all_inputs_in_a_row(self):
        html = """
        <table>
            <tr>
                <td>Outdoor Temperature</td>
                <td><input name="tempf"></td>
                <td><input name="tempinf"></td>
            </tr>
            <tr>
                <td>Ignored Row</td>
                <td><input></td>
            </tr>
            <tr>
                <td>Humidity</td>
            </tr>
        </table>
        """

        self.assertEqual(
            extract_labels(html),
            {
                "tempf": "Outdoor Temperature",
                "tempinf": "Outdoor Temperature",
            },
        )

    def test_extract_units_reads_selected_values_and_adds_defaults(self):
        html = """
        <div class="item_1">Temperature</div>
        <select>
            <option>degC</option>
            <option selected>degF</option>
        </select>
        <div class="item_1">Pressure</div>
        <select>
            <option selected>inhg</option>
        </select>
        <div class="item_1">Not a real unit</div>
        <select>
            <option selected>ignored</option>
        </select>
        """

        self.assertEqual(
            extract_units(html),
            {
                Units.TEMPERATURE: "degF",
                Units.PRESSURE: "inhg",
                Units.HUMIDITY: "%",
                Units.WIND_DIRECTION: "°",
            },
        )
