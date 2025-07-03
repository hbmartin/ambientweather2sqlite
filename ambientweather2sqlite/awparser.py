from html.parser import HTMLParser

from ambientweather2sqlite.units_mapping import Units


class DisabledInputParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.filtered_values = {}

    def handle_starttag(self, tag, attrs):
        if tag == "input":
            # Convert attrs list to dict for easier access
            attr_dict = dict(attrs)

            # Check if input is disabled (disabled attribute present)
            if "disabled" in attr_dict:
                name = attr_dict.get("name")
                value = attr_dict.get("value")

                if name and value:
                    # Exclude battery-related inputs
                    if "Batt" in name or "Time" in name or "ID" in name:
                        return

                    try:
                        self.filtered_values[name] = float(value)
                    except ValueError:
                        self.filtered_values[name] = None


def extract_values(html_content: str) -> dict[str, float | None]:
    """Extracts values from disabled input fields in HTML content.

    Args:
        html_content (str): The HTML content as a string.

    Returns:
        dict: A dictionary where keys are the 'name' attributes of the input fields
              and values are their 'value' attributes, filtered as described.

    """
    parser = DisabledInputParser()
    parser.feed(html_content)
    return parser.filtered_values


class LabeledInputParser(HTMLParser):
    """A custom HTML parser to extract input names and their corresponding
    labels from the livedata.htm file.
    """

    def __init__(self):
        super().__init__()
        self.in_td = False
        self.is_label_cell = False
        self.row_cell_count = 0
        self.current_label = ""
        self.current_row_inputs = []
        self.data_dict: dict[str, str] = {}

    def handle_starttag(self, tag, attrs):
        # Reset row-specific state when a new table row starts
        if tag == "tr":
            self.row_cell_count = 0
            self.current_label = ""
            self.current_row_inputs = []

        # Track when we enter a table data cell
        elif tag == "td":
            self.in_td = True
            self.row_cell_count += 1
            # The first cell in a row with 2 cells is the label
            if self.row_cell_count == 1:
                self.is_label_cell = True

        # If we find an input tag, extract its name
        elif tag == "input" and not self.is_label_cell:
            attrs_dict = dict(attrs)
            if "name" in attrs_dict:
                self.current_row_inputs.append(attrs_dict["name"])

    def handle_data(self, data):
        # If we are inside the first td of a row, capture the text as a label
        if self.in_td and self.is_label_cell:
            self.current_label += data.strip()

    def handle_endtag(self, tag):
        if tag == "td":
            self.in_td = False
            # Once we leave the first cell, the next ones are not labels
            if self.row_cell_count == 1:
                self.is_label_cell = False

        # At the end of a row, process the collected data
        elif tag == "tr":
            if self.current_label and self.current_row_inputs:
                for input_name in self.current_row_inputs:
                    self.data_dict[input_name] = self.current_label


def extract_labels(html_content: str) -> dict[str, str]:
    """Parses the HTML content from livedata.htm to extract input names
    and their corresponding labels using Python's html.parser.

    Args:
        html_content (str): The HTML content of the file.

    Returns:
        dict: A dictionary mapping input names to the text of the
              preceding <td> element.

    """
    parser = LabeledInputParser()
    parser.feed(html_content)
    return parser.data_dict


class UnitsHTMLParser(HTMLParser):
    """A parser to extract selected weather station units from an HTML file.

    This parser identifies sections for each unit type as defined in the Units
    enum, finds the corresponding <select> element, and extracts the text from
    the <option> tag that has the 'selected' attribute.
    """

    def __init__(self, *, convert_charrefs: bool = True) -> None:
        super().__init__(convert_charrefs=convert_charrefs)
        # pyrefly: ignore  # bad-argument-type
        self._all_unit_values = {member.value for member in Units}
        self._is_in_unit_label_div = False
        self._is_in_selected_option = False
        self._current_unit_label: str | None = None
        self.extracted_units: dict[Units, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Processes start tags to identify relevant sections and selected options."""
        attributes = dict(attrs)
        # Check if we are entering a <div> that contains a unit label.
        if tag == "div" and attributes.get("class") == "item_1":
            self._is_in_unit_label_div = True

        # Check if we have found an <option> tag that is selected.
        # This must happen after a valid unit label has been identified.
        elif tag == "option" and self._current_unit_label is not None:
            if "selected" in attributes:
                self._is_in_selected_option = True

    def handle_endtag(self, tag: str) -> None:
        """Processes end tags to reset state flags."""
        if tag == "div":
            self._is_in_unit_label_div = False
        elif tag == "option":
            self._is_in_selected_option = False
        # When a <select> tag closes, we are done with the current unit.
        elif tag == "select" and self._current_unit_label:
            self._current_unit_label = None

    def handle_data(self, data: str) -> None:
        """Processes the text content within tags to extract labels and values."""
        # If inside a unit label div, check if the text corresponds to a
        # unit type we are interested in.
        if self._is_in_unit_label_div and data.strip() in self._all_unit_values:
            self._current_unit_label = data.strip()

        # If we are inside a selected option for a tracked unit, extract its text.
        elif self._is_in_selected_option and self._current_unit_label:
            if unit_value := data.strip():
                # Map the found label (e.g., "Solar Radiation") to the Enum member
                unit_enum_member = next(
                    (
                        member
                        # pyrefly: ignore  # bad-argument-type
                        for member in Units
                        if member.value == self._current_unit_label
                    ),
                    None,
                )
                if unit_enum_member:
                    self.extracted_units[unit_enum_member] = unit_value
                # Reset the flag to ensure we only capture one value.
                self._is_in_selected_option = False


def extract_units(html_content: str) -> dict[Units, str]:
    """Parses the HTML content from station.htm to extract selected units.

    Args:
        html_content (str): The HTML content of the file.

    Returns:
        dict: A dictionary mapping Units enum members to the text of the
              selected option.

    """
    parser = UnitsHTMLParser()
    parser.feed(html_content)
    found_units = parser.extracted_units
    if Units.HUMIDITY not in found_units:
        found_units[Units.HUMIDITY] = "%"
    if Units.WIND_DIRECTION not in found_units:
        found_units[Units.WIND_DIRECTION] = "°"
    return found_units
