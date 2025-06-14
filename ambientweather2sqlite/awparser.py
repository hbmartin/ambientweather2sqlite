from html.parser import HTMLParser


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
