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
