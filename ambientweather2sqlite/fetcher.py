import mureq
from awparser import extract_values


def fetch_live_data(live_data_url: str) -> dict[str, float | None]:
    response = mureq.get(live_data_url)
    body = response.body.decode("utf-8")
    return extract_values(body)
