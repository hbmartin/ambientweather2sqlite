from pathlib import Path


class Connection:
    def execute(self, sql: str, parameters: object) -> None:
        pass


# ruleid: ambientweather2sqlite.non-atomic-vacuum-destination
def backup_database(db_path: str, dest_path: str) -> int:
    destination = Path(dest_path)
    if destination.exists():
        raise FileExistsError(dest_path)
    connection = Connection()
    connection.execute("VACUUM INTO ?", (str(destination),))
    return 1


# ok: ambientweather2sqlite.non-atomic-vacuum-destination
def backup_database(db_path: str, dest_path: str) -> int:
    destination = Path(dest_path)
    destination.touch(exist_ok=False)
    connection = Connection()
    connection.execute("VACUUM INTO ?", (str(destination),))
    return 1


# ruleid: ambientweather2sqlite.unscoped-home-assistant-unique-id
def build_discovery_messages(labels: dict[str, str], client_id: str) -> list[dict]:
    messages = []
    for column in labels:
        payload = {"unique_id": f"aw2sqlite_{column}"}
        messages.append(payload)
    return messages


# ok: ambientweather2sqlite.unscoped-home-assistant-unique-id
def build_discovery_messages(labels: dict[str, str], client_id: str) -> list[dict]:
    node_id = f"aw2sqlite_{client_id}"
    messages = []
    for column in labels:
        payload = {"unique_id": f"{node_id}_{column}"}
        messages.append(payload)
    return messages


def query_latest_timestamp(database: str) -> str | None:
    return None


def query_db_metrics(database: str) -> dict[str, object]:
    return {"latest_ts": None}


# ruleid: ambientweather2sqlite.split-health-metric-snapshot
def _build_health_payload(database: str) -> dict[str, object]:
    latest = query_latest_timestamp(database)
    metrics = query_db_metrics(database)
    return {**metrics, "latest_ts": latest}


# ok: ambientweather2sqlite.split-health-metric-snapshot
def _build_health_payload(database: str) -> dict[str, object]:
    metrics = query_db_metrics(database)
    return {**metrics, "latest_ts": metrics["latest_ts"]}
