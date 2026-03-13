from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypedDict

type SensorValue = int | float | None
type LiveData = dict[str, SensorValue]
type ObservationValue = str | SensorValue
type Observation = Mapping[str, ObservationValue]
type LabelMap = dict[str, str]
type ColumnUnitMap = dict[str, str]
type AggregationField = tuple[str, str, str]
type AggregatedValue = str | int | float | None
type AggregatedRow = dict[str, AggregatedValue]
type HourlyAggregatedData = dict[str, list[AggregatedRow | None]]
type QueryParams = dict[str, list[str]]


@dataclass(frozen=True, slots=True)
class AppConfig:
    live_data_url: str
    database_path: str
    port: int | None = None
    log_format: str = "text"


class LiveDataMetadata(TypedDict):
    labels: LabelMap


class LiveDataPayload(TypedDict):
    data: LiveData
    metadata: LiveDataMetadata


class DailyAggregatedPayload(TypedDict):
    data: list[AggregatedRow]


class HourlyAggregatedPayload(TypedDict):
    data: HourlyAggregatedData


class ErrorPayload(TypedDict):
    error: str


class HealthPayload(TypedDict):
    status: str
    last_observation_ts: str | None
    row_count: int


class DbMetrics(TypedDict):
    row_count: int
    db_file_size_bytes: int
    earliest_ts: str | None
    latest_ts: str | None
    column_count: int


type JsonResponse = (
    LiveDataPayload
    | DailyAggregatedPayload
    | HourlyAggregatedPayload
    | ErrorPayload
    | HealthPayload
    | DbMetrics
)


def build_live_data_payload(data: LiveData, labels: LabelMap) -> LiveDataPayload:
    return {
        "data": data,
        "metadata": {
            "labels": labels,
        },
    }


def build_daily_aggregated_payload(
    data: list[AggregatedRow],
) -> DailyAggregatedPayload:
    return {"data": data}


def build_hourly_aggregated_payload(
    data: HourlyAggregatedData,
) -> HourlyAggregatedPayload:
    return {"data": data}


def build_error_payload(message: str) -> ErrorPayload:
    return {"error": message}
