from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypedDict

type SensorValue = int | float | None
type ObservationValue = str | SensorValue
type Observation = Mapping[str, ObservationValue]
type LabelMap = dict[str, str]
type ColumnUnitMap = dict[str, str]
type AggregationField = tuple[str, str, str]
type AggregatedValue = str | int | float | None
type AggregatedRow = dict[str, AggregatedValue]
type HourlyAggregatedData = dict[str, list[AggregatedRow | None]]
type QueryParams = dict[str, list[str]]
type LiveData = dict[str, SensorValue]


@dataclass(frozen=True, slots=True)
class AppConfig:
    live_data_url: str
    database_path: str
    port: int | None = None


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


type JsonResponse = (
    LiveDataPayload | DailyAggregatedPayload | HourlyAggregatedPayload | ErrorPayload
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
