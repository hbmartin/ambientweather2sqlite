class Aw2SqliteError(Exception):
    pass


class InvalidTimezoneError(Aw2SqliteError):
    def __init__(self, tz: str):
        super().__init__(f"Invalid timezone: {tz}")


class InvalidPriorDaysError(Aw2SqliteError):
    def __init__(self, prior_days: str):
        super().__init__(f"prior_days must be an integer, got {prior_days}")


class InvalidFormatError(Aw2SqliteError):
    def __init__(self, field: str):
        super().__init__(
            f"Invalid aggregation field: {field}. Expected format: 'function_column'",
        )


class InvalidColumnNameError(Aw2SqliteError):
    def __init__(self, column_name: str):
        super().__init__(
            f"Invalid column name: {column_name}.",
        )


class InvalidDateError(Aw2SqliteError):
    def __init__(self, date: str):
        super().__init__(f"Invalid date format: {date}. Expected YYYY-MM-DD")


class MissingAggregationFieldsError(Aw2SqliteError):
    def __init__(self):
        super().__init__(
            "At least one aggregation field must be provided e.g. /daily?q=avg_outHumi",
        )


class UnexpectedEmptyDictionaryError(Aw2SqliteError):
    def __init__(self):
        super().__init__("Dictionary is unexpectedly empty")
