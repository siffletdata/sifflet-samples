from typing import Literal, List
from typing_extensions import NotRequired, TypedDict


class ParametersThresholdFieldDict(TypedDict):
    sensitivity: NotRequired[Literal["Low", "Normal", "High"]]
    bounds: NotRequired[Literal["Min", "Max", "MinAndMax"]]
    kind: NotRequired[Literal["Static", "Dynamic", "Exact"]]
    isMinInclusive: NotRequired[bool]
    min: NotRequired[float]
    isMaxInclusive: NotRequired[bool]
    max: NotRequired[float]
    onAddedCategory: NotRequired[bool]
    onRemovedCategory: NotRequired[bool]


class ParametersMetricsAggregationFieldDict(TypedDict):
    kind: Literal[
        "Average",
        "Count",
        "Range",
        "Quantile",
        "Sum",
        "StandardDeviation",
        "Variance",
        "NormalizedAverage",
        "DistinctCount",
        "Min",
        "Max",
    ]
    quantile: float


class ParametersPartitionFieldDict(TypedDict):
    kind: Literal["IngestionTime", "TimeUnitColumn", "IntegerRange"]
    interval: NotRequired[str]
    field: NotRequired[str]
    min: NotRequired[int]
    max: NotRequired[int]


class ParametersProfilingFieldDict(TypedDict):
    kind: Literal[
        "NullCount", "NullPercentage", "DuplicateCount", "DuplicatePercentage"
    ]


class ParametersGroupByFieldDict(TypedDict):
    field: str


class ParametersReferenceFieldDict(TypedDict):
    kind: Literal["Fixed", "Duration"]
    timestamp: NotRequired[str]
    delay: NotRequired[str]


class ParametersTimeWindowFieldDict(TypedDict):
    field: NotRequired[str]
    duration: NotRequired[str]
    offset: NotRequired[str]
    disableDeltaQuerying: NotRequired[bool]
    deltaQuerying: NotRequired[str]
    frequency: NotRequired[str]


class ParametersFieldProfilingFieldDict(TypedDict):
    kind: Literal[
        "NullCount", "NullPercentage", "DuplicateCount", "DuplicatePercentage"
    ]


class ParametersMetricsFieldDict(TypedDict):
    # TODO: complete this dict
    pass


class ParametersFormatFieldDict(TypedDict):
    kind: Literal["Email", "Phone", "UUID", "Regex"]
    regex: NotRequired[str]


class ParametersFieldDict(TypedDict):
    """
    Field ```parameter``` of a monitor.
    """

    kind: Literal[
        "Completeness",
        "Duplicates",
        "Freshness",
        "SchemaChange",
        "StaticMetrics",
        "DynamicMetrics",
        "CustomMetrics",
        "InterlinkedMetrics",
        "StaticFieldProfiling",
        "DynamicFieldProfiling",
        "Distribution",
        "FieldCardinality",
        "FieldDate",
        "FieldInList",
        "FieldUniqueness",
        "FieldFormat",
        "Sql",
    ]
    field: NotRequired[str]
    schedule: NotRequired[str]
    threshold: NotRequired[ParametersThresholdFieldDict]
    whereStatement: NotRequired[str]
    groupBy: NotRequired[ParametersGroupByFieldDict]
    timeWindow: NotRequired[ParametersTimeWindowFieldDict]
    partition: NotRequired[ParametersPartitionFieldDict]
    sql: NotRequired[str]
    aggregation: NotRequired[ParametersMetricsAggregationFieldDict]
    # metrics: NotRequired[List[ParametersMetricsFieldDict]]]
    metrics: NotRequired[List[dict]]
    profiling: NotRequired[ParametersProfilingFieldDict]
    fieldProfiling: NotRequired[ParametersFieldProfilingFieldDict]
    reference: NotRequired[ParametersReferenceFieldDict]
    minDate: NotRequired[str]
    maxDate: NotRequired[str]
    timeField: NotRequired[str]
    values: NotRequired[List[str]]
