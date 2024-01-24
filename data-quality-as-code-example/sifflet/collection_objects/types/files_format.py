from typing import List
from typing_extensions import NotRequired, TypedDict


class CollectionsToRenderFileDict(TypedDict):
    collections: List[str]


class DatasetInCollectionMonitorsFileDict(TypedDict):
    dataset: str
    monitors: list


class CollectionMonitorsFileDict(TypedDict):
    """
    The structure of a collection file.
    """

    datasets: List[DatasetInCollectionMonitorsFileDict]
    default_values: NotRequired[dict]


class CollectionDefaultValuesFileDict(TypedDict):
    """
    The structure of a default values yaml file
    """

    description: str
    tags: List[dict]
    terms: List[dict]
    schedule: str
    incident: dict
    notifications: dict
    parameters: dict
