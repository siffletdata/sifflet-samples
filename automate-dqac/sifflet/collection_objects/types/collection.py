from typing import List
from typing_extensions import TypedDict


from .communs import MonitorsSharedFieldsDict
from .parameters_field import ParametersFieldDict


class DQACMonitorDict(MonitorsSharedFieldsDict):
    """
    The structure of a monitor in a DQAC file, ready for CLI
    """

    id: str  # (REQUIRED) ID of the monitor
    datasets: List[dict]  # (REQUIRED) List of datasets to monitor


class CollectionMonitorDict(MonitorsSharedFieldsDict):
    """
    The structure of a monitor in a collection file
    """

    identifier: str  # (REQUIRED) Identifier of the monitor. Unique in the collection
    parameters: ParametersFieldDict  # (REQUIRED) See Parameters


class CollectionDatasetFieldDict(TypedDict):
    """
    The structure of a dataset in a collection file
    """

    dataset: str
    monitors: List[CollectionMonitorDict]
