from __future__ import annotations

from collections import OrderedDict

import typing as t


from sifflet.collection_objects.errors.classes import check_data_structure

from .types.collection import CollectionMonitorDict, DQACMonitorDict
from .settings import (
    COLLECTION_MONITOR_DATASETS_KEY,
    COLLECTION_MONITOR_IDENTIFIER_KEY,
    DQAC_MONITOR_ID_KEY,
)

if t.TYPE_CHECKING:
    from .collection import Collection


class Monitor:
    """
    A monitor is a yaml file containing a monitor configuration. It is located in a collection.
    The default values of the collection must be merged with the monitor.values dict before
    creating the monitor object.

    Args:
        monitor (dict): The monitor configuration, already merged with the default values
        collection (Collection): The collection containing the monitor
        dataset (UUID): The dataset id of the monitor
        filename (str, optional): The name of the file containing the monitor.
            Defaults to None.
    """

    def __init__(
        self,
        monitor: OrderedDict,
        collection: Collection,
        dataset: str,
        filepath: t.Optional[str] = None,
    ) -> None:
        self.values = check_data_structure(
            monitor, CollectionMonitorDict, filepath=filepath
        )
        self.collection = collection
        self.dataset = dataset

    def __str__(self) -> str:
        collection_name = str(self.collection)
        monitor_name = self.values[COLLECTION_MONITOR_IDENTIFIER_KEY]
        return f"{collection_name}.{monitor_name}"

    def clear_fields_for_api(self) -> DQACMonitorDict:
        cleared_monitor = OrderedDict(self.values)
        cleared_monitor[DQAC_MONITOR_ID_KEY] = self.collection.get_monitor_uuid(
            str(self)
        )
        del cleared_monitor[COLLECTION_MONITOR_IDENTIFIER_KEY]
        cleared_monitor[COLLECTION_MONITOR_DATASETS_KEY] = [
            {DQAC_MONITOR_ID_KEY: str(self.dataset)}
        ]
        return cleared_monitor  # type: ignore
