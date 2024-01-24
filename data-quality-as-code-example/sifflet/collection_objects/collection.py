from __future__ import annotations
from collections import OrderedDict

import os
import typing as t
from typing import List
from uuid import UUID


from sifflet.utils import dump_dict_to_yaml_file, merge_yaml_files, read_yaml_file
from .monitor import Monitor
from .errors.classes import check_data_structure
from .types import CollectionMonitorsFileDict
from .settings import DEFAULT_VALUES_FILENAME

if t.TYPE_CHECKING:
    from sifflet.renderer.database import Database


class Collection:
    """
    A collection is a folder containing monitors. It can be a root collection or a sub-collection.
    """

    def __init__(
        self,
        collection_root: str,
        database: Database,
        parent_collection: t.Optional[Collection] = None,
    ) -> None:
        self.database = database
        self.collection_root = collection_root
        self.default_values = self.get_default_values(parent_collection)
        self.monitors = self.get_monitors()
        self.check_monitors_unicity()

    def check_monitors_unicity(self) -> None:
        """
        Check that all the monitors have a unique name
        """
        monitors_names = [str(monitor) for monitor in self.monitors]
        if len(monitors_names) != len(set(monitors_names)):
            raise ValueError(
                f"Monitors identifiers must be unique in the collection {self.collection_root}"
            )

    def get_default_values(
        self, parent_collection: t.Optional[Collection]
    ) -> OrderedDict:
        """
        Reads the default values file of the collection and returns the default values
        merged with the parent collection's default values. If the collection does not
        have a default values file, an empty dict is returned.

        Args:
            parent_collection (Collection): The parent collection if any.

        Returns:
            dict: The default values of the collection
        """
        collection_default_values_file = os.path.join(
            self.collection_root, DEFAULT_VALUES_FILENAME
        )
        if not os.path.exists(collection_default_values_file):
            default_values = OrderedDict({})
        else:
            default_values = read_yaml_file(collection_default_values_file)

        if parent_collection is None:
            return default_values

        merged_default_values = merge_yaml_files(
            parent_collection.default_values, default_values
        )

        return merged_default_values

    def get_monitor_uuid(self, monitor_identifier: str) -> UUID:
        """
        Reads the database to retrieve the uuid of the monitor and write it to the
        monitor's dict. If the monitor is not in the database, it is added.
        Args:
            monitor_identifier (str): The monitor identifier
            uuid_value (str): The uuid value
        """
        uuid_value = self.database.read_uuid(monitor_identifier)
        if not uuid_value:
            uuid_value = self.database.add_uuid(monitor_identifier)
        return uuid_value

    def get_monitors_files(self) -> List[str]:
        """
        Returns:
            list[str]: The list of monitors files
        """
        monitors_files = [
            file
            for file in os.listdir(self.collection_root)
            if file.endswith((".yaml", ".yml")) and file != DEFAULT_VALUES_FILENAME
        ]

        return monitors_files

    def check_files_format(
        self, files_path: List[str]
    ) -> List[CollectionMonitorsFileDict]:
        """
        Check that all the files have a valid yaml format, i.e. a "datasets"
        and optional "default_values" keys/

        Args:
            files (list[str]): The list of files to check
            collection_root (str): The collection root
        """
        files_config: List[CollectionMonitorsFileDict] = []
        for file in files_path:
            file_config = read_yaml_file(file)
            file_config = check_data_structure(
                file_config,
                CollectionMonitorsFileDict,
                filepath=file,
            )
            files_config.append(file_config)
        return files_config

    def get_monitors(self) -> List[Monitor]:
        """
        Reads the collection's root directory and the yaml files it contains.
        The format of yaml files is checked and an error is raised is the format is not valid.

        Returns:
            list[str]: The list of monitors, merged with the default values
        """
        monitors = []
        yaml_files_names = self.get_monitors_files()
        yaml_files_paths = [
            os.path.join(self.collection_root, file) for file in yaml_files_names
        ]
        files_config = self.check_files_format(yaml_files_paths)

        for file_config, filename in zip(files_config, yaml_files_names):
            for dataset in file_config["datasets"]:
                for monitor in dataset["monitors"]:
                    monitor = self.build_monitor(monitor, dataset["dataset"], filename)
                    monitors.append(monitor)
        return monitors

    def build_monitor(
        self,
        monitor: OrderedDict,
        dataset: str,
        filename: t.Optional[str] = None,
    ) -> Monitor:
        """
        Build a monitor from a dict. The dict is merged with the default values
        of the collection, and an uuid is added to the monitor.
        Args:
            monitor (dict): The monitor specific values
            dataset (str): The dataset to which the monitor belongs\n
            filename (str): [Optional] The filename of the monitor file if the monitor.
            comes from a file.

        Returns:
            Monitor: the Monitor object
        """
        monitor = merge_yaml_files(self.default_values, monitor)
        kargs = {}
        if filename:
            kargs["filepath"] = os.path.join(self.collection_root, filename)

        built_monitor = Monitor(monitor, self, dataset, **kargs)
        return built_monitor

    def add_monitor_to_files(
        self,
        monitor: OrderedDict,
        dataset: str,
        filename: t.Optional[str] = None,
        **kargs,
    ) -> None:
        merged_monitor = merge_yaml_files(self.default_values, monitor)
        monitor_to_add = self.build_monitor(merged_monitor, dataset, filename)
        if str(monitor_to_add) in [str(monitor) for monitor in self.monitors]:
            if kargs.get("update_monitor", False):
                self.remove_monitor_from_files(str(monitor_to_add))
                self.monitors = [
                    monitor
                    for monitor in self.monitors
                    if str(monitor) != str(monitor_to_add)
                ]
                self.monitors.append(monitor_to_add)
            else:
                raise ValueError(
                    f"Monitor {monitor_to_add} already exists "
                    f"in collection {self}.\n"
                    "If you want to replace it, use the --update_monitor flag."
                )
        else:
            self.monitors.append(monitor_to_add)

        self.check_monitors_unicity()

        if not filename:
            filename = self.get_filename_for_dataset(dataset)

        file_path = os.path.join(self.collection_root, filename)
        file_config = read_yaml_file(file_path)
        for dataset_config in file_config["datasets"]:
            if dataset_config["dataset"] == dataset:
                # if the dataset is already in the file, add the monitor to it
                dataset_config["monitors"].append(monitor)
                dump_dict_to_yaml_file(file_path, file_config)
                return

        # if the dataset is not in the file, add it
        file_config["datasets"].append({"dataset": dataset, "monitors": [monitor]})
        dump_dict_to_yaml_file(file_path, file_config)

    def remove_monitor_from_files(self, monitor_identifier: str) -> None:
        """
        Remove a monitor from the collection. If the monitor is not in the collection,
        an error is raised.

        Args:
            monitor_identifier (str): The monitor identifier
        """
        for file in self.get_monitors_files():
            file_config = read_yaml_file(os.path.join(self.collection_root, file))
            for dataset in file_config["datasets"]:
                for monitor in dataset["monitors"]:
                    if (
                        ".".join([str(self), monitor["identifier"]])
                        == monitor_identifier
                    ):
                        dataset["monitors"].remove(monitor)
                        dump_dict_to_yaml_file(
                            os.path.join(self.collection_root, file), file_config
                        )
                        print(f"Removed monitor {monitor_identifier} from file {file}")
                        return
        raise ValueError(
            f"Monitor {monitor_identifier} is not in collection {self.collection_root}"
        )

    def get_filename_for_dataset(self, dataset: str) -> str:
        """
        Get the filename for a dataset. If the dataset is not in a file of the collection,
        a new file is created with the dataset as a name. The file is initialized
        with an empty list of monitors.

        Args:
            dataset (str): The dataset

        Returns:
            str: The filename containing the dataset
        """
        monitors_files = self.get_monitors_files()
        for monitors_file in monitors_files:
            file_config = read_yaml_file(
                os.path.join(self.collection_root, monitors_file)
            )
            datasets_id = [dataset["dataset"] for dataset in file_config["datasets"]]
            if dataset in datasets_id:
                return monitors_file

        filename = f"{dataset}.yaml"
        init_file_data = OrderedDict(
            {"datasets": [{"dataset": dataset, "monitors": []}]}
        )
        dump_dict_to_yaml_file(
            os.path.join(self.collection_root, filename),
            init_file_data,
        )
        return filename

    def __len__(self) -> int:
        return len(self.monitors)

    def __iter__(self) -> t.Iterator[Monitor]:
        return iter(self.monitors)

    def __getitem__(self, index: int) -> Monitor:
        return self.monitors[index]

    def __str__(self) -> str:
        return self.collection_root.replace(os.sep, ".")

    def __repr__(self) -> str:
        return f"Collection({self.collection_root})"
