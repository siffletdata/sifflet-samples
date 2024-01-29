"""
The database is a json file. It stores a list of key / values, where keys are
CollectionMonitor names and values are uuids.
"""
from abc import ABC, abstractmethod
import os
from typing import Optional
import uuid
import json


class Database(ABC):
    @abstractmethod
    def add_uuid(self, monitor_key: str) -> uuid.UUID:
        """
        Generates an uuid and adds the monitor to the database.
        Args:
            monitor_key (str): The monitor name (i.e. str(monitor))
        Raises:
            ValueError: If the monitor already exists in the database.
        """

    @abstractmethod
    def read_uuid(self, monitor_key: str) -> Optional[uuid.UUID]:
        pass

    @abstractmethod
    def delete_uuid(self, monitor_key: str) -> None:
        pass


class DatabaseManager(Database):
    def __init__(self, json_path: str) -> None:
        self.database_file = json_path
        self.create_database()

    def create_database(self) -> None:
        """
        Create the database file if it does not exist.
        """
        dirs = os.path.dirname(self.database_file)
        if dirs and not os.path.exists(dirs):
            os.makedirs(dirs)
        if not os.path.exists(self.database_file):
            with open(self.database_file, "w", encoding="utf-8") as database:
                json.dump({}, database)

    def add_uuid(self, monitor_key: str) -> uuid.UUID:
        with open(self.database_file, "r", encoding="utf-8") as database:
            data = json.load(database)
        if monitor_key in data:
            raise ValueError(f"Monitor {monitor_key} already exists in database")
        data[monitor_key] = str(uuid.uuid4())
        with open(self.database_file, "w", encoding="utf-8") as database:
            json.dump(data, database)
        return data[monitor_key]

    def read_uuid(self, monitor_key: str) -> Optional[uuid.UUID]:
        with open(self.database_file, "r", encoding="utf-8") as database:
            data = json.load(database)
            if not monitor_key in data:
                return None
            return data[monitor_key]

    def delete_uuid(self, monitor_key: str) -> None:
        with open(self.database_file, "r", encoding="utf-8") as database:
            data = json.load(database)
            if not monitor_key in data:
                raise ValueError(f"Monitor {monitor_key} does not exist in database")
            del data[monitor_key]
        with open(self.database_file, "w", encoding="utf-8") as database:
            json.dump(data, database)
