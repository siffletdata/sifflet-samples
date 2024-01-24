import os
import typing as t
from typing import List

from sifflet.utils import read_yaml_file
from sifflet.renderer.database import Database
from sifflet.collection_objects.collection import Collection
from sifflet.collection_objects.errors.classes import check_data_structure
from sifflet.collection_objects.types import CollectionsToRenderFileDict

from .settings import WORKSPACE_COLLECTIONS_SETTING


class StructureManager:
    def __init__(self, collections_yaml_file: str, database: Database) -> None:
        """
        Initialize the StructureManager. This will read the workspace yaml file
        and initialize the list of root collections under the parameter
        WORKSPACE_COLLECTIONS_SETTING. The root collections are the root folders of the
        workspace directory. They can contain sub-collections that will automatically be imported.

        Args:
            workspace (str): The path to the workspace yaml file
        """
        self.database = database
        self.collections = self.get_collections_from_workspace(collections_yaml_file)
        self.collections_to_render = self.get_collections_to_render(
            collections_yaml_file, self.collections
        )

    def read_collections_declaration_file(
        self, collections_yaml_file: str
    ) -> List[str]:
        """
        Read the collections declaration file and add the collections to the collections list.
        Args:
            collections_yaml_file (str): The path to the collections declaration file
        """
        config = read_yaml_file(collections_yaml_file)
        config = check_data_structure(
            config,
            CollectionsToRenderFileDict,
            filepath=collections_yaml_file,
        )
        return config[WORKSPACE_COLLECTIONS_SETTING]

    def get_collections_from_workspace(
        self, collections_yaml_file: str
    ) -> List[Collection]:
        """
        Reads the workspace yaml file and returns the list of root and
        sub collections. If children collections are called, their parents
        will also be called to read default values.

        Args:
            workspace (str): The path to the workspace yaml file

        Returns:
            list[str]: all collections as relative paths
        """

        collections = self.read_collections_declaration_file(collections_yaml_file)
        collections_dir = os.path.dirname(collections_yaml_file)
        collections_root = [
            os.path.join(collections_dir, collections.split(".")[0])
            for collections in collections
        ]
        collections = [
            Collection(collection, database=self.database)
            for collection in collections_root
        ]
        for collection in collections:
            self.add_child_collections(collection, collections)
        return collections

    def get_collections_to_render(
        self, collections_yaml_file: str, collections: List[Collection]
    ) -> List[Collection]:
        """
        Filter the collections to only render the one declared in the workspace yaml file.
        Children of called collections will also be called to be rendered.

        Args:
            workspace (str): The path to the workspace yaml file

        Returns:
            list[str]: all collections as relative paths
        """

        collections_to_render_names = [
            os.path.join(os.path.dirname(collections_yaml_file), collection)
            .replace("/", ".")
            .split(".")
            for collection in self.read_collections_declaration_file(
                collections_yaml_file
            )
        ]
        collections_to_render = []

        for collection in collections:
            collection_name = str(collection).split(".")
            for collection_to_render_name in collections_to_render_names:
                # If the collection or a parent is called,
                # add it to the list of collections to render
                if len(collection_name) < len(collection_to_render_name):
                    continue
                if (
                    collection_name[: len(collection_to_render_name)]
                    == collection_to_render_name
                ):
                    collections_to_render.append(collection)

        return collections_to_render

    def get_collection(self, collection_id: str) -> Collection:
        """
        Get the collection object from the collection root. This will raise a FileNotFoundError
        if the collection does not exist.

        Args:
            collection_id (str): The path to the collection

        Returns:
            Collection: The collection object
        """
        for collection in self.collections:
            if str(collection) == collection_id:
                return collection
        raise FileNotFoundError(
            f"Could not find collection {collection_id} "
            "Please make sure the collection exists and is correctly setup."
        )

    def add_child_collections(
        self, collection: Collection, collections: List[Collection]
    ) -> None:
        """
        Add all the child collections recursively of the given collection to the collections list.
        Args:
            collection (str): The path to the collection
            collections (list[str]): The list of collections

        Returns:
            list[str]: The list of collections with the child collections
        """
        for child_collection in os.listdir(collection.collection_root):
            child_collection_root = os.path.join(
                str(collection.collection_root), child_collection
            )
            if os.path.isdir(child_collection_root):
                child_collection = Collection(
                    child_collection_root,
                    database=self.database,
                    parent_collection=collection,
                )
                collections.append(child_collection)
                self.add_child_collections(child_collection, collections)

    def __getitem__(self, index: int) -> Collection:
        return self.collections[index]

    def __iter__(self) -> t.Iterator[Collection]:
        return iter(self.collections)

    def __next__(self) -> Collection:
        return next(iter(self.collections))

    def __len__(self) -> int:
        return len(self.collections)
