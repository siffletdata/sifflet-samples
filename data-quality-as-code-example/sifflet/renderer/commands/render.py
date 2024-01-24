import os
import shutil

from termcolor import colored
from sifflet.collection_objects.collection import Collection
from sifflet.renderer.database import Database
from sifflet.utils import dump_dict_to_yaml_file
from ..structure_manager import StructureManager
from ..settings import DATABASE, RENDERED_FOLDER


def validate_file_extension(workspace_file: str) -> None:
    if not workspace_file.endswith((".yaml", ".yml")):
        raise ValueError(f"Workspace file must be a yaml file, got {workspace_file}")


def render_collection_to_folder(collection: Collection, rendered_folder: str) -> None:
    for monitor in collection:
        filepath = os.path.join(rendered_folder, f"{monitor}.yaml")
        monitor_ready_for_api = monitor.clear_fields_for_api()
        dump_dict_to_yaml_file(filepath, monitor_ready_for_api)  # type: ignore


def print_end_of_rendering(collections_manager: StructureManager):
    num_collections = len(collections_manager.collections_to_render)
    number_of_monitors = sum(
        len(collection) for collection in collections_manager.collections_to_render
    )
    print(
        colored("\n[SUCCESS]", "green", attrs=["bold"]),
        colored(
            f"Successfully rendered {number_of_monitors} "
            f"{'monitors' if number_of_monitors > 1 else 'monitor'} as code "
            f'from {num_collections} {"collections" if num_collections > 1 else "collection"}!',
            "green",
        ),
    )


def render_monitors(
    database: Database = DATABASE,
    rendered_folder: str = RENDERED_FOLDER,
    collections_yaml_file: str = "collections.yaml",
) -> None:
    """
    Renders monitors from a given workspace file using helper functions.

    Parameters:
        - workspace_file (str): Path to the workspace file.
        - database (Database): Database to be used. Defaults to DATABASE.
        - rendered_folder (str): Folder to save rendered monitors. Defaults to RENDERED_FOLDER.

    Returns:
        None
    """
    print(f"\nRendering monitors from {collections_yaml_file}...")
    validate_file_extension(collections_yaml_file)

    collections_manager = StructureManager(collections_yaml_file, database)
    print(
        f"Found {len(collections_manager.collections_to_render)} "
        f"{'collections' if len(collections_manager.collections_to_render) > 1 else 'collection'}\n"
    )

    shutil.rmtree(rendered_folder, ignore_errors=True)

    os.makedirs(rendered_folder, exist_ok=True)

    for collection in collections_manager.collections_to_render:
        print(f"Rendering monitors from {collection}...")
        render_collection_to_folder(collection, rendered_folder)

    print_end_of_rendering(collections_manager)
