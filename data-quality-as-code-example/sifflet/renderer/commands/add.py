import typing as t

from termcolor import colored
from sifflet.renderer.structure_manager import StructureManager

from ..template_renderer import render_jinja2_template_to_dict
from ..settings import DATABASE


def print_end_of_adding(monitor_values, collection_root):
    print(
        colored("\n[SUCCESS]", "green", attrs=["bold"]),
        colored(
            f"Successfully added monitor <{monitor_values['name']}> "
            f"to collection {collection_root}",
            "green",
        ),
    )


def add_monitor(
    collection_root: str,
    dataset: str,
    template: str,
    collections_file: t.Optional[str],
    env: t.Optional[t.Dict[str, str]] = None,
    database=DATABASE,
    **kargs,
) -> None:
    if not env:
        env = {}

    if not collections_file:
        collections_file = "collections.yaml"
    monitor_values = render_jinja2_template_to_dict(template, env)
    collection_manager = StructureManager(collections_file, database)
    collection = collection_manager.get_collection(collection_root.replace("/", "."))
    collection.add_monitor_to_files(monitor_values, dataset, **kargs)
    print_end_of_adding(monitor_values, collection_root)
