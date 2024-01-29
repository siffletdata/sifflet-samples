import os
from sifflet.collection_objects.settings import DEFAULT_VALUES_FILENAME


def create_collection(collection_root: str) -> None:
    """
    Create a new collection.

    Args:
        collection_root (str): The path to the collection where the monitor is added,
        in the format path.to.collection
    """

    path_to_collection = collection_root.replace(".", "/")

    try:
        os.makedirs(path_to_collection)
    except OSError as exc:
        raise OSError(
            f"Creation of the directory {path_to_collection} failed. "
            "Check the path and try again."
        ) from exc

    with open(
        os.path.join(path_to_collection, DEFAULT_VALUES_FILENAME), "w", encoding="utf-8"
    ) as default_file:
        default_file.write("")

    with open(
        os.path.join(path_to_collection, "README.md"), "w", encoding="utf-8"
    ) as init_file:
        init_file.write(f"# {collection_root}\n")

    print(f"Successfully created the collection {collection_root}.")
