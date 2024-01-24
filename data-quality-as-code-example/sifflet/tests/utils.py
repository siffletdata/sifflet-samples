import os
import shutil
from collections import OrderedDict
import typing as t


from sifflet.utils import dump_dict_to_yaml_file
from sifflet.collection_objects.settings import DEFAULT_VALUES_FILENAME


def compare_folders(tester, test_folder: str, correct_folder: str) -> None:
    """
    Compare two folders and check that they have the same files,
    with same content.

    Args:
        test_folder (str): Path to the folder containing collections to test
        correct_folder(str): Path to the folder containing correct collections
    """

    tester.assertTrue(os.path.isdir(test_folder))
    tester.assertEqual(
        len(os.listdir(test_folder)),
        len(os.listdir(correct_folder)),
    )

    for rendered_file in os.listdir(test_folder):
        with open(
            os.path.join(test_folder, rendered_file), "r", encoding="utf-8"
        ) as rendered:
            rendered_content = rendered.read()
        with open(
            os.path.join(correct_folder, rendered_file),
            "r",
            encoding="utf-8",
        ) as correct_rendered:
            correct_rendered_content = correct_rendered.read()
        tester.assertEqual(rendered_content, correct_rendered_content)


def reset_collection(
    collection_root: str,
    default_values: OrderedDict,
    monitor_files: t.Dict[str, OrderedDict],
):
    shutil.rmtree(collection_root, ignore_errors=True)
    os.makedirs(collection_root)

    dump_dict_to_yaml_file(
        os.path.join(collection_root, DEFAULT_VALUES_FILENAME),
        default_values,
    )

    for monitor_file, values in monitor_files.items():
        dump_dict_to_yaml_file(os.path.join(collection_root, monitor_file), values)
