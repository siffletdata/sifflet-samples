import typing as t
from collections import OrderedDict
import os
import yaml

from termcolor import colored


def ordered_load(stream, loader=yaml.SafeLoader) -> OrderedDict:
    class OrderedLoader(loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return OrderedDict(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )
    return yaml.load(stream, OrderedLoader)


def ordered_dump(
    data: t.Union[OrderedDict, t.TypedDict], stream=None, dumper=yaml.SafeDumper, **kwds
) -> str:
    class OrderedDumper(dumper):
        pass

    def _ordered_dict_representer(dumper, data):
        return dumper.represent_dict(data.items())

    OrderedDumper.add_representer(OrderedDict, _ordered_dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)


def dump_dict_to_yaml_file(file: str, data: t.Union[OrderedDict, t.TypedDict]) -> None:
    with open(file, "w", encoding="utf-8") as file_to_dump:
        ordered_dump(data, file_to_dump)


def get_dict_as_yaml_string(data: t.Union[OrderedDict, t.TypedDict]) -> str:
    return ordered_dump(data, None, default_flow_style=False)


def read_yaml_file(file: str) -> OrderedDict:
    if not os.path.isfile(file):
        raise FileNotFoundError(
            f"Could not find file {file}. Please make sure the file exists."
        )
    try:
        with open(file, "r", encoding="utf-8") as file_loaded:
            file_content = ordered_load(file_loaded)
    except Exception as exc:
        raise Exception(  # pylint: disable=broad-exception-raised
            f"Error loading file {file}. Please make sure the file has a valid format."
        ) from exc
    if not file_content:
        return OrderedDict({})
    return file_content


def merge_yaml_files(
    default_values: t.Union[OrderedDict, t.TypedDict],
    values: t.Union[OrderedDict, t.TypedDict],
) -> t.Any:
    """
    This file will merge the values of two yaml files. If conflicts arise, the
    values from the second file will be used. This is useful for overriding default
    values with user values.
    Args:
        default_values (dict): default values from yaml file
        values (dict): values to override default values
    Returns:
        dict: merged values
    """
    default_values_copy = default_values.copy()
    for key, value in values.items():
        if key not in default_values_copy:
            default_values_copy[key] = value

        elif isinstance(value, dict):
            default_values_key = OrderedDict(
                default_values_copy[key] if default_values_copy[key] else {}
            )
            default_values_copy[key] = merge_yaml_files(default_values_key, value)

        else:
            default_values_copy[key] = value
    return default_values_copy


def print_error(error: Exception) -> None:
    print(colored("\n[ERROR]\n\n", "red", attrs=["bold"]), colored(str(error), "red"))
