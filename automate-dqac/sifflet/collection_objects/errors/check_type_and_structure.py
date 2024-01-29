from typing import Any, List, Type, get_type_hints, Literal, Union
from collections import OrderedDict
from typing_extensions import NotRequired


def is_literal(typ: Type) -> bool:
    """Check if a type is a Literal type."""
    return getattr(typ, "__origin__", None) == Literal


def get_detailed_type(data):
    basic_type = type(data)

    if basic_type.__name__ == "list" and data:
        element_type = type(data[0]).__name__
        return f"list.{element_type}"

    return basic_type


def check_structure_and_type(
    data: Any, expected_type: Type, path: str = ""
) -> List[str]:
    """Recursively check structure and type of a given data against expected type."""
    errors = []
    actual_type = type(data)

    if is_literal(expected_type):
        if data not in expected_type.__args__:
            errors.append(
                f"Expected one of {expected_type.__args__} at {path}, but got value: {data}"
            )
        return errors

    if getattr(expected_type, "__origin__", None) == Union:
        union_errors = []
        for utype in expected_type.__args__:
            union_errors = check_structure_and_type(data, utype, path)
            if not union_errors:
                break
        if union_errors:
            errors.extend(union_errors)
        return errors
    if actual_type == list and getattr(expected_type, "__origin__", None) == list:
        for index, item in enumerate(data):
            # get None if expected_type.__args__[0] doesn't exists

            child_expected_type = (
                expected_type.__args__[0]
                if getattr(expected_type, "__args__", None)
                else None
            )
            if not child_expected_type:
                continue
            item_errors = check_structure_and_type(
                item, child_expected_type, f"{path}[{index}]"
            )
            errors.extend(item_errors)
        return errors

    if actual_type in [dict, OrderedDict]:
        expected_keys = get_type_hints(expected_type)

        # Check for missing or extra keys
        for key, expected_key_type in expected_keys.items():
            # Check if the key is not required and not present in the data
            if (
                getattr(expected_key_type, "__origin__", None) == NotRequired
                and key not in data
            ):
                continue

            if key not in data:
                path_string = ".".join([path, key]) if path else key
                errors.append(f"Missing key: {path_string}")
            else:
                key_errors = check_structure_and_type(
                    data[key],
                    expected_key_type.__args__[0]
                    if getattr(expected_key_type, "__origin__", None) == NotRequired
                    else expected_key_type,
                    ".".join([path, key]) if path else key,
                )
                errors.extend(key_errors)

        for key in data.keys():
            if key not in expected_keys:
                path_string = ".".join([path, key]) if path else key
                errors.append(f"Extra key: {path_string}")

        return errors

    # Check for type mismatch
    if actual_type != expected_type and expected_type in [
        str,
        int,
        float,
        bool,
        list,
    ]:
        errors.append(f"Expected {expected_type} at {path}, but got {actual_type}")

    return errors
