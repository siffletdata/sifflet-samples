from __future__ import annotations
import typing as t
from typing_extensions import OrderedDict

from sifflet.utils import get_dict_as_yaml_string
from .check_type_and_structure import check_structure_and_type
from ..settings import COLLECTION_MONITOR_IDENTIFIER_KEY

TABULATION = "   "

T = t.TypeVar("T")


class WrongCollectionMonitorsFileFormatError(Exception):
    """
    Raised when a collection file containing monitors has a wrong format.

    Additionnal arguments:
        filepath (str): The path to the wrong file
        format_error (str): The error message from typeguard library
    """

    def __init__(self, **kargs) -> None:
        self.kargs = kargs
        super().__init__()

    @property
    def format_error_message(self) -> str:
        format_error = self.kargs.get("format_error")
        if format_error is None:
            return ""
        return f"\n\n[Format errors]\n{format_error}"

    @property
    def filepath_message(self) -> str:
        filepath = self.kargs.get("filepath")
        if filepath is None:
            return ""
        return f"\n\n[File]\n{TABULATION}{filepath}"

    def __str__(self) -> str:
        return f"Wrong file format.{self.filepath_message}{self.format_error_message}"


class WrongCollectionDefaultValuesFileFormatError(Exception):
    """
    Raised when a collection default values file has a wrong format.

    Additionnal arguments:
        filepath (str): The path to the wrong file
        format_error (str): The error message from typeguard library
    """

    def __init__(self, **kargs) -> None:
        self.kargs = kargs
        super().__init__()

    @property
    def format_error_message(self) -> str:
        format_error = self.kargs.get("format_error")
        if format_error is None:
            return ""
        return f"\n\n[Format errors]\n{format_error}"

    @property
    def filepath_message(self) -> str:
        filepath = self.kargs.get("filepath")
        if filepath is None:
            return ""
        return f"\n\n[File]\n{TABULATION}{filepath}"

    def __str__(self) -> str:
        return (
            "Wrong default values file format."
            f"{self.filepath_message}{self.format_error_message}"
        )


class WrongCollectionsToRenderFileFormatError(Exception):
    """
    Raised when a collections to render file has a wrong format.

    Additionnal arguments:
        filepath (str): The path to the wrong file
        format_error (str): The error message from typeguard library
    """

    def __init__(self, **kargs) -> None:
        self.kargs = kargs
        super().__init__()

    @property
    def format_error_message(self) -> str:
        format_error = self.kargs.get("format_error")
        if format_error is None:
            return ""
        return f"\n\n[Format errors]{format_error}"

    @property
    def filepath_message(self) -> str:
        filepath = self.kargs.get("filepath")
        if filepath is None:
            return ""
        return f"\n\n[File]\n{TABULATION}{filepath}"

    def __str__(self) -> str:
        return (
            "Wrong collections to render file format."
            f"{self.filepath_message}{self.format_error_message}"
        )


class WrongCollectionMonitorFormatError(Exception):
    """
    Raised when a monitor inside a collection file has a wrong format. If
        a filepath is provided, the error message will also contain the line
        number of the monitor in the file.

        Additionnal arguments:
            filepath (str): The path to the file containing the monitor
            format_error (str): The error message from typeguard library
        Args:
            monitor (Monitor): The monitor that has a wrong format
    """

    def __init__(self, wrong_value: OrderedDict, **kargs) -> None:
        self.monitor = wrong_value
        self.kargs = kargs
        super().__init__()

    @property
    def filepath_message(self) -> str:
        filepath = self.kargs.get("filepath")
        if filepath is None:
            return ""
        return f"\n\n[File]\n{TABULATION}{filepath}"

    @property
    def format_error_message(self) -> str:
        format_error = self.kargs.get("format_error")
        if format_error is None:
            return ""
        format_error = TABULATION + format_error.replace("\n", f"\n{TABULATION}")
        return f"\n\n[Format errors]\n{format_error}"

    @property
    def file_line_error_message(self) -> str:
        filepath = self.kargs.get("filepath")
        if not filepath:
            return ""

        monitor_identifier = self.monitor.get(COLLECTION_MONITOR_IDENTIFIER_KEY)
        if not monitor_identifier:
            return ""

        with open(filepath, encoding="utf-8") as datasetsfile:
            for index, line in enumerate(datasetsfile):
                if monitor_identifier in line:
                    return f", line {index + 1}"
        return ""

    @property
    def merged_monitor(self) -> str:
        yaml_as_string = get_dict_as_yaml_string(self.monitor)
        yaml_as_string = TABULATION + yaml_as_string.replace("\n", f"\n{TABULATION}")
        return f"\n\n[Merged monitor]\n{yaml_as_string}"

    def __str__(self) -> str:
        return (
            f"Wrong format for monitor after merging with default values."
            f"{self.merged_monitor}"
            f"{self.format_error_message}"
            f"{self.filepath_message}"
            f"{self.file_line_error_message}"
        )


ERRORS = {
    "CollectionMonitorsFileDict": WrongCollectionMonitorsFileFormatError,
    "CollectionMonitorDict": WrongCollectionMonitorFormatError,
    "CollectionDefaultValuesFileDict": WrongCollectionDefaultValuesFileFormatError,
    "CollectionsToRenderFileDict": WrongCollectionsToRenderFileFormatError,
}


def check_data_structure(dic_to_check: dict, expected_type: t.Type[T], **kargs) -> T:
    """
    Checks that the monitor or file has the expected structure.
    If not, raises a corresponding error. This helps debugging the yaml files.
    Returns the dictionary if it has the expected structure, with a correct type hint
    for static type checking.

    Args:
        dic_to_check (dict): the dictionary to check
        expected_type (t.Type[T]): the Class the dictionary should be an instance of.

    Additionnal arguments:
        filepath (str): The path to the file containing `dic_to_check`
    """

    errors = check_structure_and_type(dic_to_check, expected_type)
    if errors:
        kargs["wrong_value"] = dic_to_check
        kargs["format_error"] = "- " + "\n- ".join(errors)
        raise ERRORS[expected_type.__name__](**kargs)

    return dic_to_check  # type: ignore
