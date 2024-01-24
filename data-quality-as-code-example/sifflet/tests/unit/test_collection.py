# pylint: disable=redefined-outer-name

import os
from unittest.mock import Mock, patch

import pytest
from sifflet.collection_objects.collection import Collection
from sifflet.collection_objects.errors.classes import (
    WrongCollectionMonitorsFileFormatError,
)
from sifflet.tests.settings import TEST_FOLDER

TEST_COLLECTION = os.path.join(TEST_FOLDER, "render_monitors/collections/collection_2")


@pytest.fixture
def mock_database():
    return Mock()


@pytest.fixture
def mock_collection(mock_database) -> Collection:
    return Collection(TEST_COLLECTION, mock_database)


def test_check_monitors_unicity_pass(mock_collection: Collection) -> None:
    mock_collection.monitors = [Mock(name="monitor1"), Mock(name="monitor2")]  # type: ignore
    mock_collection.check_monitors_unicity()


def test_check_monitors_unicity_fail(mock_collection: Collection) -> None:
    mock_monitor1 = Mock(name="monitor1")
    mock_monitor1.__str__ = Mock(return_value="monitor1")
    mock_monitor2 = Mock(name="monitor1")
    mock_monitor2.__str__ = Mock(return_value="monitor1")
    mock_collection.monitors = [mock_monitor1, mock_monitor2]  # type: ignore
    with pytest.raises(ValueError):
        mock_collection.check_monitors_unicity()


def test_get_default_values_no_parent_no_file(mock_collection: Collection):
    with patch("os.path.exists", return_value=False):
        result = mock_collection.get_default_values(None)
    assert result == {}


def test_get_default_values_with_file(mock_collection: Collection):
    with patch("os.path.exists", return_value=True):
        with patch(
            "sifflet.collection_objects.collection.read_yaml_file",
            return_value={"key": "value"},
        ):
            result = mock_collection.get_default_values(None)
    assert result == {"key": "value"}


def test_get_default_values_with_file_and_parent_collection(
    mock_collection: Collection,
):
    mock_parent_collection = Mock(
        collection_root="root",
        default_values={"key": "parent_value", "key_parent": "value"},
    )

    with patch("os.path.exists", return_value=True):
        with patch(
            "sifflet.collection_objects.collection.read_yaml_file",
            return_value={"key": "child_value", "key_child": "value"},
        ):
            result = mock_collection.get_default_values(mock_parent_collection)
    assert result == {
        "key": "child_value",
        "key_parent": "value",
        "key_child": "value",
    }


def test_check_files_format_valid_format():
    """
    Tests that reading files in a collection checks that files format are valid,
    i.e. that they can contain "datasets" and "defaul_values" keys .
    """
    mock_collection: Collection = Mock()
    mock_file = Mock()
    with patch(
        "sifflet.collection_objects.collection.read_yaml_file",
        return_value={
            "datasets": [{"dataset": "aa", "monitors": [{}]}],
            "default_values": {},
        },
    ):
        # Assuming that this is a valid config
        result = Collection.check_files_format(mock_collection, [mock_file])
    assert result == [
        {"datasets": [{"dataset": "aa", "monitors": [{}]}], "default_values": {}}
    ]


def test_check_files_format_invalid_format():
    mock_collection = Mock()
    mock_file = Mock()
    with patch(
        "sifflet.collection_objects.collection.read_yaml_file",
        return_value={"invalid": "config"},
    ):
        with pytest.raises(WrongCollectionMonitorsFileFormatError):
            Collection.check_files_format(mock_collection, [mock_file])
