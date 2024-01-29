from copy import deepcopy
import unittest
from sifflet.collection_objects.types import CollectionMonitorDict
from sifflet.collection_objects.errors import check_structure_and_type

GOOD_MONITOR: CollectionMonitorDict = {
    "identifier": "is_email_monitor_on__airbyte_ab_id",
    "kind": "Monitor",
    "version": 1,
    "name": "Is Email Monitor on _airbyte_ab_id",
    "description": "The monitor fails if the selected field "
    "contains at least one row that does not have an email format.",
    "schedule": "@hourly",
    "incident": {"severity": "Low", "message": ""},
    "parameters": {
        "kind": "FieldFormat",
        "field": "_airbyte_ab_id",
        "partition": {
            "kind": "TimeUnitColumn",
            "field": "_airbyte_emitted_at",
            "interval": "PT1H",
        },
    },
    "tags": [
        {
            "id": "05fad67c-53a0-4412-8772-78336bec296d",
            "name": "tag_name",
            "kind": "Tag",
        },
        {
            "id": "05fad67c-53a0-4412-8772-78336bec296d",
            "name": "tag_name",
            "kind": "Tag",
        },
    ],
}


class TestTypedDictValidation(unittest.TestCase):
    def test_missing_key(self):
        data = deepcopy(GOOD_MONITOR)
        del data["parameters"]  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(["Missing key: parameters"], errors)

    def test_extra_key(self):
        data = deepcopy(GOOD_MONITOR)
        data["extra_key"] = "value"  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(["Extra key: extra_key"], errors)

    def test_nested_missing_key(self):
        data = deepcopy(GOOD_MONITOR)
        del data["parameters"]["kind"]  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(["Missing key: parameters.kind"], errors)

    def test_nested_extra_key(self):
        data = deepcopy(GOOD_MONITOR)
        data["parameters"]["extra_key"] = "value"  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(["Extra key: parameters.extra_key"], errors)

    def test_literal_type(self):
        data = deepcopy(GOOD_MONITOR)
        data["incident"]["severity"] = "InvalidSeverity"  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(
            errors,
            [
                "Expected one of ('Low', 'Moderate', 'High', 'Critical') at incident.severity, "
                "but got value: InvalidSeverity"
            ],
        )

    def test_mismatched_type(self):
        data = deepcopy(GOOD_MONITOR)
        data["identifier"] = 1  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(
            errors,
            ["Expected <class 'str'> at identifier, but got <class 'int'>"],
        )

    def test_correct_structure(self):
        errors = check_structure_and_type(GOOD_MONITOR, CollectionMonitorDict)
        self.assertEqual(errors, [])

    def test_list_instead_of_str(self):
        data = deepcopy(GOOD_MONITOR)
        data["name"] = ["aa", "bb", 1]  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(
            errors,
            [
                "Expected <class 'str'> at name, but got <class 'list'>",
            ],
        )

    def test_list_of_strings_with_int(self):
        data = deepcopy(GOOD_MONITOR)
        data["name"] = ["aa", "bb", 1]  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(
            errors,
            [
                "Expected <class 'str'> at name, but got <class 'list'>",
            ],
        )

    def test_list_with_wrong_inside_type(self):
        data = deepcopy(GOOD_MONITOR)
        data["tags"][0]["id"] = 1  # type: ignore
        data["tags"][1]["name"] = ["a"]  # type: ignore
        errors = check_structure_and_type(data, CollectionMonitorDict)
        self.assertEqual(
            errors,
            [
                "Expected <class 'str'> at tags[0].id, but got <class 'int'>",
                "Expected <class 'str'> at tags[1].name, but got <class 'list'>",
            ],
        )


if __name__ == "__main__":
    unittest.main()
