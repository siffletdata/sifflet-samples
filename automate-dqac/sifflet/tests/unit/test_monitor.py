from collections import OrderedDict
import unittest
import uuid

from sifflet.collection_objects import Monitor, Collection
from sifflet.collection_objects.errors.classes import WrongCollectionMonitorFormatError
from sifflet.collection_objects.settings import COLLECTION_MONITOR_IDENTIFIER_KEY


class MockCollection:
    def __init__(self, name="test_collection"):
        self.name = name
        self.collection_root = "/mock/path"

    def __str__(self):
        return self.name

    def get_monitor_uuid(self, _: Monitor) -> uuid.UUID:
        return uuid.uuid4()


class UnitTestMonitor(unittest.TestCase):
    def setUp(self):
        self.database = "test_database.json"
        self.collection: Collection = MockCollection()  # type: ignore
        self.valid_monitor_config = OrderedDict(
            {
                "kind": "Monitor",
                "identifier": "sample_monitor_identifier",
                "parameters": {
                    "kind": "Completeness",
                    "field": "some_field",
                    "schedule": "0 0 * * *",
                    "threshold": {
                        "sensitivity": "Low",
                        "bounds": "Min",
                        "kind": "Static",
                        "isMinInclusive": True,
                        "min": 0.1,
                    },
                    "whereStatement": "some_field IS NOT NULL",
                    "groupBy": {"field": "group_field"},
                    "timeWindow": {
                        "field": "time_field",
                        "duration": "1h",
                        "offset": "10m",
                    },
                    "partition": {"kind": "TimeUnitColumn", "field": "partition_field"},
                    "aggregation": {"kind": "Average", "quantile": 0.5},
                    "metrics": [{}],
                    "profiling": {"kind": "NullCount"},
                    "fieldProfiling": {"kind": "DuplicateCount"},
                    "reference": {
                        "kind": "Duration",
                        "timestamp": "2023-01-01T12:00:00Z",
                    },
                    "values": ["value1", "value2"],
                },
                "version": "1",
                "name": "Sample Monitor",
                "description": "This is a sample monitor.",
                "tags": [
                    {
                        "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                        "name": "sample_tag",
                        "kind": "Tag",
                    }
                ],
                "terms": [
                    {
                        "id": "f47ac10b-58cc-4372-a567-0e02b2c3d480",
                        "name": "sample_term",
                    }
                ],
                "incident": {
                    "severity": "Low",
                    "message": "This is a sample incident.",
                },
                "notifications": [
                    {
                        "kind": "Slack",
                        "id": "f47ac10b-58cc-4372-a567-0e02b2c3d481",
                        "name": "general",
                    }
                ],
            }
        )

        self.invalid_monitor_config = OrderedDict(
            {
                "identifier": 12345,  # This should be a string
                "parameters": {
                    "kind": "InvalidKind",  # This value doesn't match any of the allowed literals
                    "field": "some_field",
                },
                "version": "1",  # This should be an integer
                "name": "Sample Monitor",
                "kind": "InvalidMonitor",  # This value doesn't match the allowed literal
                "tags": "NotAList",  # This should be a list
                "incident": {
                    "severity": "VeryLow"  # This value doesn't match any of the allowed literals
                },
                "notifications": "NotAList",  # This should be a list
            }
        )
        self.dataset = str(uuid.uuid4())

    def test_init_with_valid_config(self):
        # also tests the function check_data_format() in the __init__ function
        monitor = Monitor(self.valid_monitor_config, self.collection, self.dataset)
        self.assertIsInstance(monitor, Monitor)

    def test_init_with_invalid_config(self):
        # also tests the function check_data_format() in the __init__ function
        with self.assertRaises(WrongCollectionMonitorFormatError):
            Monitor(self.invalid_monitor_config, self.collection, self.dataset)  # type: ignore

    def test_str_method(self):
        monitor = Monitor(self.valid_monitor_config, self.collection, self.dataset)
        expected_str = (
            f"{self.collection}."
            f"{self.valid_monitor_config[COLLECTION_MONITOR_IDENTIFIER_KEY]}"
        )
        self.assertEqual(str(monitor), expected_str)

    def test_clear_fields_for_api(self):
        monitor = Monitor(self.valid_monitor_config, self.collection, self.dataset)
        result = monitor.clear_fields_for_api()
        self.assertNotIn("setting_name", result)
        self.assertEqual(result["datasets"], [{"id": str(self.dataset)}])
