from collections import OrderedDict
import unittest
from sifflet.utils import merge_yaml_files


class TestMergeYamlFiles(unittest.TestCase):
    def test_merge_yaml_files(self):
        default_values = OrderedDict(
            {
                "foo": {"bar": "bar2", "bar3": "bar5"},
                "spam": "eggs",
                "test": "response",
            }
        )
        values = OrderedDict(
            {
                "foo": {"bar": "bar4", "bar2": "bar4"},
                "spam": "ham",
                "animal": {
                    "dog": "bark",
                    "cat": "meow",
                },
            }
        )
        expected_result = OrderedDict(
            {
                "foo": {"bar": "bar4", "bar2": "bar4", "bar3": "bar5"},
                "spam": "ham",
                "test": "response",
                "animal": {
                    "dog": "bark",
                    "cat": "meow",
                },
            }
        )
        result = merge_yaml_files(default_values, values)
        self.assertEqual(result, expected_result)
