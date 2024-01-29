import os
import unittest
from collections import OrderedDict

import pytest
from sifflet.renderer.commands import add_monitor
from sifflet.renderer.database import DatabaseManager
from sifflet.tests.settings import ADD_FOLDER, TEST_FOLDER
from sifflet.tests.utils import compare_folders, reset_collection

TEST_TEMPLATE = os.path.join(ADD_FOLDER, "templates/test_template.j2")
TEST_COLLECTIONS_FOLDER = os.path.join(ADD_FOLDER, "test_collections")
CORRECT_COLLECTIONS_FOLDER = os.path.join(ADD_FOLDER, "correct_collections")

monitor_file_content_for_test = OrderedDict(
    {
        "datasets": [
            {
                "dataset": "fcc34946-9ef5-438f-9473-99ab692cdac7",
                "monitors": [
                    {
                        "identifier": "test_identifier",
                        "name": "[DQAC] Freshness_for_fcc34946-9ef5-438f-9473-99ab692cdac7",
                        "parameters": {
                            "kind": "Freshness",
                            "timeWindow": {
                                "duration": "P1D",
                                "field": "creationTimestamp",
                            },
                        },
                    }
                ],
            }
        ]
    }
)


class FunctionalTestAddMonitor(unittest.TestCase):
    def setUp(self):
        self.test_database = DatabaseManager(
            os.path.join(TEST_FOLDER, "test_database.json")
        )
        self.test_env = OrderedDict(
            {
                "identifier": "test_identifier",
                "kind": "Freshness",
                "name": "test_name",
            }
        )
        self.collections_file = os.path.join(
            TEST_COLLECTIONS_FOLDER, "collections.yaml"
        )

    def test_add_monitor_without_file(self):
        test_collection = os.path.join(
            TEST_COLLECTIONS_FOLDER, "add_monitor_without_file"
        )
        correct_collection = os.path.join(
            CORRECT_COLLECTIONS_FOLDER, "add_monitor_without_file"
        )
        add_monitor(
            test_collection,
            "test_dataset",
            template=TEST_TEMPLATE,
            env=self.test_env,
            update_monitor=True,
            collections_file=self.collections_file,
        )
        compare_folders(self, test_collection, correct_collection)

    def test_update_monitor_without_update_monitor_flag(self):
        test_collection = os.path.join(
            TEST_COLLECTIONS_FOLDER, "update_monitor_without_update_monitor_flag"
        )
        correct_collection = os.path.join(
            CORRECT_COLLECTIONS_FOLDER, "update_monitor_without_update_monitor_flag"
        )
        with pytest.raises(ValueError):
            add_monitor(
                test_collection,
                "test_dataset",
                template=TEST_TEMPLATE,
                env=self.test_env,
                update_monitor=False,
                collections_file=self.collections_file,
            )
        compare_folders(self, test_collection, correct_collection)

    def test_update_monitor_with_update_monitor_flag(self):
        test_collection = os.path.join(
            TEST_COLLECTIONS_FOLDER, "update_monitor_with_update_monitor_flag"
        )
        correct_collection = os.path.join(
            CORRECT_COLLECTIONS_FOLDER, "update_monitor_with_update_monitor_flag"
        )
        add_monitor(
            test_collection,
            "fcc34946-9ef5-438f-9473-99ab692cdac7",
            template=TEST_TEMPLATE,
            env=self.test_env,
            update_monitor=True,
            collections_file=self.collections_file,
        )
        compare_folders(self, test_collection, correct_collection)

    def tearDown(self) -> None:
        default_values = OrderedDict(
            {
                "kind": "Monitor",
                "version": 1,
                "name": "test monitor",
                "description": "Monitors made with DQAC for test",
                "incident": {"message": "test message incident", "severity": "Low"},
            }
        )
        reset_collection(
            os.path.join(TEST_COLLECTIONS_FOLDER, "add_monitor_without_file"),
            default_values,
            {},
        )
        reset_collection(
            os.path.join(
                TEST_COLLECTIONS_FOLDER, "update_monitor_without_update_monitor_flag"
            ),
            default_values,
            {"not_to_be_updated.yaml": monitor_file_content_for_test},
        )
        reset_collection(
            os.path.join(
                TEST_COLLECTIONS_FOLDER, "update_monitor_with_update_monitor_flag"
            ),
            default_values,
            {"to_be_updated.yaml": monitor_file_content_for_test},
        )
