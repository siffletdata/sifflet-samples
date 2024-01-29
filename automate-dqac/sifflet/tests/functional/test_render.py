import os
import shutil
import unittest

from sifflet.renderer.commands import render_monitors
from sifflet.renderer.database import DatabaseManager
from sifflet.tests.settings import RENDER_FOLDER, TEST_FOLDER
from sifflet.tests.utils import compare_folders

TEST_DATABASE_PATH = os.path.join(TEST_FOLDER, "test_database.json")


class FunctionalTestRender(unittest.TestCase):
    def setUp(self):
        self.test_database = DatabaseManager(TEST_DATABASE_PATH)

    def test_render_monitors(self):
        """
        Test rendering monitors from top-level collections.
        """
        rendered_folder = os.path.join(RENDER_FOLDER, "rendered_monitors")
        correct_rendered_folder = os.path.join(RENDER_FOLDER, "correct_rendered")
        test_collections_path = os.path.join(RENDER_FOLDER, "test_collections.yaml")
        render_monitors(self.test_database, rendered_folder, test_collections_path)
        compare_folders(self, rendered_folder, correct_rendered_folder)

    def test_render_monitors_from_child_collection(self):
        """
        Test rendering monitors from children collections.
        """
        rendered_folder = os.path.join(RENDER_FOLDER, "rendered_monitor_from_child")
        correct_rendered_folder = os.path.join(
            RENDER_FOLDER, "correct_rendered_monitors_from_child"
        )
        test_collections_path = os.path.join(
            RENDER_FOLDER, "test_collections_from_child.yaml"
        )
        render_monitors(self.test_database, rendered_folder, test_collections_path)
        compare_folders(self, rendered_folder, correct_rendered_folder)

    def tearDown(self):
        """
        Database is not removed to allow UUID to persist between tests
        """

        shutil.rmtree(
            os.path.join(RENDER_FOLDER, "rendered_monitors"),
            ignore_errors=True,
        )
        shutil.rmtree(
            os.path.join(RENDER_FOLDER, "rendered_monitor_from_child"),
            ignore_errors=True,
        )
