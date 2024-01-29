# pylint: disable=redefined-outer-name
import os

from sifflet.renderer.template_renderer import render_jinja2_template_to_dict
from sifflet.tests.settings import TEST_FOLDER

TEST_TEMPLATE = os.path.join(TEST_FOLDER, "unit/templates/test_template.j2")


def test_render_jinja2_template_to_dict():
    env_vars = {"name": "testName"}
    expected_output = {"name": "[DQAC] testName"}
    result = render_jinja2_template_to_dict(TEST_TEMPLATE, env_vars)
    assert result == expected_output
