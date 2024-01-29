from collections import OrderedDict
import jinja2
import yaml


def render_jinja2_template_to_dict(template_path: str, env_vars: dict) -> OrderedDict:
    """
    Render a Jinja2 template with environment variables and return a Python dictionary.

    Parameters:
        - template_path (str): Path to the Jinja2 template file.
        - env_vars (dict): Dictionary of environment variables to be used in rendering.

    Returns:
        dict: Rendered content as a Python dictionary.
    """
    # Load the template
    with open(template_path, "r", encoding="utf-8") as file:
        template_content = file.read()

    # Create a Jinja2 environment and render the template
    try:
        template = jinja2.Template(template_content)
        rendered_content = template.render(**env_vars)
    except jinja2.exceptions.TemplateSyntaxError as error:
        raise ValueError(
            f"Error rendering template {template_path}: {error}"
        ) from error

    # Convert the rendered content (in YAML format) to a Python dictionary
    return OrderedDict(yaml.safe_load(rendered_content))
