import argparse
from sifflet.renderer.commands import render_monitors, add_monitor, create_collection
from sifflet.utils import print_error


COMMANDS = {"render": render_monitors, "add": add_monitor, "create": create_collection}

COMMANDS_DESCRIPTION = argparse.ArgumentParser(
    description="Project aiming at generating monitors at scale."
)
subparsers = COMMANDS_DESCRIPTION.add_subparsers(dest="command", required=True)

render_parser = subparsers.add_parser("render", help="Run the project")
render_parser.add_argument(
    "collections_yaml_file", type=str, help="The name of the file to render."
)

add_parser = subparsers.add_parser("add", help="Add a monitor to a dataset")
add_parser.add_argument(
    "collection_root",
    type=str,
    help="The path to the collection where the monitor is added.",
)
add_parser.add_argument(
    "--dataset", type=str, help="Dataset to which a monitor is added.", required=True
)
add_parser.add_argument(
    "--template", type=str, help="Template to use for generating the monitor."
)
add_parser.add_argument(
    "--env",
    nargs="*",
    type=str,
    help="Environment variables for generating monitors in the format var=value.",
)
add_parser.add_argument(
    "--collections_file",
    type=str,
    help="The name of the file containing the first-level collections.",
)
add_parser.add_argument(
    "--update_monitor",
    action="store_true",
    help="Replace the monitor if it already exists in the collection",
)
create_parser = subparsers.add_parser("create", help="Create a new collection")

create_parser.add_argument(
    "collection_root",
    type=str,
    help="The path to the collection where the monitor is added, in the format path.to.collection",
)


def parse_environment_variables(env_list):
    """Convert a list of strings in format 'key=value' to a dictionary."""
    env_dict = {}
    for item in env_list:
        key, value = item.split("=")
        env_dict[key] = value
    return env_dict


def run_command_from_args(args: argparse.Namespace) -> None:
    kwargs = vars(args)
    command = kwargs.pop("command")
    if command in COMMANDS:
        if command == "add" and args.env:
            # Convert the env list to a dictionary
            kwargs["env"] = parse_environment_variables(args.env)
        try:
            COMMANDS[command](**kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            print_error(exc)

    else:
        raise NotImplementedError(f"Command {args.command} is not implemented.")
