from sifflet.commands import COMMANDS_DESCRIPTION, run_command_from_args


if __name__ == "__main__":
    args = COMMANDS_DESCRIPTION.parse_args()
    run_command_from_args(args)
