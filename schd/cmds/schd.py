import argparse
import sys
from schd.cmds.jobs import JobsCommand
from schd.scheduler import main as scheduler_main
from .daemon import DaemonCommand
from .run import RunCommand
from schd import __version__ as schd_version

commands = {
    'daemon': DaemonCommand(),
    'run': RunCommand(),
    'jobs': JobsCommand(),
}

def main():
    sys.path.append('.')
    parser = argparse.ArgumentParser('schd')
    parser.add_argument('--version', action='store_true', default=False)
    sub_command_parsers = parser.add_subparsers(dest='cmd')

    for cmd, cmd_obj in commands.items():
        sub_command_parser = sub_command_parsers.add_parser(cmd)
        cmd_obj.add_arguments(sub_command_parser)

    args = parser.parse_args()

    if args.version:
        print('schd version ', schd_version)
        return
    
    if not args.cmd:
        parser.print_help()
        return
    
    commands[args.cmd].run(args)


if __name__ == '__main__':
    main()
