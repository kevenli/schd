import asyncio
import logging
import sys
from .base import CommandBase
from schd.scheduler import run_daemon
from schd import __version__  as schd_version


class DaemonCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('--config', '-c')
        parser.add_argument('--logfile')

    def run(self, args):
        config_file = args.config
        print(f'starting schd, {schd_version}, config_file={config_file}')

        if args.logfile:
            log_stream = open(args.logfile, 'a', encoding='utf8')
            sys.stdout = log_stream
            sys.stderr = log_stream
        else:
            log_stream = sys.stdout

        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=log_stream)
        asyncio.run(run_daemon(config_file))
