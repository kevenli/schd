"""
list jobs
"""

from schd.scheduler import read_config
from .base import CommandBase


class JobsCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('--config', '-c', default=None, help='config file')

    def run(self, args):
        config = read_config(config_file=args.config)
        for job_name, _ in config.jobs.items():
            print(job_name)
