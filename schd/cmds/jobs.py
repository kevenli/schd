"""
list jobs
"""
import sys
from .base import CommandBase


class JobsCommand(CommandBase):
    def add_arguments(self, parser):
        # parser.add_argument('--config', '-c', default=None, help='config file')
        pass

    def run(self, args, config=None):
        if config is None:
            print("No configuration provided.")
            sys.exit(1)

        for job_name, _ in config.jobs.items():
            print(job_name)
