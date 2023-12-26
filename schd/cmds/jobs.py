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
        for job_name, job_config in config['jobs'].items():
            job_class_name = job_config.pop('class')
            job_cron = job_config.pop('cron')
            # print(f'{job_name} : {job_class_name} {job_cron}')
            print(job_name)
