import yaml
from schd.cmds.base import CommandBase
from schd.scheduler import build_job, read_config


def run_job(config_file, job_name):
    config = read_config(config_file)

    job_config = config['jobs'][job_name]

    job_class_name = job_config.pop('class')
    job_cron = job_config.pop('cron')
    job = build_job(job_name, job_class_name, job_config)
    job()


class RunCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('job')
        parser.add_argument('--config', '-c')

    def run(self, args):
        job_name = args.job
        config_file = args.config
        run_job(config_file, job_name)
