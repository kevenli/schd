import yaml
from schd.cmds.base import CommandBase
from schd.scheduler import build_job


def run_job(config_filepath, job_name):
    with open(config_filepath, 'r', encoding='utf8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    job_config = config['jobs'][job_name]

    job_class_name = job_config.pop('class')
    job_cron = job_config.pop('cron')
    job = build_job(job_name, job_class_name, job_config)
    job()


class RunCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('job')
        parser.add_argument('--config')

    def run(self, args):
        job_name = args.job
        config_filepath = args.config
        run_job(config_filepath, job_name)
