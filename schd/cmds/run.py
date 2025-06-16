import logging
from schd.cmds.base import CommandBase
from schd.scheduler import build_job, read_config, JobContext


def run_job(config_file, job_name):
    config = read_config(config_file)

    job_config = config.jobs[job_name]

    job = build_job(job_name, job_config.cls, job_config)
    job_context = JobContext(job_name)
    job_context.output_to_console = True
    ret = job.execute(context=job_context)
    print(ret)


class RunCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('job')
        parser.add_argument('--config', '-c')

    def run(self, args):
        logging.basicConfig(format='%(asctime)s %(name)s - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
        job_name = args.job
        config_file = args.config
        run_job(config_file, job_name)
