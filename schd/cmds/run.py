import asyncio
import logging
from schd.cmds.base import CommandBase
from schd.scheduler import LocalScheduler, build_job, read_config


async def run_job(config_file, job_name):
    config = read_config(config_file)
    scheduler = LocalScheduler(config)

    job_config = config.jobs[job_name]

    job = build_job(job_name, job_config.cls, job_config)
    await scheduler.add_job(job, job_name, job_config)
    scheduler.execute_job(job_name)


class RunCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('job')
        parser.add_argument('--config', '-c')

    def run(self, args):
        logging.basicConfig(format='%(asctime)s %(name)s - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
        job_name = args.job
        config_file = args.config
        asyncio.run(run_job(config_file, job_name))
