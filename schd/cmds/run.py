import asyncio
import logging
import sys
from schd.cmds.base import CommandBase
from schd.scheduler import LocalScheduler, build_job


async def run_job(config, job_name):
    scheduler = LocalScheduler(config)
    job_config = config.jobs[job_name]
    job = build_job(job_name, job_config.cls, job_config)
    await scheduler.add_job(job, job_name, job_config)
    scheduler.execute_job(job_name)


class RunCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('job')

    def run(self, args, config):
        if config is None:
            print("No configuration provided.")
            sys.exit(1)
            
        logging.basicConfig(format='%(asctime)s %(name)s - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
        job_name = args.job
        asyncio.run(run_job(config, job_name))
