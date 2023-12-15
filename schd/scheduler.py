import argparse
import logging
import importlib
import os
import sys
from typing import Any
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
import yaml


logger = logging.getLogger(__name__)


def build_job(job_name, job_class_name, config):
    if not '.' in job_class_name:
        module = sys.modules[__name__]
        job_cls = getattr(module, job_class_name)
    else:
        module_name, cls_name = job_class_name.rsplit('.', 1)
        m = importlib.import_module(module_name)
        job_cls = getattr(m, cls_name)

    if hasattr('job_cls', 'from_settings'):
        job = job_cls.from_settings(job_name=job_name, config=config)
    else:
        job = job_cls(**config)

    return job


class CommandJob:
    def __init__(self, cmd, job_name=None):
        self.cmd = cmd
        self.logger = logging.getLogger(f'CommandJob#{job_name}')

    @classmethod
    def from_settings(cls, job_name, config):
        return cls(cmd=config['cmd'], job_name=job_name)
    
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.logger.info('Running command: %s', self.cmd)
        p = subprocess.run(self.cmd, shell=True, env=os.environ, stdout=sys.stdout)
        self.logger.info('process completed, %s', p.returncode)
        self.logger.info('process output %s', p.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logfile')
    parser.add_argument('--config', '-c')
    args = parser.parse_args()

    if args.logfile:
        log_stream = open(args.logfile, 'a')
        sys.stdout = log_stream
    else:
        log_stream = sys.stdout
    
    config_file = args.config or 'conf/schd.yaml'

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=log_stream)
    sched = BlockingScheduler(executors={'default': ThreadPoolExecutor(1)})
    with open(config_file, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    for job_name, job_config in config['jobs'].items():
        job_class_name = job_config.pop('class')
        job_cron = job_config.pop('cron')
        job = build_job(job_name, job_class_name, job_config)
        sched.add_job(job, CronTrigger.from_crontab(job_cron), id=job_name)
        logger.info('job added, %s', job_name)

    logger.info('scheduler starting.')
    sched.start()


if __name__ == '__main__':
    main()