import argparse
import asyncio
from contextlib import redirect_stdout
import logging
import importlib
import io
import os
import sys
from typing import Any, Optional, Dict
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import subprocess
import tempfile
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from schd import __version__ as schd_version
from schd.schedulers.remote import RemoteScheduler
from schd.util import ensure_bool
from schd.job import Job, JobContext, JobExecutionResult
from schd.config import JobConfig, SchdConfig, read_config

logger = logging.getLogger(__name__)


class DefaultJobExecutionResult(JobExecutionResult):
    def __init__(self, code:int, log:str):
        self.code = code
        self.log = log


def build_job(job_name, job_class_name, config:JobConfig)->Job:
    if not ':' in job_class_name:
        module = sys.modules[__name__]
        job_cls = getattr(module, job_class_name)
    else:
        # format    "packagea.moduleb:ClassC"
        module_name, cls_name = job_class_name.rsplit(':', 1)
        m = importlib.import_module(module_name)
        job_cls = getattr(m, cls_name)

    if hasattr(job_cls, 'from_settings'):
        job = job_cls.from_settings(job_name=job_name, config=config)
    else:
        job = job_cls(**config.params)

    return job


class JobFailedException(Exception):
    def __init__(self, job_name, error_message, inner_ex:"Optional[Exception]"=None):
        self.job_name = job_name
        self.error_message = error_message
        self.inner_ex = inner_ex


class CommandJobFailedException(JobFailedException):
    def __init__(self, job_name, error_message, returncode, output):
        super(CommandJobFailedException, self).__init__(job_name, error_message)
        self.returncode = returncode
        self.output = output


class CommandJob:
    def __init__(self, cmd, job_name=None):
        self.cmd = cmd
        self.job_name = job_name
        self.logger = logging.getLogger(f'CommandJob#{job_name}')

    @classmethod
    def from_settings(cls, job_name=None, config=None, **kwargs):
        # compatible with old cmd field
        command = config.params.get('cmd') or config.cmd
        return cls(cmd=command, job_name=job_name)
    
    def execute(self, context:JobContext) -> int:
        process = subprocess.Popen(
            self.cmd,
            shell=True,
            env=os.environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate()
        if context.stdout:
            context.stdout.write(stdout)
            context.stdout.write(stderr)
                
        ret_code = process.wait()
        return ret_code

    
    def __call__(self, context:"Optional[JobContext]"=None, **kwds: Any) -> Any:
        output_to_console = False
        if context is not None:
            output_to_console = context.output_to_console

        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            self.logger.info('Running command: %s', self.cmd)

            if output_to_console:
                output_stream = sys.stdout
                output_stream_err = sys.stderr
            else:
                output_stream = temp_file
                output_stream_err = temp_file

            process = subprocess.Popen(self.cmd, shell=True, env=os.environ, stdout=output_stream, stderr=output_stream_err)
            process.communicate()

            temp_file.seek(0)
            output = temp_file.read()
        
            self.logger.info('process completed, %s', process.returncode)
            self.logger.info('process output: \n%s', output)

            if process.returncode != 0:
                raise CommandJobFailedException(self.job_name, "process failed.", process.returncode, output)


class JobExceptionWrapper:
    def __init__(self, job, handler):
        self.job = job
        self.handler = handler

    def __call__(self, *args, **kwds):
        try:
            self.job(*args, **kwds)
        except Exception as e:
            self.handler(e)


class EmailErrorNotifier:
    def __init__(self, from_addr, to_addr, smtp_server, smtp_port, smtp_user, smtp_password, start_tls=True, debug=False):
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.start_tls = start_tls
        self.debug=debug

    def __call__(self, ex:"Exception"):
        if isinstance(ex, JobFailedException):
            job_name = ex.job_name
            error_message = str(ex)
        else:
            job_name = "unknown"
            error_message = str(ex)

        mail_subject = f'Schd job failed. {job_name}' 
        msg = MIMEText(error_message, 'plain', 'utf8')
        msg['From'] = str(Header(self.from_addr, 'utf8'))
        msg['To'] = str(Header(self.to_addr, 'utf8'))
        msg['Subject'] = str(Header(mail_subject, 'utf8'))

        try:
            smtp = smtplib.SMTP(self.smtp_server, self.smtp_port)
            smtp.set_debuglevel(self.debug)
            if self.start_tls:
                smtp.starttls()

            smtp.login(self.smtp_user, self.smtp_password)
            smtp.sendmail(self.from_addr, self.to_addr, msg.as_string())
            smtp.quit()
            logger.info('Error mail notification sent. %s', mail_subject)
        except Exception as ex:
            logger.error('Error when sending email notification, %s', ex, exc_info=ex)


class ConsoleErrorNotifier:
    def __call__(self, e):
        print('ConsoleErrorNotifier:')
        print(e)


class LocalScheduler:
    def __init__(self, max_concurrent_jobs: int = 10):
        """
        Initialize the LocalScheduler with support for concurrent job execution.
        
        :param max_concurrent_jobs: Maximum number of jobs to run concurrently.
        """
        executors = {
            'default': ThreadPoolExecutor(max_concurrent_jobs)
        }
        self.scheduler = BlockingScheduler(executors=executors)
        self._jobs:Dict[str, Job] = {}
        logger.info("LocalScheduler initialized in 'local' mode with concurrency support")

    async def init(self):
        pass

    async def add_job(self, job: Job, cron_expression: str, job_name: str) -> None:
        """
        Add a job to the scheduler.

        :param job: An instance of a class conforming to the Job protocol.
        :param cron_expression: A string representing the cron schedule.
        :param job_name: Optional name for the job.
        """
        self._jobs[job_name] = job
        try:
            cron_trigger = CronTrigger.from_crontab(cron_expression)
            self.scheduler.add_job(self.execute_job, cron_trigger, kwargs={'job_name':job_name})
            logger.info(f"Job '{job_name or job.__class__.__name__}' added with cron expression: {cron_expression}")
        except Exception as e:
            logger.error(f"Failed to add job '{job_name or job.__class__.__name__}': {str(e)}")
            raise

    def execute_job(self, job_name:str):
        job = self._jobs[job_name]
        output_stream = io.StringIO()
        context = JobContext(job_name=job_name, stdout=output_stream)
        try:
            with redirect_stdout(output_stream):
                job_result = job.execute(context)

            if job_result is None:
                ret_code = 0
            elif isinstance(job_result, int):
                ret_code = job_result
            elif hasattr(job_result, 'get_code'):
                ret_code = job_result.get_code()
            else:
                raise ValueError('unsupported result type: %s', job_result)
            
        except Exception as ex:
            logger.exception('error when executing job, %s', ex)
            ret_code = -1

        logger.info('job %s execute complete: %d', job_name, ret_code)
        logger.info('job %s process output: \n%s', job_name, output_stream.getvalue())

    def run(self):
        """
        Start the scheduler.
        """
        try:
            logger.info("Starting LocalScheduler...")
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")

    def start(self):
        self.scheduler.start()


def build_scheduler(config:SchdConfig):
    scheduler_cls = os.environ.get('SCHD_SCHEDULER_CLS') or config.scheduler_cls
    
    if scheduler_cls == 'LocalScheduler':
        scheduler = LocalScheduler()
    elif scheduler_cls == 'RemoteScheduler':
        logger.info('scheduler_cls: %s', scheduler_cls)
        scheduler_remote_host = os.environ.get('SCHD_SCHEDULER_REMOTE_HOST') or config.scheduler_remote_host
        assert scheduler_remote_host, 'scheduler_remote_host cannot be none'
        logger.info('scheduler_remote_host: %s ', scheduler_remote_host)
        scheduler = RemoteScheduler(worker_name=config.worker_name, remote_host=scheduler_remote_host)
    else:
        raise ValueError('invalid scheduler_cls: %s' % scheduler_cls)
    return scheduler


async def run_daemon(config_file=None):
    config = read_config(config_file=config_file)
    scheduler = build_scheduler(config)
    await scheduler.init()

    if hasattr(config, 'error_notifier'):
        error_notifier_config = config['error_notifier']
        error_notifier_type = error_notifier_config.get('type', 'console')
        if error_notifier_type == 'console':
            job_error_handler = ConsoleErrorNotifier()
        elif error_notifier_type == 'email':
            smtp_server = error_notifier_config.get('smtp_server', os.environ.get('SMTP_SERVER'))
            smtp_port = int(error_notifier_config.get('smtp_port', os.environ.get('SMTP_PORT', 587)))
            smtp_starttls = ensure_bool(error_notifier_config.get('smtp_starttls', os.environ.get('SMTP_STARTTLS', 'true')))
            smtp_user = error_notifier_config.get('smtp_user', os.environ.get('SMTP_USER'))
            smtp_password = error_notifier_config.get('smtp_password', os.environ.get('SMTP_PASS'))
            if error_notifier_config.get('from_addr', os.environ.get('SMTP_FROM')):
                from_addr = error_notifier_config.get('from_addr', os.environ.get('SMTP_FROM'))
            else:
                from_addr = smtp_user

            to_addr = error_notifier_config.get('to_addr', os.environ.get('SCHD_ADMIN_EMAIL'))
            debug = error_notifier_config.get('debug', False)
            logger.info(f'using EmailErrorNotifier, smtp_server: {smtp_server}, smtp_port: {smtp_port}, debug: {debug}')
            job_error_handler = EmailErrorNotifier(from_addr, to_addr, smtp_server, smtp_port, smtp_user,
                                                   smtp_password, start_tls=smtp_starttls, debug=debug)
        else:
            raise Exception("Unknown error_notifier type: %s" % error_notifier_type)
    else:
        job_error_handler = ConsoleErrorNotifier()
        
    for job_name, job_config in config.jobs.items():
        job_class_name = job_config.cls
        job_cron = job_config.cron
        job = build_job(job_name, job_class_name, job_config)
        await scheduler.add_job(job, job_cron, job_name=job_name)
        logger.info('job added, %s', job_name)

    logger.info('scheduler starting.')
    scheduler.start()
    while True:
        await asyncio.sleep(1000)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logfile')
    parser.add_argument('--config', '-c')
    args = parser.parse_args()
    config_file = args.config

    logging.basicConfig(level=logging.DEBUG)

    print(f'starting schd, {schd_version}, config_file={config_file}')

    if args.logfile:
        log_stream = open(args.logfile, 'a', encoding='utf8')
        sys.stdout = log_stream
        sys.stderr = log_stream
    else:
        log_stream = sys.stdout

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=log_stream)
    await run_daemon(config_file)


if __name__ == '__main__':
    asyncio.run(main())
