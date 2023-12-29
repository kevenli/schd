import argparse
import logging
import importlib
import os
import sys
from typing import Any
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import subprocess
import tempfile
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
import yaml
from schd import __version__ as schd_version
from schd.util import ensure_bool


logger = logging.getLogger(__name__)


def build_job(job_name, job_class_name, config):
    if not '.' in job_class_name:
        module = sys.modules[__name__]
        job_cls = getattr(module, job_class_name)
    else:
        module_name, cls_name = job_class_name.rsplit('.', 1)
        m = importlib.import_module(module_name)
        job_cls = getattr(m, cls_name)

    if hasattr(job_cls, 'from_settings'):
        job = job_cls.from_settings(job_name=job_name, config=config)
    else:
        job = job_cls(**config)

    return job


class JobFailedException(Exception):
    def __init__(self, job_name, error_message, inner_ex:"Exception"=None):
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
        return cls(cmd=config['cmd'], job_name=job_name)
    
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
            self.logger.info('Running command: %s', self.cmd)
            process = subprocess.Popen(self.cmd, shell=True, env=os.environ, stdout=temp_file, stderr=temp_file)
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
            ex: "JobFailedException" = ex
            job_name = ex.job_name
            error_message = str(ex)
        else:
            job_name = "unknown"
            error_message = str(ex)

        mail_subject = f'Schd job failed. {job_name}' 
        msg = MIMEText(error_message, 'plain', 'utf8')
        msg['From'] = Header(self.from_addr)
        msg['To'] = Header(self.to_addr)
        msg['Subject'] = Header(mail_subject)

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


def read_config(config_file=None):
    if config_file is None and 'SCHD_CONFIG' in os.environ:
        config_file = os.environ['SCHD_CONFIG']

    if config_file is None:
        config_file = 'conf/schd.yaml'

    with open(config_file, 'r', encoding='utf8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    return config


def run_daemon(config_file=None):
    config = read_config(config_file=config_file)
    sched = BlockingScheduler(executors={'default': ThreadPoolExecutor(10)})

    if 'error_notifier' in config:
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
        
    for job_name, job_config in config['jobs'].items():
        job_class_name = job_config.pop('class')
        job_cron = job_config.pop('cron')
        job = build_job(job_name, job_class_name, job_config)
        job_warpped = JobExceptionWrapper(job, job_error_handler)
        sched.add_job(job_warpped, CronTrigger.from_crontab(job_cron), id=job_name, misfire_grace_time=10)
        logger.info('job added, %s', job_name)

    logger.info('scheduler starting.')
    sched.start()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--logfile')
    parser.add_argument('--config', '-c')
    args = parser.parse_args()
    config_file = args.config

    print(f'starting schd, {schd_version}, config_file={config_file}')

    if args.logfile:
        log_stream = open(args.logfile, 'a', encoding='utf8')
        sys.stdout = log_stream
        sys.stderr = log_stream
    else:
        log_stream = sys.stdout

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s - %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=log_stream)
    run_daemon(config_file)


if __name__ == '__main__':
    main()