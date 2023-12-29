import argparse
import logging
import importlib
import os
import sys
from typing import Any
import subprocess
import tempfile
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
import yaml
from schd import __version__ as schd_version


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


class CommandFailedException(Exception):
    def __init__(self, returncode, output):
        self.returncode = returncode
        self.output = output


class CommandJob:
    def __init__(self, cmd, job_name=None):
        self.cmd = cmd
        self.logger = logging.getLogger(f'CommandJob#{job_name}')

    @classmethod
    def from_settings(cls, job_name, config):
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
                raise CommandFailedException(process.returncode, output)


class JobExceptionWrapper:
    def __init__(self, job, handler):
        self.job = job
        self.handler = handler

    def __call__(self, *args, **kwds):
        try:
            self.job(*args, **kwds)
        except Exception as e:
            self.handler(e)


class EmailErrorNotificator:
    def __init__(self, from_addr, to_addr, smtp_server, smtp_port, smtp_user, smtp_password, start_tls=True, debug=False):
        self.from_addr = from_addr
        self.to_addr = to_addr
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.start_tls = start_tls
        self.debug=debug

    def __call__(self, e):
        import smtplib
        from email.mime.text import MIMEText
        from email.header import Header

        msg = MIMEText(str(e), 'plain', 'utf8')
        msg['From'] = Header(self.from_addr)
        msg['To'] = Header(self.to_addr)
        msg['Subject'] = Header('Error from schd')

        smtp = smtplib.SMTP(self.smtp_server, self.smtp_port)
        smtp.set_debuglevel(self.debug)
        if self.start_tls:
            smtp.starttls()

        smtp.login(self.smtp_user, self.smtp_password)
        smtp.sendmail(self.from_addr, self.to_addr, msg.as_string())
        smtp.quit()


class ConsoleErrorNotificator:
    def __call__(self, e):
        print('ConsoleErrorNotificator')
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

    if 'error_notificator' in config:
        error_notificator_type = config['error_notificator'].get('type', 'console')
        if error_notificator_type == 'console':
            job_error_handler = ConsoleErrorNotificator()
        elif error_notificator_type == 'email':
            smtp_server = config['error_notificator'].get('smtp_server', os.environ.get('SMTP_SERVER'))
            smtp_port = int(config['error_notificator'].get('smtp_port', os.environ.get('SMTP_PORT', '587')))
            smtp_starttls = config['error_notificator'].get('smtp_starttls', os.environ.get('SMTP_STARTTLS', 'true')).lower() == 'true'
            smtp_user = config['error_notificator'].get('smtp_user', os.environ.get('SMTP_USER'))
            smtp_password = config['error_notificator'].get('smtp_password', os.environ.get('SMTP_PASS'))
            from_addr = config['error_notificator'].get('from_addr', os.environ.get('SMTP_FROM'))
            to_addr = config['error_notificator'].get('to_addr', os.environ.get('SCHD_ADMIN_EMAIL'))
            job_error_handler = EmailErrorNotificator(from_addr, to_addr, smtp_server, smtp_port, smtp_user, 
                                                      smtp_password, start_tls=smtp_starttls, debug=True)
        else:
            raise Exception("Unknown error_notificator type: %s" % error_notificator_type)
    else:
        job_error_handler = ConsoleErrorNotificator()
        
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

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=log_stream)
    run_daemon(config_file)


if __name__ == '__main__':
    main()