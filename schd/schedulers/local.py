from contextlib import redirect_stdout
from datetime import datetime
import logging
import socket
import threading
from time import sleep
from typing import Dict
from queue import Queue
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from schd.config import JobConfig, SchdConfig
from schd.email import EmailService
from schd.job import Job, JobContext

logger = logging.getLogger(__name__)


class JobResult:
    def __init__(self, job_name, code: int, output_filepath: str = None):
        self.job_name = job_name
        self.code = code
        self.output_filepath = output_filepath

    def get_code(self):
        return self.code

    def get_output_filepath(self):
        return self.output_filepath

    def get_job_name(self):
        return self.job_name


class JobQueue:
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self._queue = Queue()
        self._thread = None
        self._result_queue = Queue()

    def add_job(self, job_name, job):
        self._queue.put((job_name, job))

    def get_next_job(self):
        return self._queue.get()

    def run(self):
        while True:
            job_name, job_instance = self.get_next_job()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            logger.info(f'[{self.queue_name}] Starting job: {job_name} {timestamp}')
            job_log_filepath = f'logs/{job_name}_{timestamp}.log'
            try:
                with open(job_log_filepath, 'w') as log_file:
                    with redirect_stdout(log_file):
                        context = JobContext(job_name=job_name, stdout=log_file, stderr=log_file)
                        job_result = job_instance.execute(context)
                        log_file.flush()

                logger.info(f'[{self.queue_name}] Job {job_name} completed with result: {job_result}')
                if job_result is None:
                    ret_code = 0
                elif isinstance(job_result, int):
                    ret_code = job_result
                elif hasattr(job_result, 'get_code'):
                    ret_code = job_result.get_code()
                else:
                    raise ValueError('unsupported result type: %s', job_result)

                self._result_queue.put(JobResult(job_name, ret_code, job_log_filepath))
            except Exception as e:
                logger.error(f'[{self.queue_name}] Job {job_name} failed with error: {str(e)}')
                self._result_queue.put(JobResult(job_name, -1, job_log_filepath))

    def start(self):
        import threading
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()


class LocalScheduler:
    def __init__(self, config:SchdConfig, max_concurrent_jobs: int = 10):
        """
        Initialize the LocalScheduler with support for concurrent job execution.
        
        :param max_concurrent_jobs: Maximum number of jobs to run concurrently.
        """
        executors = {
            'default': ThreadPoolExecutor(max_concurrent_jobs)
        }
        self.scheduler = BlockingScheduler(executors=executors)
        self._jobs:"Dict[str, Job]" = {}
        self._queues:"Dict[str, JobQueue]" = {}
        self._job_queue_mapping:"Dict[str, str]" = {}
        self.email_service = EmailService.from_config(config.email)
        self.to_mail = config.email.to_addr
        self.worker_name = config.worker_name or socket.gethostname()
        self._result_collect_thread = None
        logger.info("LocalScheduler initialized in 'local' mode with concurrency support")

    async def init(self):
        pass

    async def add_job(self, job: Job, job_name: str, job_config:JobConfig) -> None:
        """
        Add a job to the scheduler.

        :param job: An instance of a class conforming to the Job protocol.
        :param cron_expression: A string representing the cron schedule.
        :param job_name: Optional name for the job.
        """
        
        try:
            cron_expression = job_config.cron
            cron_trigger = CronTrigger.from_crontab(cron_expression)
            self._jobs[job_name] = job
            job_queue_name = job_config.queue or 'default'
            if job_queue_name not in self._queues:
                self._queues[job_queue_name] = JobQueue(job_queue_name)
                self._queues[job_queue_name].start()
            self._job_queue_mapping[job_name] = job_queue_name

            self.scheduler.add_job(self.execute_job, cron_trigger, kwargs={'job_name':job_name})
            logger.info(f"Job '{job_name or job.__class__.__name__}' added with cron expression: {cron_expression}")
        except Exception as e:
            logger.error(f"Failed to add job '{job_name or job.__class__.__name__}': {str(e)}")
            raise

    def execute_job(self, job_name:str):
        job = self._jobs[job_name]
        job_queue_name = self._job_queue_mapping[job_name]
        self._queues[job_queue_name].add_job(job_name, job)
        logger.info(f"Job '{job_name}' added to queue '{job_queue_name}' for execution.")

    def collect_job_result(self):
        while True:
            for queue_name, job_queue in self._queues.items():
                while not job_queue._result_queue.empty():
                    job_result = job_queue._result_queue.get()
                    logger.info(f"Collected result for job '{job_result.get_job_name()}': code={job_result.get_code()}, output={job_result.get_output_filepath()}")
                    if job_result.get_code() != 0 and self.to_mail:
                        with open(job_result.get_output_filepath(), 'r') as log_file:
                            output = log_file.read()
                        self.email_service.send_mail(
                            subject=f'Job failed: {self.worker_name} - {job_result.get_job_name()}',
                            content=output,
                            to_emails=self.to_mail
                        )
            sleep(5)  # Sleep for a while before checking again

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
        self._result_collect_thread = threading.Thread(target=self.collect_job_result, daemon=True)
        self._result_collect_thread.start()
        self.scheduler.start()
