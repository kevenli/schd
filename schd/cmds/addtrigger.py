import asyncio
import logging
import sys
from .base import CommandBase
from schd.schedulers.remote import RemoteApiClient
from schd.config import SchdConfig


class AddTriggerCommand(CommandBase):
    def add_arguments(self, parser):
        parser.add_argument('--base-url', type=str, required=False, help='Base URL of the remote scheduler API')
        parser.add_argument('--worker-name', type=str, required=False, help='Name of the worker to register')
        parser.add_argument('--job-name', type=str, required=True, help='Name of the job to register')
        parser.add_argument('--on-worker-name', type=str, required=False, help='Name of the worker to register')
        parser.add_argument('--on-job-name', type=str, required=True, help='Name of the job to register')
        parser.add_argument('--on-job-status', type=str, required=True, help='Status of the job to register')

    def run(self, args, config:SchdConfig=None):
        remote_url = args.base_url or config.scheduler_remote_host
        client = RemoteApiClient(remote_url)
        worker_name = args.worker_name or config.worker_name
        job_name = args.job_name
        # if on_worker_name is not provided, use current worker name
        on_worker_name = args.on_worker_name or worker_name
        on_job_name = args.on_job_name
        on_job_status = args.on_job_status
        asyncio.run(client.add_trigger(
            worker_name=worker_name,
            job_name=job_name,
            on_worker_name=on_worker_name,
            on_job_name=on_job_name,
            on_job_status=on_job_status,
        ))
        logging.info(f"Trigger added successfully for job '{job_name}'")
