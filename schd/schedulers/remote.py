import asyncio
from contextlib import redirect_stdout
import io
import json
from typing import Dict
from urllib.parse import urljoin
import aiohttp
from schd.job import JobContext

import logging

logger = logging.getLogger(__name__)


class RemoteApiClient:
    def __init__(self, base_url:str):
        self._base_url = base_url

    async def register_worker(self, name:str):
        url = urljoin(self._base_url, '/api/workers')
        post_data = {
            'name': name,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=post_data) as response:
                result = await response.json()

    async def register_job(self, worker_name, job_name, cron):
        url = urljoin(self._base_url, f'/api/workers/{worker_name}/jobs/{job_name}')
        post_data = {
            'cron': cron,
        }
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=post_data) as response:
                result = await response.json()

    async def subscribe_worker_eventstream(self, worker_name):
        url = urljoin(self._base_url, f'/api/workers/{worker_name}/eventstream')

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                async for line in resp.content:
                    decoded = line.decode("utf-8").strip()
                    logger.info('got event, raw data: %s', decoded)
                    event = json.loads(decoded)
                    event_type = event['event_type']
                    if event_type == 'NewJobInstance':
                        # event = JobInstanceEvent()
                        yield event
                    else:
                        raise ValueError('unknown event type %s' % event_type)


class RemoteScheduler:
    def __init__(self, worker_name):
        self.client = RemoteApiClient('http://localhost:8899/')
        self._worker_name = worker_name
        self._jobs = {}
        self._loop_task = None
        self._loop = asyncio.get_event_loop()

    async def init(self):
        await self.client.register_worker(self._worker_name)

    async def add_job(self, job, cron, job_name):
        await self.client.register_job(self._worker_name, job_name=job_name, cron=cron)
        self._jobs[job_name] = job

    async def start_main_loop(self):
        while True:
            logger.info('start_main_loop ')
            async for event in self.client.subscribe_worker_eventstream(self._worker_name):
                print(event)
                self.execute_task(event['data']['job_name'], event['data']['id'])

    def start(self):
        self._loop_task = self._loop.create_task(self.start_main_loop())

    def execute_task(self, job_name, instance_id:int):
        job = self._jobs[job_name]
        context = JobContext(job_name)
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
