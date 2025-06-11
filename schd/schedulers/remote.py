import asyncio
from contextlib import redirect_stdout
import io
import json
import os
from typing import Dict
from urllib.parse import urljoin
import aiohttp
import aiohttp.client_exceptions
from schd.job import JobContext, Job

import logging

logger = logging.getLogger(__name__)


class RemoteApiClient:
    def __init__(self, base_url:str):
        self._base_url = base_url

    async def register_worker(self, name:str):
        url = urljoin(self._base_url, f'/api/workers/{name}')
        async with aiohttp.ClientSession() as session:
            async with session.put(url) as response:
                response.raise_for_status()
                result = await response.json()

    async def register_job(self, worker_name, job_name, cron):
        url = urljoin(self._base_url, f'/api/workers/{worker_name}/jobs/{job_name}')
        post_data = {
            'cron': cron,
        }
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=post_data) as response:
                response.raise_for_status()
                result = await response.json()

    async def subscribe_worker_eventstream(self, worker_name):
        url = urljoin(self._base_url, f'/api/workers/{worker_name}/eventstream')

        timeout = aiohttp.ClientTimeout(sock_read=600)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
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
                    
    async def update_job_instance(self, worker_name, job_name, job_instance_id, status, ret_code=None):
        url = urljoin(self._base_url, f'/api/workers/{worker_name}/jobs/{job_name}/{job_instance_id}')
        post_data = {'status':status}
        if ret_code is not None:
            post_data['ret_code'] = ret_code

        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=post_data) as response:
                response.raise_for_status()
                result = await response.json()

    async def commit_job_log(self, worker_name, job_name, job_instance_id, logfile_path):
        upload_url = urljoin(self._base_url, f'/api/workers/{worker_name}/jobs/{job_name}/{job_instance_id}/log')
        async with aiohttp.ClientSession() as session:
            with open(logfile_path, 'rb') as f:
                data = aiohttp.FormData()
                data.add_field('logfile', f, filename=os.path.basename(logfile_path), content_type='application/octet-stream')

                async with session.put(upload_url, data=data) as resp:
                    print("Status:", resp.status)
                    print("Response:", await resp.text())


class RemoteScheduler:
    def __init__(self, worker_name:str, remote_host:str):
        self.client = RemoteApiClient(remote_host)
        self._worker_name = worker_name
        self._jobs:"Dict[str,Job]" = {}
        self._loop_task = None
        self._loop = asyncio.get_event_loop()

    async def init(self):
        await self.client.register_worker(self._worker_name)

    async def add_job(self, job:Job, cron, job_name):
        await self.client.register_job(self._worker_name, job_name=job_name, cron=cron)
        self._jobs[job_name] = job

    async def start_main_loop(self):
        while True:
            logger.info('start_main_loop ')
            try:
                async for event in self.client.subscribe_worker_eventstream(self._worker_name):
                    print(event)
                    await self.execute_task(event['data']['job_name'], event['data']['id'])
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.info('connection lost')
            except aiohttp.client_exceptions.SocketTimeoutError:
                logger.info('SocketTimeoutError')
            except aiohttp.client_exceptions.ClientConnectorError:
                # cannot connect, try later
                logger.debug('connect failed, ClientConnectorError, try later.')
                await asyncio.sleep(10)
                continue
            except Exception as ex:
                logger.error('error in start_main_loop, %s', ex, exc_info=ex)
                break

    def start(self):
        self._loop_task = self._loop.create_task(self.start_main_loop())

    async def execute_task(self, job_name, instance_id:int):
        job = self._jobs[job_name]
        logfile_dir = f'joblog/{instance_id}'
        if not os.path.exists(logfile_dir):
            os.makedirs(logfile_dir)
        logfile_path = os.path.join(logfile_dir, 'output.txt')
        output_stream = io.FileIO(logfile_path, mode='w+')
        text_stream = io.TextIOWrapper(output_stream, encoding='utf-8')

        context = JobContext(job_name=job_name, stdout=text_stream)
        await self.client.update_job_instance(self._worker_name, job_name, instance_id, status='RUNNING')
        try:
            with redirect_stdout(text_stream):
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

        logger.info('job %s execute complete: %d, log_file: %s', job_name, ret_code, logfile_path)
        text_stream.flush()
        output_stream.flush()
        output_stream.close()
        await self.client.commit_job_log(self._worker_name, job_name, instance_id, logfile_path)
        await self.client.update_job_instance(self._worker_name, job_name, instance_id, status='COMPLETED', ret_code=ret_code)
