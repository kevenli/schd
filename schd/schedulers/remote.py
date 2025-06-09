from typing import Dict
from urllib.parse import urljoin
import aiohttp


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


class RemoteScheduler:
    def __init__(self, worker_name):
        self.client = RemoteApiClient('http://localhost:8899/')
        self._worker_name = worker_name
        self._jobs = {}

    async def init(self):
        await self.client.register_worker(self._worker_name)

    async def add_job(self, job, cron, job_name):
        await self.client.register_job(self._worker_name, job_name=job_name, cron=cron)
        self._jobs[job_name] = job

    def start(self):
        pass
