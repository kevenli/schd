import unittest
from contextlib import redirect_stdout
import io
from schd.config import JobConfig
from schd.scheduler import LocalScheduler, build_job


class TestOutputJob:
    def execute(self, context):
        print('test output')


class RedirectStdoutTest(unittest.TestCase):
    def test_redirect(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print("This goes into the buffer, not the console.")
        output = buffer.getvalue()
        # print(f"Captured: {output}")
        self.assertEqual('This goes into the buffer, not the console.\n', output)

    def test_redirect_job(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            job = TestOutputJob()
            job.execute(None)
        output = buffer.getvalue()
        self.assertEqual('test output\n', output)


class LocalSchedulerTest(unittest.IsolatedAsyncioTestCase):
    async def test_add_execute(self):
        job = TestOutputJob()
        target = LocalScheduler()
        await target.add_job(job, "0 1 * * *", 'test_job')
        target.execute_job("test_job")


class JobHasParams:
    def __init__(self, x, y):
        self.x = x
        self.y = y


class JobHasFromSettingsMethod:
    def __init__(self, job_name, z):
        self.job_name = job_name
        self.z = z
    @classmethod
    def from_settings(cls, job_name=None, config=None, **kwargs):
        return JobHasFromSettingsMethod(job_name, config.params['z'])


class TestBuildJob(unittest.TestCase):
    def test_build_no_param(self):
        job_cls = 'test_scheduler:TestOutputJob'
        built_job = build_job('TestOutputJob', job_cls, JobConfig(cls=job_cls, cron='* * * * *'))
        self.assertIsNotNone(built_job)

    def test_build_has_param(self):
        job_cls = 'test_scheduler:JobHasParams'
        built_job:JobHasParams = build_job('JobHasParams', job_cls, JobConfig(cls=job_cls, cron='* * * * *', params={'x':1,'y':2}))
        self.assertIsNotNone(built_job)
        # build_job should pass params into contrustor accordingly
        self.assertEqual(built_job.x, 1)
        self.assertEqual(built_job.y, 2)

    def test_build_from_settings(self):
        job_cls = 'test_scheduler:JobHasFromSettingsMethod'
        built_job:JobHasFromSettingsMethod = build_job('JobHasFromSettingsMethod', job_cls, JobConfig(cls=job_cls, cron='* * * * *', params={'z':3}))
        self.assertIsNotNone(built_job)
        # build_job should pass params into contrustor accordingly
        self.assertEqual(built_job.job_name, 'JobHasFromSettingsMethod')
        self.assertEqual(built_job.z, 3)
