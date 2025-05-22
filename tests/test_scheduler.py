import unittest
from contextlib import redirect_stdout
import io
from schd.scheduler import LocalScheduler


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


class LocalSchedulerTest(unittest.TestCase):
    def test_add_execute(self):
        job = TestOutputJob()
        target = LocalScheduler()
        target.add_job(job, "0 1 * * *", 'test_job')
        target.execute_job("test_job")
