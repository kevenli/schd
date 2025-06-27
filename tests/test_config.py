import unittest
from schd.config import SchdConfig, JobConfig

class TestSchdConfig(unittest.TestCase):
    def setUp(self):
        self.config = SchdConfig(
            jobs={"job1": JobConfig(cls='', cron='* * * * *')},
            scheduler_cls="RemoteScheduler",
            scheduler_remote_host="10.0.0.1",
            worker_name="remote_worker"
        )

    def test_get_existing_field(self):
        self.assertEqual(self.config["scheduler_cls"], "RemoteScheduler")
        self.assertEqual(self.config["worker_name"], "remote_worker")
        self.assertEqual(self.config["scheduler_remote_host"], "10.0.0.1")

    def test_jobs_access(self):
        self.assertIn("job1", self.config.jobs)
        self.assertEqual(self.config.jobs['job1'].cls, '')
        # self.assertEqual(self.config["jobs"]["job1"].name, "job1")

    def test_invalid_key_raises(self):
        with self.assertRaises(KeyError):
            _ = self.config["nonexistent"]

    def test_attribute_access_still_works(self):
        self.assertEqual(self.config.scheduler_cls, "RemoteScheduler")
        self.assertEqual(self.config.worker_name, "remote_worker")