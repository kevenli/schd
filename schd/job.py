from typing import Protocol, Union


class JobExecutionResult(Protocol):
    def get_code(self) -> int:...


class JobContext:
    def __init__(self, job_name:str, logger=None, stdout=None, stderr=None):
        self.job_name = job_name
        self.logger = logger
        self.output_to_console = False
        self.stdout = stdout
        self.stderr = stderr


class Job(Protocol):
    """
    Protocol to represent a job structure.
    """
    def execute(self, context:JobContext) -> Union[JobExecutionResult, int, None]:
        """
        execute the job
        """
        pass
