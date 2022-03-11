from threading import BoundedSemaphore
import subprocess
import os
import sys

class JobRunner:
    pass


class ProcessJobRunner(JobRunner):
    def __init__(self, **kwargs):
        super(ProcessJobRunner, self).__init__()
        self.max_process_count = kwargs.get('process_count') or os.cpu_count()*2
        self.semaphore = BoundedSemaphore(self.max_process_count)
        self.runners = []


    def run(self, project_path, dataprocess_name):
        cmd_args = [sys.executable, 'run-process', project_path, dataprocess_name]
        with self.semaphore.acquire():
            p = subprocess.Popen(cmd_args)
            self.runners.append(p)
