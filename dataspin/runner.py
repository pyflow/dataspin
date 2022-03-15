from threading import BoundedSemaphore, Event
import subprocess
import os
import sys
import time
import tempfile
from .utils.tail import FileTail

class JobRunner:
    pass


class ProcessJobRunner(JobRunner):
    def __init__(self, **kwargs):
        super(ProcessJobRunner, self).__init__()
        self.max_process_count = kwargs.get('process_count') or os.cpu_count()*2
        self.semaphore = BoundedSemaphore(self.max_process_count)
        self.close_event = Event()
        self.runners = []


    def run(self, project_path, dataprocess_name):
        cmd_args = [sys.executable, '-m', 'dataspin', 'run-process', project_path, dataprocess_name]
        self.semaphore.acquire()
        fd, logfile = tempfile.mkstemp()
        # logfile_obj = open(logfile, 'ab')
        p = subprocess.Popen(cmd_args, stdout=fd, stderr=fd)
        self.runners.append((p, FileTail(logfile)))

    def manage_loop(self, empty_exit=False):
        while not self.close_event.is_set():
            for p in self.runners:
                p[0].poll()
            running_runners = list(filter(lambda x:x[0].returncode == None, self.runners))
            num = len(self.runners) - len(running_runners)
            if num > 0:
                for i in range(num):
                    self.semaphore.release()
            terminated_runner = set(self.runners) - set(running_runners)
            for runner, logfile in terminated_runner:
                sys.stdout.write(''.join(logfile.tail()))
            self.runners = running_runners
            if empty_exit and len(self.runners) == 0:
                break
            time.sleep(1)

    def close(self):
        for p, _ in self.runners:
            p.terminate()
        self.close_event.set()