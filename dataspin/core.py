
from .providers import get_provider_class, get_provider
from multiprocessing import Process, Pool

class DataStream:
    def __init__(self, conf):
        self.conf = conf
        self._provider = get_provider(conf.url)
        
class ObjectStorage:
    def __init__(self, conf):
        self.conf = conf

class DataFunction:
    def __init__(self, conf):
        self.conf = conf

class DataProcess:
    def __init__(self, conf):
        self.conf = conf
        
    
    def run(self):
        pass

class SpinEngine:
    def __init__(self, conf):
        self.conf = conf
        self.streams = []
        self.storages = []
        self.data_processes = []
        self.runner_pool = Pool(4)
        self.load()
    
    def load(self):
        conf = self.conf
        for stream in conf.streams:
            self.streams.append(DataStream(stream))
        
        for storage in conf.storages:
            self.storages.append(ObjectStorage(storage))
        
        for process_conf in conf.data_processes:
            self.data_processes.append(DataProcess(process_conf))
    
    def run(self):
        for process in self.data_processes:
            self.run_process(process)
    
    def run_process(self, process):
        self.runner_pool.apply_async(process.run)
    
    def join(self):
        self.runner_pool.join()
