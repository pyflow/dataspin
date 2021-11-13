
from .providers import get_provider_class, get_provider

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

class SpinEngine:
    def __init__(self, conf):
        self.conf = conf
        self.streams = []
        self.storages = []
        self.data_processes = []
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
        pass
    
