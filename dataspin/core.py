
import os
import re
from dataspin.providers import get_provider_class, get_provider
from dataspin.utils.util import uuid_generator
from dataspin.functions import get_function_class
from multiprocessing import Process, Pool


class DataStream:
    def __init__(self, conf):
        self.conf = conf
        self._name = conf.name
        self._provider = get_provider(conf.url)
    
    @property
    def name(self):
        return self._name

    @property
    def provider(self):
        return self._provider
        
class ObjectStorage:
    def __init__(self, conf):
        self.conf = conf
        self._name = conf.name
        self._provider = get_provider(conf.url)

    @property
    def name(self):
        return self._name


class DataFunction:
    def __init__(self, conf):
        self.conf = conf
        self._name = conf.name
        self._type = conf.function
        self._args = conf.args
        self._kv_args = conf.kv_args

    @property
    def name(self):
        return self._name

class DataProcess:
    def __init__(self, conf):
        self.conf = conf
        self._name = conf.name
        self._source = conf.source
        
    
    def run(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def source(self):
        return self._source


class SpinEngine:
    def __init__(self, conf):
        self.conf = conf
        self.runner_pool = Pool(4)
        self.streams = {}
        self.storages = {}
        self.data_processes = {}
        self.load()
        self.uuid = uuid_generator()
        self.temp_dir_path = os.path.join(os.getcwd(), self.uuid)
    
    def load(self):
        conf = self.conf
        for stream in conf.streams:
            self.streams[stream.name] = DataStream(stream)
        
        for storage in conf.storages:
            self.storages[storage.name] = ObjectStorage(storage)
        
        for process_conf in conf.data_processes:
            self.data_processes[process_conf.name] = DataProcess(process_conf)
    
    def _run_process(self, process):
        source = self.streams.get(process.source)
        process_temp_dir_list = []
        if not source:
            return
        source_temp_dir_path = os.path.join(self.temp_dir_path, 'source') + '/'
        source.provider.get(storage_path=source_temp_dir_path)
        process_temp_dir_list.append(source_temp_dir_path)
        last_task_name = 'source'
        for task in process.conf.processes:
            function_type = task.function
            function = get_function_class(function_type, task)
            process_temp_dir_list = function.process(process_temp_dir_list, self.temp_dir_path, last_task_name)
            last_task_name = function

    def run(self):
        # for process in self.data_processes:
        #     self._run_process(process)
        for process_name, process in self.data_processes.items():
            self._run_process(process)
    
    # def run_process(self, process):
    #     self.runner_pool.apply_async(process.run)
    
    def join(self):
        self.runner_pool.join()


