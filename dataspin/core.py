
import os
import re
from dataspin.providers import get_provider_class, get_provider
from dataspin.providers.local import LocalStorageProvider
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

    @property
    def provider(self):
        return self._provider


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
        self._task_list = []
        self._load()
        
    def _load(self):
        for task in self.conf.processes:
            function_type = task.function
            function = get_function_class(function_type, task)
            self._task_list.append(function)


    def run(self):
        pass

    @property
    def name(self):
        return self._name

    @property
    def source(self):
        return self._source

    @property
    def task_list(self):
        return self._task_list


class SpinEngine:
    def __init__(self, conf):
        self.conf = conf
        self.runner_pool = Pool(4)
        self.streams = {}
        self.storages = {}
        self.data_processes = {}
        self.load()
        self.uuid = 'project_' + uuid_generator()
        self.temp_dir_path = os.path.join(os.getcwd(), self.uuid)
    
    def load(self):
        conf = self.conf
        for stream in conf.streams:
            self.streams[stream.name] = DataStream(stream)
        
        for storage in conf.storages:
            self.storages[storage.name] = ObjectStorage(storage)
        
        for process_conf in conf.data_processes:
            data_process = DataProcess(process_conf)
            self.data_processes[process_conf.name] = data_process
            for task in data_process.task_list:
                if task.type == 'save':
                    task.set_storage(self.storages)

    
    def _run_process(self, process):
        process_uuid = 'process_' + uuid_generator()
        source = self.streams.get(process.source)
        process_temp_dir_list = []
        delete_temp_path_list = []
        if not source:
            return

        for absolute_path_list in source.provider.stream():
            data_list = LocalStorageProvider.read_path_list(absolute_path_list)
            last_task_name = 'source'
            for task in process.task_list:
                process_temp_dir_list = task.process(delete_temp_path_list, os.path.join(self.temp_dir_path, process_uuid), last_task_name, process_temp_dir_list=process_temp_dir_list, data_list=data_list)
                last_task_name = task.name

            import time
            time.sleep(5)
            for path in delete_temp_path_list:
                LocalStorageProvider.delete(path)

    def run(self):
        for process_name, process in self.data_processes.items():
            self._run_process(process)
    
    # def run_process(self, process):
    #     self.runner_pool.apply_async(process.run)
    
    def join(self):
        self.runner_pool.join()


