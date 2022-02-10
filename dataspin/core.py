import os
import re
from typing import Optional

from dataspin.providers import get_provider_class, get_provider
from dataspin.utils.util import uuid_generator
from dataspin.utils import file_operator
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
    
    def get(self, context, block=True, timeout=None):
        file_path = self.provider.get(block=block, timeout=timeout)
        if file_path:
            context.data_files.append(DataFile(file_path=file_path))
            return context
        return None

    def get_nowait(self):
        return self.get(block=False)
    
    def task_done(self, context):
        return self.provider.task_done(context.src_file)

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

class DataFile:
    def __init__(self, file_path, file_type="table"):
        self.name = os.path.splitext(os.path.basename(file_path))[0]
        self.file_path = file_path
        self.file_type = file_type # table or index
        self.format = "jsonl" # can be jsonl, parquet
    

class DataTaskContext:
    def __init__(self, run_id, temp_dir, data_files = [],  **kwargs):
        self.run_id = run_id
        self.temp_dir = temp_dir
        self.data_files = data_files
        self.final_files = data_files
        self.end_flag = False
        self.files_history = []
    
    @property
    def valid(self):
        return len(self.final_files) > 0
    
    @property
    def single_file(self):
        return len(self.final_files) == 1
    
    @property
    def multi_files(self):
        return len(self.final_files) > 1
    
    @property
    def eof(self):
        return self.end_fl
    
    def set_data_files(self, data_files):
        self.final_files = data_files
        self.files_history.append(data_files)



class DataProcess:
    def __init__(self, conf, engine):
        self.conf = conf
        self._name = conf.name
        self._source = conf.source
        self.engine = engine
        self._task_list = []
        self._load()

    def _load(self):
        for task in self.conf.processes:
            function_type = task.function
            function = get_function_class(function_type, task)
            self._task_list.append(function)

    def run(self):
        run_id = uuid_generator('PR')
        temp_dir = os.path.join(os.getcwd(), run_id)
        
        while True:
            stream = self.engine.streams.get(self._source)
            context = DataTaskContext(run_id, temp_dir)
            stream.get(context)
            if context.eof:
                return
            for task in self.task_list:
                new_data_files = []
                if context.single_file:
                    new_data_file = task.process(context.final_files[0])
                    new_data_files.append(new_data_file)
                elif context.multi_files:
                    if hasattr(task, 'process_multi'):
                        new_data_files = task.process_multi(context.final_files)
                    else:
                        for data_file in context.final_files:
                            new_data_files.append(task.process(data_file))
                context.set_final_files(new_data_files)
            stream.task_done(context)


    @property
    def name(self):
        return self._name

    @property
    def source(self):
        return self._source

    @property
    def task_list(self):
        return self._task_list


class GeneratedObject:
    def __init__(self):
        self._process_temp_dir_dict = {}
        self._data_dict = {}

    @property
    def generated_object_type(self):
        if self._process_temp_dir_dict:
            return 'path_list'
        elif self._data_dict:
            return 'data_list'
        else:
            return 'none'

    def get_generated_object(self, type):
        if type == 'path_list':
            for key, process_temp_dir in self._process_temp_dir_dict.items():
                yield key, process_temp_dir
        if type == 'data_list':
            for key, data_list in self._data_dict.items():
                yield key, data_list

    def clear(self):
        self._process_temp_dir_dict.clear()
        self._data_dict.clear()

    def _merge_object_dict(self, source_dict, destination_dict):
        for key, process_temp_list in source_dict.items():
            if key in destination_dict:
                destination_dict[key].extend(process_temp_list)
            else:
                destination_dict[key] = process_temp_list

    def set_generated_object(self, temp_generated_list):
        for type, generated_object_dict in temp_generated_list:
            if type == 'path_list':
                self._merge_object_dict(generated_object_dict, self._process_temp_dir_dict)
            if type == 'data_list':
                self._merge_object_dict(generated_object_dict, self._data_dict)


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
            data_process = DataProcess(process_conf, self)
            self.data_processes[process_conf.name] = data_process
            for task in data_process.task_list:
                if task.type == 'save':
                    task.set_storage(self.storages)

    def _run_process(self, process):
        process_uuid = 'process_' + uuid_generator()
        source = self.streams.get(process.source)
        delete_temp_path_list = []
        if not source:
            return

        generate_object = GeneratedObject()
        for absolute_path_list in source.provider.stream():
            source_data_list = file_operator.read_path_list(absolute_path_list)
            generate_object.set_generated_object([('data_list', {'default': source_data_list})])
            for task in process.task_list:
                temp_generated_list = []
                if generate_object.generated_object_type == 'path_list':
                    for key, process_path_list in generate_object.get_generated_object('path_list'):
                        temp_generated_object = task.process(delete_temp_path_list,
                                                             os.path.join(self.temp_dir_path, process_uuid),
                                                             key,
                                                             process_temp_dir_list=process_path_list)
                        temp_generated_list.append(temp_generated_object)
                if generate_object.generated_object_type == 'data_list':
                    for key, data_list in generate_object.get_generated_object('data_list'):
                        temp_generated_object = task.process(delete_temp_path_list,
                                                             os.path.join(self.temp_dir_path, process_uuid),
                                                             key,
                                                             data_list=data_list)
                        temp_generated_list.append(temp_generated_object)
                generate_object.clear()
                generate_object.set_generated_object(temp_generated_list)

            generate_object.clear()

            import time
            time.sleep(5)
            for path in delete_temp_path_list:
                file_operator.delete(path)

    def run(self):
        for process_name, process in self.data_processes.items():
            self._run_process(process)

    # def run_process(self, process):
    #     self.runner_pool.apply_async(process.run)

    def join(self):
        self.runner_pool.join()
