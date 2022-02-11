import os
import re
from typing import Optional
import time
import tempfile

from dataspin.providers import get_provider_class, get_provider
from dataspin.utils.common import uuid_generator
from dataspin.functions import creat_function_with
from multiprocessing import Process, Pool
from basepy.log import logger


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
        logger.debug('stream get function got', file_path=file_path)
        if file_path:
            context.init_data_files([DataFile(file_path=file_path)])
            return context
        return None

    def get_nowait(self):
        return self.get(block=False)
    
    def task_done(self, context):
        return self.provider.task_done(context.data_file)

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

    def save(self, key, local_file):
        self.provider.save(key, local_file)
    
    def save_data(self, key, lines):
        self.provider.save_data(key, lines)

class DataFunction:
    def __init__(self, name, args):
        self._name = name
        self.args = args

    @property
    def name(self):
        return self._name

class DataFile:
    def __init__(self, file_path, file_type="table"):
        self.name, self.ext = os.path.splitext(os.path.basename(file_path))
        if self.ext in ['.gz']:
            self.name, ext = os.path.splitext(self.name)
            self.ext = f'{ext}{self.ext}'
        self.file_path = file_path
        self.file_type = file_type # table or index
        self.file_format = "jsonl" # can be jsonl, parquet
    
    @property
    def basename(self):
        return '{}{}'.format(self.name, self.ext)

class DataTaskContext:
    def __init__(self, run_id, temp_dir, data_files,  **kwargs):
        self.run_id = run_id
        self.temp_dir = temp_dir
        self.data_files = data_files
        self.final_files = data_files
        self.end_flag = False
        self.files_history = []
        self.engine = kwargs['engine']
    
    @property
    def data_file(self):
        return self.data_files[0] if len(self.data_files) > 0 else None

    @property
    def final_file(self):
        return self.final_files[0] if len(self.final_files) > 0 else None

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
        return self.end_flag
    
    def init_data_files(self, data_files):
        self.data_files = data_files
        self.final_files = data_files

    def set_data_files(self, data_files):
        logger.debug('set data files,', data_files = data_files)
        self.final_files = data_files
        self.files_history.append(data_files)
    
    def create_data_file(self, file_path, file_type="table", data_format="jsonl"):
        datafile =  DataFile(file_path=file_path, file_type=file_type)
        datafile.data_format = data_format
        return datafile
    
    def get_storage(self, name):
        return self.engine.storages.get(name)

    def get_stream(self, name):
        return self.engine.streams.get(name)

class DataProcess:
    def __init__(self, conf, engine):
        self.conf = conf
        self._name = conf.name
        self._source = conf.source
        self.engine = engine
        self._task_list = []
        self._load()

    def _load(self):
        for proc in self.conf.processes:
            function_name = proc.function
            function = creat_function_with(function_name, proc.args)
            self._task_list.append(function)

    def run(self):
        def append_or_extend(datafiles, newfile):
            if not newfile:
                return
            if isinstance(newfile, (list, tuple)):
                datafiles.extend(newfile)
            else:
                datafiles.append(newfile)

        run_id = uuid_generator('PR')
        temp_dir = os.path.join(self.engine.working_dir, run_id)
        os.makedirs(temp_dir, exist_ok=True)
        stream = self.engine.streams.get(self._source)

        while True:
            context = DataTaskContext(run_id, temp_dir, data_files=[], engine=self.engine)
            if stream.get(context) == None:
                break
            if context.eof:
                break
            logger.debug('handle task of source.', source_file=context.data_file.basename)
            for task in self.task_list:
                logger.debug('handle task', task_name=task.name, task=task, data_file=context.final_file)
                new_data_files = []
                if context.single_file:
                    new_data_file = task.process(context.final_file, context)
                    append_or_extend(new_data_files, new_data_file)
                elif context.multi_files:
                    if hasattr(task, 'process_multi'):
                        new_data_files = task.process_multi(context.final_files)
                    else:
                        for data_file in context.final_files:
                            new_data_file = task.process(data_file, context)
                            append_or_extend(new_data_files, new_data_file)
                context.set_data_files(new_data_files)
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


class SpinEngine:
    def __init__(self, conf):
        self.conf = conf
        # self.runner_pool = Pool(4)
        self.config = {}
        self.streams = {}
        self.storages = {}
        self.data_processes = {}
        self.load()
        self.uuid = 'project_' + uuid_generator()
        self.temp_dir_path = os.path.join(os.getcwd(), self.uuid)
    
    @property
    def working_dir(self):
        return self.config.working_dir

    def load(self):
        conf = self.conf
        self.config = conf.dataspin
        if not self.config.working_dir:
            self.config.working_dir = tempfile.mkdtemp()
        working_dir = os.path.abspath(self.config.working_dir)
        os.makedirs(working_dir, exist_ok=True)
        self.config.working_dir = working_dir
        for stream in conf.streams:
            self.streams[stream.name] = DataStream(stream)

        for storage in conf.storages:
            self.storages[storage.name] = ObjectStorage(storage)

        for process_conf in conf.data_processes:
            data_process = DataProcess(process_conf, self)
            self.data_processes[process_conf.name] = data_process

    def run(self):
        for process_name, process in self.data_processes.items():
            self.run_process(process)

    def run_process(self, process):
        process.run()
        #self.runner_pool.apply_async(process.run)

    def join(self):
        self.runner_pool.join()
