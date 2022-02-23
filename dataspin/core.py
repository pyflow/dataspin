import os
import re
import shutil
from typing import Optional
import datetime
import tempfile
import importlib
import json
from boltons.fileutils import atomic_save

from dataspin.providers import get_provider_class, get_provider
from dataspin.utils.common import uuid_generator, marshal
from dataspin.functions import creat_function_with
from dataspin.meta import ContextMetaData, TaskMetaData, FileMetaData, factory
from dataspin.utils.file import DataFileReader
from multiprocessing import Process, Pool
from basepy.log import logger


class DataSource:
    @classmethod
    def load(cls, conf):
        name = conf.name
        source_type = conf.source_url.split("::")
        args = conf.args
        if source_type[0] == 'pyclass':
            return PyClassSource(name, source_type[1], source_type[2], args)
        else:
            return cls(name=name)

    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def fetch(self, fetch_args, context):
        datasets = self._do_fetch(fetch_args)
        data_files = []
        for key in datasets.keys():
            file_path = os.path.join(context.temp_dir, f"source_{key}.jsonl")
            with atomic_save(file_path, text_mode=False) as fo:
                for data in datasets[key]:
                    line = "{}\n".format(marshal(data))
                    fo.write(line.encode('utf-8'))
            data_files.append(context.create_data_file(file_path=file_path))
        return DataFileStream(data_files=data_files)


class PyClassSource(DataSource):
    def __init__(self, name, module, cls_name, args=None):
        self.name = name
        self.module = module
        self.cls_name = cls_name
        self.source_cls = None
        self.args = args if args is not None else {}

    def get_source_cls(self):
        if self.source_cls is None:
            module = importlib.import_module(self.module)
            self.source_cls = getattr(module, self.cls_name)
        return self.source_cls

    def _do_fetch(self, fetch_args):
        source_cls = self.get_source_cls()(**self.args)
        return source_cls.fetch(**fetch_args)


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

    def recover(self, processed_files, processing_files):
        self.provider.recover(processed_files, processing_files)

    def get_nowait(self):
        return self.get(block=False)

    def task_done(self, context):
        return self.provider.task_done(context.data_file.file_path)


class DataFileStream:
    def __init__(self, data_files):
        self._name = "data_file_stream"
        self.data_files = data_files
        self.processing_data_files = []

    @property
    def name(self):
        return self._name

    def get(self, context, block=True, timeout=None):
        if not self.data_files:
            return None
        data_file = self.data_files.pop(0)
        if data_file:
            context.init_data_files([data_file])
            return context
        return None

    def get_nowait(self):
        return self.get(block=False)

    def task_done(self, context):
        data_file = context.data_file
        if data_file in self.processing_data_files:
            self.processing_data_files.remove(data_file)


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
        self.file_type = file_type  # table or index
        self.file_format = "jsonl"  # can be jsonl, parquet
        self.generation_time = datetime.datetime.now()

    @property
    def basename(self):
        return '{}{}'.format(self.name, self.ext)

    @property
    def data(self):
        return {'name': self.name,
                'ext': self.ext,
                'file_path': self.file_path,
                'file_type': self.file_type,
                'file_format': self.file_format,
                'generation_time': str(self.generation_time)}

    @classmethod
    def load(cls, meta_data):
        return DataFile(file_path=meta_data.file_path)


class DataTaskContext:
    def __init__(self, run_id, temp_dir, **kwargs):
        self.run_id = run_id
        self.temp_dir = temp_dir
        self.end_flag = False
        self.engine = kwargs['engine']
        self.task_order = 0
        self.meta = ContextMetaData.load(run_id, temp_dir) if kwargs.get('meta_data') is None else kwargs['meta_data']

    @property
    def data_file(self):
        source_task_meta = self.meta.task_meta[0] if len(self.meta.task_meta) > 0 else None
        if source_task_meta:
            return DataFile.load(source_task_meta.output_files[0]) if len(source_task_meta.output_files) > 0 else None
        else:
            return None

    @property
    def final_file(self):
        task_meta = self.meta.task_meta[-1] if len(self.meta.task_meta) > 0 else None
        if task_meta:
            return DataFile.load(task_meta.output_files[0]) if len(task_meta.output_files) > 0 else None
        else:
            return None

    @property
    def data_files(self):
        return [DataFile.load(meta_data) for meta_data in self.meta.task_meta[0].output_files] if len(self.meta.task_meta) > 0 else []

    @property
    def final_files(self):
        return [DataFile.load(meta_data) for meta_data in self.meta.task_meta[-1].output_files] if len(self.meta.task_meta) > 0 else []

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
        source_task_meta = TaskMetaData.load(function='source')
        source_task_meta.success_flag = True
        source_task_meta.input_files.extend([FileMetaData.load(data_file.data) for data_file in data_files])
        source_task_meta.output_files.extend([FileMetaData.load(data_file.data) for data_file in data_files])
        self.meta.task_meta.append(source_task_meta)

    def create_data_file(self, file_path, file_type="table", data_format="jsonl"):
        datafile = DataFile(file_path=file_path, file_type=file_type)
        datafile.data_format = data_format
        return datafile

    def get_storage(self, name):
        return self.engine.storages.get(name)

    def get_stream(self, name):
        return self.engine.streams.get(name)

    def create_task_meta(self, function, input_files=[]):
        self.task_order += 1
        task_meta = TaskMetaData.load(function=function)
        task_meta.task_order = self.task_order
        task_meta.input_files = [FileMetaData.load(data_file.data) for data_file in input_files]
        self.meta.task_meta.append(task_meta)
        return task_meta

    def end(self):
        self.end_flag = True
        self.meta.end_time = datetime.datetime.now()
        self.meta.success_flag = True

    def meta_save(self, dst_path, temporary=False):
        serialized_meta = factory.dump(self.meta)
        logger.debug(str(serialized_meta))
        temporary_path = os.path.join(dst_path, '_temp')
        if temporary:
            os.makedirs(temporary_path, exist_ok=True)
            file_path = os.path.join(temporary_path, 'meta_data.jsonl')
            with open(file_path, 'w') as f:
                f.write(json.dumps(serialized_meta))
                f.writelines('\n')
        else:
            file_path = os.path.join(dst_path, 'meta_data.jsonl')
            with open(file_path, 'a') as f:
                f.write(json.dumps(serialized_meta))
                f.writelines('\n')
            shutil.rmtree(temporary_path, ignore_errors=True)

    @classmethod
    def recover_context(cls, context_meta_data, **kwargs):
        if context_meta_data:
            run_id = context_meta_data.run_id
            temp_dir = context_meta_data.temp_dir
            return DataTaskContext(run_id, temp_dir, engine=kwargs['engine'], meta_data=context_meta_data)
        return None


class DataProcess:
    def __init__(self, conf, engine):
        self.conf = conf
        self._name = conf.name
        self._source = conf.source
        self._fetch_args = conf.fetch_args
        self.engine = engine
        self._task_list = []
        self._load()

    def _load(self):
        for proc in self.conf.processes:
            function_name = proc.function
            function = creat_function_with(function_name, proc.args)
            self._task_list.append(function)

    def recover(self, stream, **kwargs):
        meta_dir = os.path.join(self.engine.working_dir, 'meta/meta_data.jsonl')
        meta_file_reader = DataFileReader(meta_dir)
        processed_data_files = []
        for (meta_data, meta_data_line) in meta_file_reader.readlines():
            ctx_meta = ContextMetaData.load(meta_data=meta_data)
            for data_file in ctx_meta.task_meta[0].output_files:
                processed_data_files.append(data_file.file_path)

        failed_meta_dir = os.path.join(self.engine.working_dir, 'meta/_temp/meta_data.jsonl')
        failed_meta_file_reader = DataFileReader(failed_meta_dir)
        processing_data_files = []
        for (meta_data, meta_data_line) in failed_meta_file_reader.readlines():
            ctx_meta = ContextMetaData.load(meta_data=meta_data)
            processing_ctx_meta_data = ctx_meta
            for data_file in ctx_meta.task_meta[0].output_files:
                processing_data_files.append(data_file.file_path)

        stream.recover(processed_data_files, processing_data_files)

        return DataTaskContext.recover_context(processing_ctx_meta_data, engine=kwargs.get('engine'))

    def run(self, recover):
        def append_or_extend(datafiles, newfile):
            if not newfile:
                return
            if isinstance(newfile, (list, tuple)):
                datafiles.extend(newfile)
            else:
                datafiles.append(newfile)

        run_id = uuid_generator('PR')
        temp_dir = os.path.join(self.engine.working_dir, run_id)
        meta_temp_dir = os.path.join(self.engine.working_dir, 'meta')

        if self._source in self.engine.sources:
            data_source = self.engine.sources.get(self._source)
            context = DataTaskContext(run_id, temp_dir, engine=self.engine)
            stream = data_source.fetch(self._fetch_args, context)
        elif self._source in self.engine.streams:
            stream = self.engine.streams.get(self._source)
        else:
            raise Exception(f'source {self._source} is not specified.')

        if recover:
            context = self.recover(stream, engine=self.engine)
            if context:
                run_id = context.run_id
                temp_dir = context.temp_dir

        os.makedirs(temp_dir, exist_ok=True)
        os.makedirs(meta_temp_dir, exist_ok=True)

        while True:
            if recover is False:
                context = DataTaskContext(run_id, temp_dir, engine=self.engine)
            else:
                recover = False

            if stream.get(context) is None:
                break
            if context.eof:
                break
            context.meta_save(meta_temp_dir, temporary=True)
            logger.debug('handle task of source.', source_file=context.data_file.basename)
            task_order = context.meta.task_meta[-1].task_order
            for task in self.task_list[task_order:]:
                logger.debug('handle task', task_name=task.name, task=task, data_file=context.final_file)
                new_data_files = []
                if context.single_file:
                    new_data_file = task.process(context.final_file, context)
                    append_or_extend(new_data_files, new_data_file)
                elif context.multi_files:
                    if hasattr(task, 'process_multi'):
                        new_data_file = task.process_multi(context.final_files, context)
                        append_or_extend(new_data_files, new_data_file)
                    else:
                        for data_file in context.final_files:
                            new_data_file = task.process(data_file, context)
                            append_or_extend(new_data_files, new_data_file)
                task_meta = context.create_task_meta(task.name, context.final_files)
                task_meta.end_time = datetime.datetime.now()
                task_meta.success_flag = True
                task_meta.output_files = [FileMetaData.load(data_file.data) for data_file in new_data_files]
                context.meta_save(meta_temp_dir, temporary=True)
            stream.task_done(context)
            context.end()
            context.meta_save(meta_temp_dir)

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
        self.sources = {}
        self.streams = {}
        self.storages = {}
        self.data_processes = {}
        self.load()
        self.uuid = 'project_' + uuid_generator('SE')
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
        for source in conf.sources:
            self.sources[source.name] = DataSource.load(source)

        for stream in conf.streams:
            self.streams[stream.name] = DataStream(stream)

        for storage in conf.storages:
            self.storages[storage.name] = ObjectStorage(storage)

        for process_conf in conf.data_processes:
            data_process = DataProcess(process_conf, self)
            self.data_processes[process_conf.name] = data_process

    def run(self, recover=False):
        for process_name, process in self.data_processes.items():
            self.run_process(process, recover)

    def run_process(self, process, recover):
        process.run(recover)
        # self.runner_pool.apply_async(process.run)

    def join(self):
        self.runner_pool.join()
