import atexit
from functools import partial
import os
import time
import tempfile
import importlib
from boltons.fileutils import atomic_save
from dataspin.data import AppSystemData, DataFileMessage
from dataspin.model import SystemDatabase
from dataspin.pkindex.pk_index import IndexSearcher, PKIndexCache

from dataspin.providers import get_provider
from dataspin.utils import common
from dataspin.utils.file import DataFileReader
from dataspin.utils.schedule import add_schedule, run_scheduler
from dataspin.utils.common import uuid_generator, marshal, format_timestring, parse_url
from dataspin.providers import get_provider
from dataspin.utils.schedule import add_schedule, run_scheduler
from dataspin.functions import creat_function_with
from basepy.log import logger
from .project import ProjectConfig
from .runner import ProcessJobRunner


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
        message_dict = self.provider.get(block=block, timeout=timeout)
        message = DataFileMessage.unmarshal_data(message_dict)
        logger.debug('stream get function got%s' % message)
        if message:
            if message.storage_type == 'file':
                context.init_data_files([DataFile(file_path=message.path)])
            else:
                path = message.bucket + '/' + message.path
                logger.debug('fetch stream path', path=path)
                provider = context.get_storage_provider(
                    message.storage_type, path)
                context.init_data_files([DataFile(file_path=path,
                                                  tags=message.tags,
                                                  provider=provider)])
            return context
        return None

    def send_to_stream(self, file_path: str, tags=None, storage_type=None):
        storage_scheme = storage_type or 'file'
        if storage_type != 'file':
            file_path = file_path.lstrip('/')
        file_url = f'{storage_scheme}://{file_path}'
        dm = DataFileMessage(file_url=file_url, tags=tags)
        self._provider.send_message(dm.to_dict())

    def get_nowait(self):
        return self.get(block=False)

    def task_done(self, context):
        return self.provider.task_done(context.data_file)


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
        self._args = conf.args
        self._provider = get_provider(conf.url)

    @property
    def name(self):
        return self._name

    @property
    def provider(self):
        return self._provider

    def fetch_file(self, file_path):
        for fp in self._provider.fetch_file(file_path):
            yield fp

    @property
    def args(self):
        return self._args

    @property
    def storage_type(self):
        return self._provider.storage_type

    def get(self, prefix=None):
        yield self._provider.get(prefix)

    def save(self, key, local_file):
        return self.provider.save(key, local_file)

    def save_data(self, key, lines):
        return self.provider.save_data(key, lines)

    def fetch_file(self, path):
        return self.provider.fetch_file(path)


class DataView:
    field_type_mapping = {
        'string': str,
        'int': int,
        'float': float,
        'boolean': bool,
        'date': format_timestring
    }

    def __init__(self, conf):
        self.conf = conf
        self._name = conf.name
        self._table_format = conf.table_format
        self.fields = {field.name: field for field in conf.fields}


class DataFunction:
    def __init__(self, name, args):
        self._name = name
        self.args = args

    @property
    def name(self):
        return self._name


class DataFile:
    def __init__(self, file_path, file_type="table", tags=None, provider=None):
        self.name, self.ext = os.path.splitext(os.path.basename(file_path))
        if self.ext in ['.gz']:
            self.name, ext = os.path.splitext(self.name)
            self.ext = f'{ext}{self.ext}'
        self.file_path = file_path
        self.file_type = file_type  # table or index
        self.file_format = "jsonl"  # can be jsonl, parquet
        self.tags = tags
        self.provider = provider

    @property
    def basename(self):
        return '{}{}'.format(self.name, self.ext)

    def readlines(self):
        if not self.provider:
            file_reader = DataFileReader(
                file_path=self.file_path, ext=self.ext)
            for (data, line) in file_reader.readlines():
                yield data, line
        else:
            for file in self.provider.fetch_file(self.file_path):
                file_reader = DataFileReader(file=file, ext=self.ext)
                for (data, line) in file_reader.readlines():
                    yield data, line


class DataTaskContext:
    def __init__(self, run_id, temp_dir, data_files, **kwargs):
        self.run_id = run_id
        self.temp_dir = temp_dir
        self.data_files = data_files
        self.final_files = data_files
        self.end_flag = False
        self.files_history = []
        self.engine = kwargs['engine']
        self._process = kwargs['process']

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
        logger.debug('set data files,', data_files=data_files)
        self.final_files = data_files
        self.files_history.append(data_files)

    def create_data_file(self, file_path, file_type="table", data_format="jsonl", tags=None):
        datafile = DataFile(file_path=file_path,
                            file_type=file_type, tags=tags)
        datafile.data_format = data_format
        return datafile

    def get_storage(self, name):
        return self.engine.storages.get(name)

    def get_stream(self, name):
        return self.engine.streams.get(name)

    def get_data_view(self, name):
        return self.engine.data_views.get(name)

    def get_storage_provider(self, storage_type, path: str):
        logger.debug('storages_info', storages_info=self.engine.storages_info)
        for storage_info in self.engine.storages_info:
            auth_info = storage_info['auth_info']
            if auth_info['storage_type'] != storage_type:
                continue
            if path.startswith(auth_info['path']):
                return storage_info['storage']
        return None

    def is_duplicated_data(self, data: dict):
        if self._process.index_cache:
            return self._process.index_cache.is_exists(data)
        return False

    def update_pk_cache(self, data_file, index_keys):
        self._process.update_pk_cache(data_file, index_keys)


class DataProcess:
    def __init__(self, conf, engine):
        self.conf = conf
        self._name = conf.name
        self._source = conf.source
        self._source_args = conf.source_args
        self._schedules = conf.schedules
        self.engine = engine
        self.index_cache = None
        self._index_file_paths = set()
        self.is_fetch_job = self._source in self.engine.sources
        self.is_process_job = self._source in self.engine.streams
        self._task_list = []
        self._load()

    def _load(self):
        for proc in self.conf.processes:
            function_name = proc.function
            function = creat_function_with(function_name, proc.args)
            self._task_list.append(function)

    def update_pk_cache(self, data_file, index_keys):
        provider = data_file.provider
        args = provider.args
        time_window = args['time_window']
        index_pattern = args['index_pattern']
        if not self.index_cache:
            current_timestamp = int(time.time())
            duration = common.convert_time_window_to_seconds(time_window)
            start_timestamp = current_timestamp - duration
            self.index_cache = PKIndexCache(index_keys,
                                            duration,
                                            baseline_time=current_timestamp)
        index_searcher = IndexSearcher()
        files = []
        for filepath in index_searcher.select_index_files(provider,
                                                          index_pattern,
                                                          data_file.tags,
                                                          start_timestamp,
                                                          current_timestamp):
            if filepath not in self._index_file_paths:
                self._index_file_paths.add(filepath)
                files.append(DataFile(filepath, file_type='index',
                                      tags=None, provider=self._pk_storage))
        if files:
            self.index_cache.update_pk_files(files)

    def start(self, callback_fn):
        if self._schedules:
            for schedule_str in self._schedules:
                add_schedule(schedule_str, callback_fn)
        else:
            callback_fn()

    def start(self, callback_fn):
        if self._schedules:
            for schedule_str in self._schedules:
                add_schedule(schedule_str, callback_fn)
        else:
            callback_fn()

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
        if self.is_fetch_job:
            data_source = self.engine.sources.get(self._source)
            context = DataTaskContext(
                run_id, temp_dir, data_files=[], engine=self.engine)
            stream = data_source.fetch(self._source_args, context)
        elif self.is_process_job:
            stream = self.engine.streams.get(self._source)
        else:
            raise Exception(f'source {self._source} is not specified.')
        while True:
            context = DataTaskContext(
                run_id, temp_dir, data_files=[], engine=self.engine, process=self)
            if stream.get(context) == None:
                break
            if context.eof:
                break
            logger.debug('handle task of source.',
                         source_file=context.data_file.basename)
            for task in self.task_list:
                logger.debug('handle task', task_name=task.name,
                             task=task, data_file=context.final_file)
                new_data_files = []
                if context.single_file:
                    new_data_file = task.process(context.final_file, context)
                    append_or_extend(new_data_files, new_data_file)
                elif context.multi_files:
                    if hasattr(task, 'process_multi'):
                        new_data_file = task.process_multi(
                            context.final_files, context)
                        append_or_extend(new_data_files, new_data_file)
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
        self.config = {}
        self.sources = {}
        self.storages_info = []
        self.streams = {}
        self.storages = {}
        self.data_views = {}
        self.data_processes = {}
        self.stop_scheduler_event = None
        self.scheduler_thread = None
        self.load()

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
            obj = ObjectStorage(storage)
            self.storages[storage.name] = obj
            platform, path, params, options = parse_url(storage.url)
            if platform not in ["file", "local"]:
                self.storages_info.append({
                    'auth_info': {
                        'storage_type': platform,
                        'path': path.strip('/'),
                        'access_key': params['access_key'],
                        'secret_key': params['secret_key'],
                        'region': params['region']
                    },
                    'storage': obj
                })

        for data_view in conf.data_views:
            self.data_views[data_view.name] = DataView(data_view)

        for process_conf in conf.data_processes:
            data_process = DataProcess(process_conf, self)
            self.data_processes[process_conf.name] = data_process

    def run(self):
        for process_name, process in self.data_processes.items():
            process.run()

    def run_process(self, name):
        if name not in self.data_processes:
            raise Exception(f'Named {name} data process not found.')
        process = self.data_processes[name]
        process.run()

    def start(self):
        self.stop_scheduler_event, self.scheduler_thread = run_scheduler()
        for _, process in self.data_processes.items():
            process.start()


class SpinManager:
    def __init__(self):
        self.stop_scheduler_event = None
        self.scheduler_thread = None
        self.engines = {}
        self.job_runner = ProcessJobRunner()
        self.sysdata = AppSystemData()
        self.db = SystemDatabase()
        atexit.register(self.join)

    def load_one(self, project_path):
        project_path = os.path.abspath(project_path)
        if not os.path.exists(project_path):
            raise Exception(f"proejct path {project_path} not exists.")
        conf = ProjectConfig.load(project_path)
        engine = SpinEngine(conf)
        self.engines[project_path] = engine

    def load_all(self, project_dir):
        project_paths = []
        with os.scandir(os.path.abspath(project_dir)) as it:
            for entry in it:
                if (not entry.name.startswith('.') and entry.is_file()
                        and entry.name.endwith('.json')):
                    project_paths.append(entry.path)

        for project_path in project_paths:
            self.load_one(project_path)

    def start(self):
        self.stop_scheduler_event, self.scheduler_thread = run_scheduler()
        for project_path, engine in self.engines.items():
            for name, process in engine.data_processes.items():
                process.start(partial(self.run_process, project_path, name))
        self.job_runner.manage_loop()

    def run(self):
        for project_path, engine in self.engines.items():
            for name, process in engine.data_processes.items():
                # process.run()
                self.job_runner.run(project_path, name)
        self.job_runner.manage_loop(empty_exit=True)

    def run_process(self, project_path, name):
        project_path = os.path.abspath(project_path)
        engine = self.engines.get(project_path)
        if not engine:
            raise Exception(f'project {project_path} not loaded.')
        if name not in engine.data_processes:
            raise Exception(f'Named {name} data process not found.')
        process = engine.data_processes[name]
        process.run()

    def join(self):
        self.job_runner.close()
        if self.stop_scheduler_event:
            self.stop_scheduler_event.set()
            time.sleep(3)
            self.scheduler_thread.join()
