from dataclasses import dataclass, field
import dataclass_factory
from datetime import datetime, timezone
from typing import List, Optional

from dataspin.utils.common import uuid_generator

factory = dataclass_factory.Factory()


@dataclass
class FileMetaData:
    name: str
    ext: str
    file_path: str
    file_type: str
    file_format: str
    generation_time: datetime
    tags: Optional[dict] = field(default_factory=dict)

    @classmethod
    def load(cls, meta_data):
        return factory.load(meta_data, cls)


@dataclass
class TaskMetaData:
    task_id: str
    function: str
    start_time: datetime
    end_time: datetime
    success_flag: bool
    task_order: int = 0
    input_files: Optional[List[FileMetaData]] = field(default_factory=list)
    output_files: Optional[List[FileMetaData]] = field(default_factory=list)

    @classmethod
    def load(cls, task_id=uuid_generator('FN'), function='pass', start_time=datetime.now(), end_time=datetime.now(),
             success_flag=False, meta_data=None):
        if meta_data:
            return factory.load(meta_data, cls)
        else:
            data = {'task_id': task_id,
                    'function': function,
                    'start_time': str(start_time),
                    'end_time': str(end_time),
                    'success_flag': success_flag}
            return factory.load(data, cls)


@dataclass
class ContextMetaData:
    run_id: str
    temp_dir: str
    start_time: datetime
    end_time: datetime
    success_flag: bool
    task_meta: Optional[List[TaskMetaData]] = field(default_factory=list)

    @classmethod
    def load(cls, run_id=uuid_generator('FN'), temp_dir='', start_time=datetime.now(), end_time=datetime.now(), success_flag=False, meta_data=None):
        if meta_data:
            return factory.load(meta_data, cls)
        else:
            data = {'run_id': run_id,
                    'temp_dir': temp_dir,
                    'start_time': str(start_time),
                    'end_time': str(end_time),
                    'success_flag': success_flag}
            return factory.load(data, cls)
