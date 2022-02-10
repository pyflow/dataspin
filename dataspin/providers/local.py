import os
from basepy.log import logger

from dataspin.utils.util import get_path_file_list
from dataspin.utils import file_operator


class LocalStreamProvider:
    def __init__(self, path, options):
        self.path = None
        self.polling_flag = False
        self.processed_file_list = []
        self.processing_file_list = []
        self.waiting_file_list = []
        self._load(path, options)

    def _load(self, path, options):
        self.path = path
        if (not os.path.exists(self.path)) or (not os.path.isdir(self.path)):
            logger.warning('read non-exists file path')
        watch = options.get('watch', False)
        self.polling_flag = watch

    def _scan(self):
        for file_path in get_path_file_list(self.path):
            if file_path in self.processed_file_list:
                continue
            if file_path in self.processing_file_list:
                continue
            if file_path in self.waiting_file_list:
                continue
            else:
                self.waiting_file_list.append(file_path)
    
    def get(self, block=True, timeout=None):
        file_path = self.waiting_file_list.pop(0)
        if file_path:
            self.processing_file_list.append(file_path)
            return file_path
        else:
            return None
    
    def task_done(self, file_path):
        if file_path in self.processing_file_list:
            self.processing_file_list.remove(file_path)
            self.processed_file_list.append(file_path)
                


class LocalStorageProvider:

    def __init__(self, path, options):
        self._path = path
        self.options = options

    @property
    def path(self):
        return self._path

    def save_result(self, key, process_temp_dir=None, data_list=None):
        file_operator.save(self._path, key, process_temp_dir, data_list)
