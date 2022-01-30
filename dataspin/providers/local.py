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
        self._load(path, options)

    def _load(self, path, options):
        self.path = path
        if options:
            for option in options:
                if option == 'watch':
                    self.polling_flag = True

    def stream(self):
        while True:
            if (not os.path.exists(self.path)) or (not os.path.isdir(self.path)):
                logger.warning('read non-exists file path')
                self.processing_file_list = []
            else:
                for absolute_path in get_path_file_list(self.path):
                    if absolute_path in self.processed_file_list:
                        continue
                    self.processing_file_list.append(absolute_path)
            yield self.processing_file_list
            self.processed_file_list.extend(self.processing_file_list)
            self.processing_file_list.clear()


class LocalStorageProvider:

    def __init__(self, path, options):
        self._path = path
        self.options = options

    @property
    def path(self):
        return self._path

    def save_result(self, key, process_temp_dir=None, data_list=None):
        file_operator.save(self._path, key, process_temp_dir, data_list)
