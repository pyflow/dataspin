import os
import shutil
from basepy.log import logger
from boltons.fileutils import atomic_save

from dataspin.utils.common import scantree


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
        watch = 'watch' in options
        self.polling_flag = watch

    def _scan(self):
        for file_path in scantree(self.path):
            #logger.debug('scaned file', file=file_path)
            if file_path in self.processed_file_list:
                continue
            if file_path in self.processing_file_list:
                continue
            if file_path in self.waiting_file_list:
                continue
            else:
                #logger.debug('adding file to waiting list', file=file_path)
                self.waiting_file_list.append(file_path)
    
    def get(self, block=True, timeout=None):
        self._scan()
        if self.processing_file_list:
            return self.processing_file_list[0]
        if len(self.waiting_file_list) < 1:
            return None
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

    def recover(self, processed_files, processing_files):
        self.processed_file_list.extend(processed_files)
        self.processing_file_list.extend(processing_files)


class LocalStorageProvider:

    def __init__(self, path, options):
        self._path = path
        self.options = options

    @property
    def path(self):
        return self._path

    def save(self, key, local_file):
        save_path = os.path.join(self._path, key)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy(local_file, save_path)
    
    def save_data(self, key, lines):
        save_path = os.path.join(self._path, key)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with atomic_save(save_path, text_mode=False) as fo:
            for line in lines:
                _ = fo.write(line.encode('utf-8'))