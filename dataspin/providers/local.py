import os
import shutil
import gzip
import filetype
import json
import re
from basepy.log import logger

from dataspin.utils.util import get_path_file_list


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

    @classmethod
    def read_file(cls, path, data_list: list):
        if path.endswith('.gz'):
            with gzip.open(path, mode='rt', encoding='utf-8') as gdata:
                for line in gdata:
                    data_list.append(json.loads(line))
        # with open(path, 'rb') as f:
        #     for line in f.readlines():
        #         data_list.append(json.loads(line))

    # return data list
    @classmethod
    def read(cls, path):
        if not os.path.exists(path):
            logger.warning('read non-exists file path')
            return []
        data_list = []
        if os.path.isfile(path):
            cls.read_file(path, data_list)
            return data_list
        if os.path.isdir(path):
            for file_name in os.listdir(path):
                cls.read_file(path + file_name, data_list)
            return data_list

    # return data list
    @classmethod
    def read_path_list(cls, path_list):
        data_list = []
        for path in path_list:
            if not os.path.exists(path):
                logger.warning('read non-exists file path, path={path}'.format(path=path))
                continue
            temp_data_list = []
            if os.path.isfile(path):
                cls.read_file(path, temp_data_list)
                data_list.extend(temp_data_list)
            if os.path.isdir(path):
                for file_name in os.listdir(path):
                    cls.read_file(path + file_name, data_list)
                data_list.extend(temp_data_list)
            temp_data_list.clear()
        return data_list

    @classmethod
    def save(cls, path, process_temp_dir=None, data_list=None):
        if process_temp_dir:
            if re.search('(\{.*?\})', path):
                split_key = os.path.dirname(process_temp_dir).split('/')[-1]
                save_path = re.sub('(\{.*?\})', split_key, path)
                save_path = save_path if save_path.endswith('/') else save_path + '/'
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                shutil.copy(process_temp_dir, save_path)
            else:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                shutil.copy(process_temp_dir, path)
        if data_list:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                for data in data_list[: -1]:
                    f.write(json.dumps(data))
                    f.writelines('\n')
                f.write(json.dumps(data_list[-1]))

    @classmethod
    def delete(cls, path):
        if not os.path.exists(path):
            return
        if os.path.isfile(path):
            os.remove(path)
        if os.path.isdir(path):
            for file_name in os.listdir(path):
                os.remove(os.path.join(path, file_name))