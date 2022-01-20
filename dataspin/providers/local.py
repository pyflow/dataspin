import os
import filetype
import json
from basepy.log import logger


class LocalStreamProvider:
    pass


class LocalStorageProvider:
    @classmethod
    def read_file(cls, path, data_list: list):
        with open(path, 'rb') as f:
            for line in f.readlines():
                data_list.append(json.loads(line))

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

    @classmethod
    def save(cls, path, data_list):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            for data in data_list[: -1]:
                f.write(json.dumps(data))
                f.writelines('\n')
            f.write(json.dumps(data_list[-1]))
