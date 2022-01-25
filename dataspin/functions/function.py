
import os
from basepy.log import logger

from dataspin.providers.local import LocalStorageProvider
from dataspin.utils.util import uuid_generator

class Function:
    def __init__(self, conf):
        self._name = conf.name.replace(' ', '_')
        self._type = conf.function
        self._args = conf.args
        self._kv_args = conf.kv_args

    def process(self, process_temp_dir_list, save_temp_dir, last_task_name):
        pass

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type



class SplitByFunction(Function):
    def __init__(self, conf):
        super().__init__(conf)

    def process(self, delete_temp_path_list, save_temp_dir, last_task_name, process_temp_dir_list=None, data_list=None):
        temp_dir_list = []
        if process_temp_dir_list:
            for process_temp_dir in process_temp_dir_list:
                temp_data_list = LocalStorageProvider.read(process_temp_dir)
                if len(self._args) < 1:
                    return {'': temp_data_list}
                data_map = {}
                self._split_data_list(temp_data_list, data_map)
                for split_key, value_list in data_map.items():
                    storage_path = os.path.join(save_temp_dir, self._name + '/' + split_key + '/result_' + uuid_generator())
                    LocalStorageProvider.save(storage_path, data_list=value_list)
                    temp_dir_list.append(storage_path)
                    delete_temp_path_list.append(storage_path)
        if data_list:
            data_map = {}
            self._split_data_list(data_list, data_map)
            for split_key, value_list in data_map.items():
                storage_path = os.path.join(save_temp_dir, self._name + '/' + split_key + '/result_' + uuid_generator())
                LocalStorageProvider.save(storage_path, data_list=value_list)
                temp_dir_list.append(storage_path)
                delete_temp_path_list.append(storage_path)
            data_list.clear()
        return temp_dir_list

    def _split_data_list(self, data_list, data_map):
        for data in data_list:
            split_value = []
            for split_field in self._args:
                split_value.append(data.get(split_field, ''))
            split_key = '_'.join(split_value)
            if split_key not in data_map:
                data_map[split_key] = [data]
            else:
                data_map[split_key].append(data)


class SaveFunction(Function):
    def __init__(self, conf):
        self._storage = None
        super().__init__(conf)

    def set_storage(self, storage_dict):
        if 'location' not in self._kv_args:
            return
        storage_name = self._kv_args['location']
        self._storage = storage_dict.get(storage_name, None)

    def process(self, delete_temp_path_list, save_temp_dir, last_task_name, process_temp_dir_list=None, data_list=None):
        temp_dir_list = []
        if not self._storage:
            logger.error('storage not exists')
            return
        if process_temp_dir_list:
            for process_temp_dir in process_temp_dir_list:
                self._storage.provider.save(self._storage.provider.path, process_temp_dir=process_temp_dir)
