import os

from basepy.log import logger

from dataspin.utils.util import uuid_generator
from dataspin.utils import file_operator


class Function:
    def __init__(self, conf):
        self._name = conf.name.replace(' ', '_')
        self._type = conf.function
        self._args = conf.args
        self._kv_args = conf.kv_args


    def process(self, data_file=None):
        pass

    def process_mutli(self, data_files = []):
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

    def process(self, delete_temp_path_list, save_temp_dir, key, process_temp_dir_list=None, data_list=None):
        path_dict = {}
        if process_temp_dir_list:
            for process_temp_dir in process_temp_dir_list:
                temp_data_list = file_operator.read(process_temp_dir)
                if len(self._args) < 1:
                    return {'default': temp_data_list}
                data_map = {}
                self._split_data_list(temp_data_list, data_map)
                for split_key, value_list in data_map.items():
                    storage_path = os.path.join(save_temp_dir,
                                                self._name + '/' + split_key + '/result_' + uuid_generator())
                    file_operator.save(storage_path, data_list=value_list)
                    if split_key in path_dict:
                        path_dict[split_key].append(storage_path)
                    else:
                        path_dict[split_key] = [storage_path]
                    delete_temp_path_list.append(storage_path)
        if data_list:
            data_map = {}
            self._split_data_list(data_list, data_map)
            for split_key, value_list in data_map.items():
                storage_path = os.path.join(save_temp_dir, self._name + '/' + split_key + '/result_' + uuid_generator())
                file_operator.save(storage_path, data_list=value_list)
                if split_key in path_dict:
                    path_dict[split_key].append(storage_path)
                else:
                    path_dict[split_key] = [storage_path]
                delete_temp_path_list.append(storage_path)
        return 'path_list', path_dict

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

    def process(self, delete_temp_path_list, save_temp_dir, key, process_temp_dir_list=None, data_list=None):
        if not self._storage:
            logger.error('storage not exists')
            return 'none', None
        if process_temp_dir_list:
            for process_temp_dir in process_temp_dir_list:
                self._storage.provider.save_result(key, process_temp_dir=process_temp_dir)
            return 'path_list', {key: process_temp_dir_list}
        if data_list:
            self._storage.provider.save_result(key, data_list=data_list)
            return 'data_list', {key: data_list}
        return 'none', None


class PkIndexFunction(Function):
    def __init__(self, conf):
        super().__init__(conf)

    def process(self, delete_temp_path_list, save_temp_dir, key, process_temp_dir_list=None, data_list=None):
        pk_index_save_path = os.path.join(save_temp_dir, self._name)
        pk_index_list = file_operator.read(os.path.join(pk_index_save_path, 'pk_index.csv'))
        new_pk_index_list = []
        if process_temp_dir_list:
            for process_temp_dir in process_temp_dir_list:
                temp_data_list = file_operator.read(process_temp_dir)
                self._get_pk_index(pk_index_list, new_pk_index_list, temp_data_list)
            file_operator.save(os.path.join(pk_index_save_path, 'pk_index.csv'), data_list=new_pk_index_list)
            return 'path_list', {key: process_temp_dir_list}
        if data_list:
            self._get_pk_index(pk_index_list, new_pk_index_list, data_list)
            file_operator.save(os.path.join(pk_index_save_path, 'pk_index.csv'), data_list=new_pk_index_list)
            return 'data_list', {key: data_list}
        return 'none', None

    def _get_pk_index(self, pk_index_list, new_pk_index_list, data_list):
        pk = self._kv_args.get('key')
        for data in data_list:
            pk_value = []
            for field in pk:
                value = data.get(field, '')
                pk_value.append(value)
            pk_index = ','.join(pk_value)
            if pk_index in pk_index_list:
                continue
            else:
                pk_index_list.append(pk_index)
                new_pk_index_list.append(pk_index)