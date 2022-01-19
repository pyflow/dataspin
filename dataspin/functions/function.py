
import os

from dataspin.providers.local import LocalStorageProvider

class Function:
    def __init__(self, conf):
        self._name = conf.name
        self._type = conf.function
        self._args = conf.args
        self._kv_args = conf.kv_args

    def process(self, process_temp_dir_list, save_temp_dir, last_task_name):
        pass

    @property
    def name(self):
        return self._name


class SplitByFunction(Function):
    def __init__(self, conf):
        super().__init__(conf)

    def process(self, process_temp_dir_list, save_temp_dir, last_task_name):
        temp_dir_list = []
        for process_temp_dir in process_temp_dir_list:
            data_list = LocalStorageProvider.read(process_temp_dir)
            if len(self._args) < 1:
                return {'': data_list}
            data_map = {}
            for data in data_list:
                split_value = []
                for split_field in self._args:
                    split_value.append(data.get(split_field, ''))
                split_key = '_'.join(split_value)
                if split_key not in data_map:
                    data_map[split_key] = [data]
                else:
                    data_map[split_key].append(data)
            for split_key, value_list in data_map.items():
                storage_path = os.path.join(process_temp_dir, self._name + '/' + split_key)
                LocalStorageProvider.save(storage_path)
                temp_dir_list.append(storage_path)
        return temp_dir_list


class SaveFunction(Function):
    def __init__(self, conf):
        super().__init__(conf)

    def process(self, process_temp_dir_list, save_temp_dir, last_task_name):
        temp_dir_list = []
        save_temp_sub_dir = os.path.join(save_temp_dir, last_task_name)
        for process_temp_dir in process_temp_dir_list:
            if save_temp_sub_dir in process_temp_dir:
                destination_path = process_temp_dir.replace(save_temp_sub_dir, '')
                pass

