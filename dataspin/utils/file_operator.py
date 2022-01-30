import os
import gzip
import csv
import json
import shutil
import re
from boltons.fileutils import atomic_save
from basepy.log import logger


def read_file(path, data_list: list):
    if path.endswith('.gz'):
        with gzip.open(path, mode='rt', encoding='utf-8') as gdata:
            for line in gdata:
                data_list.append(json.loads(line))
    elif path.endswith('.csv'):
        with open(path, 'r') as f:
            for line in f.readlines():
                data_list.append(line)
    else:
        with open(path, 'rb') as f:
            for line in f.readlines():
                data_list.append(json.loads(line))


# return data list
def read(path):
    if not os.path.exists(path):
        logger.warning('read non-exists file path')
        return []
    data_list = []
    if os.path.isfile(path):
        read_file(path, data_list)
        return data_list
    if os.path.isdir(path):
        for file_name in os.listdir(path):
            read_file(path + file_name, data_list)
        return data_list


# return data list
def read_path_list(path_list):
    data_list = []
    for path in path_list:
        if not os.path.exists(path):
            logger.warning('read non-exists file path, path={path}'.format(path=path))
            continue
        temp_data_list = []
        if os.path.isfile(path):
            read_file(path, temp_data_list)
            data_list.extend(temp_data_list)
        if os.path.isdir(path):
            for file_name in os.listdir(path):
                read_file(path + file_name, data_list)
            data_list.extend(temp_data_list)
        temp_data_list.clear()
    return data_list


def save(path, key='default', process_temp_dir=None, data_list=None):
    if process_temp_dir:
        if key != 'default':
            save_path = re.sub('(\{.*?\})', key, path)
            save_path = save_path if save_path.endswith('/') else save_path + '/'
        else:
            save_path = path
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        shutil.copy(process_temp_dir, save_path)
    if data_list:
        if key != 'default':
            save_path = re.sub('(\{.*?\})', key, path)
            save_path = save_path if save_path.endswith('/') else save_path + '/'
        else:
            save_path = path
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with atomic_save(save_path, text_mode=True) as f:
            if path.endswith('.csv'):
                for data in data_list[: -1]:
                    f.write(data)
                    f.writelines('\n')
                f.write(data_list[-1])
            else:
                for data in data_list[: -1]:
                    f.write(json.dumps(data))
                    f.writelines('\n')
                f.write(json.dumps(data_list[-1]))



def delete(path):
    if not os.path.exists(path):
        return
    if os.path.isfile(path):
        os.remove(path)
    if os.path.isdir(path):
        for file_name in os.listdir(path):
            os.remove(os.path.join(path, file_name))
