import os

from basepy.log import logger
from dataspin.utils import common
from dataspin.utils.file import DataFileReader
from boltons.fileutils import AtomicSaver,atomic_save
import json


class FunctionMultiMixin:

    def process_multi(self, data_files, context):
        result = []
        for data_file in data_files:
            file = self.process(data_file,context)
            if isinstance(file, (list, tuple)):
                result.extend(file)
                continue
            result.append(file)
        return result

        
class Function:
    function_name = 'pass'

    def __init__(self, args):
        self.args = args

    def process(self, data_file, context):
        pass

    @property
    def name(self):
        return self.function_name


class SplitByFunction(Function):
    function_name = 'splitby'
    
    def process(self, data_file, context):
        def write_to_group(group_name, line):
            if group_name not in group_file_savers:
                dst_path = os.path.join(context.temp_dir, f'{data_file.name}-group-{group_name}.jsonl')
                file_saver = AtomicSaver(dst_path)
                file_saver.setup()
                group_file_savers[group_name] = file_saver
            saver = group_file_savers[group_name]
            saver.part_file.write(line.encode('utf-8'))
            saver.part_file.write(b'\n')
        data_files = []
        group_file_savers = {}
        split_key = self.args['key'][0]
        file_reader = DataFileReader(data_file.file_path)
        for (data, line) in file_reader.readlines():
            group_name = data.get(split_key)
            if not group_name:
                # TODO: warning
                continue
            write_to_group(group_name, line)

        for saver in group_file_savers.values():
            saver.__exit__(None, None, None)
            data_files.append(context.create_data_file(file_path=saver.dest_path))
        return data_files


class SaveFunction(FunctionMultiMixin, Function):
    function_name = 'save'
    
    def process(self, data_file, context):
        logger.debug('save function process', data_file = data_file.file_path)
        location = self.args.get('location')
        storage = context.get_storage(location)
        if not storage:
            raise Exception('No storage defined.')
        save_filepath = storage.save(data_file.basename, data_file.file_path)
        if data_file.file_type == 'index':
            data_file.index_meta_info['file_path'] = save_filepath
            context.save_pk_metadata(data_file.index_meta_info)
        return data_file


class PkIndexFunction(FunctionMultiMixin, Function):
    function_name = 'pk_index'

    def process(self, data_file, context):
        logger.debug('index function process', data_file = data_file.file_path)
        index_key = self.args['key']
        time_key = self.args['time_key']
        file_reader = DataFileReader(data_file.file_path)
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-pk-index.jsonl')
        index_set = set()
        min_timestamp = None
        max_timestamp = None
        with atomic_save(dst_path) as f:
            for (data, line) in file_reader.readlines():
                index_data = dict()
                for key in index_key:
                    index_data[key] = data.get(key)
                time_data = common.convert_timestr(data[time_key])
                if max_timestamp == None:
                    max_timestamp = time_data
                if min_timestamp == None:
                    min_timestamp = time_data
                if min_timestamp < time_data and max_timestamp > time_data:
                    pass
                elif max_timestamp < time_data:
                    max_timestamp = time_data
                else:
                    min_timestamp = time_data
                index_line = json.dumps(index_data)
                if index_line not in index_set:
                    index_set.add(index_line)
                    f.write(index_line.encode('utf-8'))
                    f.write(b'\n')
        index_meta_info = {'min_timestamp':min_timestamp,'max_timestamp':max_timestamp}
        new_data_file = context.create_data_file(dst_path, file_type="index",index_meta_info=index_meta_info)
        return [data_file, new_data_file]


class DeduplicateFunction(FunctionMultiMixin,Function):
    function_name = 'deduplicate'

    def process(self,data_file,context):
        if data_file.file_type == 'index':
            return data_file
        key = self.args['key']
        slide_window = self.args['slide_window']
        start_time,end_time = common.parse_duration(slide_window)
        file_paths = context.search_pk_files(start_time,end_time)
        pk_index = set()
        for file_path in file_paths:
            file_reader = DataFileReader(file_path)
            for (data, line) in file_reader.readlines():
                pk = []
                for k in key:
                    pk.append(data[k])
                pk_index.add(''.join(pk))
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-deduplicate.jsonl')
        with atomic_save(dst_path) as f:
            file_reader = DataFileReader(data_file.file_path)
            for (data, line) in file_reader.readlines():
                pk = []
                for k in key:
                    pk.append(data[k])
                index = ''.join(pk)
                if index not in pk_index:
                    pk_index.add(index)
                    f.write(json.dumps(data).encode('utf-8'))
                    f.write(b'\n')
        return context.create_data_file(file_path=dst_path)


class FlattenFunction(FunctionMultiMixin,Function):
    function_name = 'flatten'

    def process(self, data_file, context):
        if data_file.data_format !='jsonl':
            raise Exception('Not supported file type')
        if data_file.file_type == 'index':
            return data_file
        file_reader = DataFileReader(data_file.file_path)
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-flatten.jsonl')
        with atomic_save(dst_path) as f:
            for data,line in file_reader.readlines():
                f.write(json.dumps(common.flatten_dict(data)).encode('utf-8'))
                f.write(b'\n')
        return context.create_data_file(file_path = dst_path)

    