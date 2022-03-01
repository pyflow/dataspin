import os
from basepy.log import logger

from dataspin.utils import common
from dataspin.utils.file import DataFileReader
from boltons.fileutils import AtomicSaver,atomic_save
import json
import hashlib


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
        storage.save(data_file.basename, data_file.file_path)
        return data_file


class PkIndexFunction(FunctionMultiMixin, Function):
    function_name = 'pk_index'

    def process(self, data_file, context):
        logger.debug('index function process', data_file = data_file.file_path)
        index_key = self.args['key']
        file_reader = DataFileReader(data_file.file_path)
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-pk-index.jsonl')
        file_saver = AtomicSaver(dst_path)
        file_saver.setup()
        index_set = set()

        for (data, line) in file_reader.readlines():
            index_data = dict()
            for key in index_key:
                index_data[key] = data.get(key)
            index_line = json.dumps(index_data)
            if index_line not in index_set:
                index_set.add(index_line)
                file_saver.part_file.write(index_line.encode('utf-8'))
                file_saver.part_file.write(b'\n')

        file_saver.__exit__(None, None, None)
        new_data_file = context.create_data_file(dst_path, file_type="index")
        return [data_file, new_data_file]


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


class DeduplicateFunction(Function):
    function_name = 'deduplicate'

    def process(self,data_file,context):
        if data_file.file_type == 'index':
            return data_file
        pks = self.args['key']
        file_reader = DataFileReader(data_file.file_path)
        dst_path = os.path.join(context.temp_dir,f'{data_file.name}-deduplicate.jsonl')
        pk_values = set()
        with atomic_save(dst_path) as f:
            for data,line in file_reader.readlines():
                pk_value = []
                for pk in pks:
                    pk_value.append(hashlib.sha1(data[pk].encode('utf-8')).hexdigest())
                pk_value = ''.join(pk_value)
                if pk_value in pk_values:
                    continue
                pk_values.add(pk_value)
                f.write(json.dumps(data).encode('utf-8'))
                f.write(b'\n')
        return data_file,context.create_data_file(file_path = dst_path)
