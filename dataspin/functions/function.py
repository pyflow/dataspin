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
            file = self.process(data_file, context)
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
        logger.debug('save function process', data_file=data_file.file_path)
        location = self.args.get('location')
        storage = context.get_storage(location)
        if not storage:
            raise Exception('No storage defined.')
        storage.save(data_file.basename, data_file.file_path)
        return data_file


class PkIndexFunction(FunctionMultiMixin, Function):
    function_name = 'pk_index'

    def process(self, data_file, context):
        logger.debug('index function process', data_file=data_file.file_path)
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


class FormatFunction(FunctionMultiMixin, Function):
    function_name = 'format'

    def process(self, data_file, context):
        def transform(data, fields, field_type_mapping):
            for key in data.keys():
                if key in fields and fields[key].type in field_type_mapping:
                    try:
                        type_format = field_type_mapping[fields[key].type]
                        data[key] = type_format(data[key])
                    except Exception:
                        logger.error(f'transform data failed, key={key}, value={data[key]}, type={fields[key].type}')
                        return None
            return json.dumps(data)

        logger.debug('format data file with data_view', data_file=data_file.file_path)
        if data_file.file_type == 'index':
            return data_file
        table_name = self.args.get('table_name')
        data_view = context.get_data_view(table_name)
        if table_name is None or data_view is None:
            logger.error('search table failed')
            return data_file

        field_type_mapping = data_view.field_type_mapping
        fields = data_view.fields
        file_reader = DataFileReader(data_file.file_path)

        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-format.jsonl')
        file_saver = AtomicSaver(dst_path)
        file_saver.setup()
        for (data, line) in file_reader.readlines():
            format_line = transform(data, fields, field_type_mapping)
            if format_line:
                file_saver.part_file.write(format_line.encode('utf-8'))
                file_saver.part_file.write(b'\n')
        file_saver.__exit__(None, None, None)
        return context.create_data_file(file_path=file_saver.dest_path)


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
                    pk_value.append(data[pk])
                pk_value = tuple(pk_value)
                if pk_value in pk_values:
                    continue
                pk_values.add(pk_value)
                f.write(json.dumps(data).encode('utf-8'))
                f.write(b'\n')
        return data_file,context.create_data_file(file_path = dst_path)


class FilterFunction(FunctionMultiMixin, Function):
    function_name = 'filter'

    relationship_func = {
        'greater than or equal': lambda x, y: x >= y,
        'less than or equal': lambda x, y: x <= y,
        'greater than': lambda x, y: x > y,
        'less than': lambda x, y: x < y,
        'equal': lambda x, y: x == y,
        'not equal': lambda x, y: x != y,
        'in': lambda x, y: x in y,
        'not in': lambda x, y: x not in y,
        "all": lambda x, y: True
    }

    def process(self, data_file, context):
        def filter(data, rule_list):
            for rule in rule_list:
                key = rule.get('key')
                relationship = rule.get('relationship')
                value = rule.get('value')
                filter_func = self.relationship_func.get(relationship, lambda x, y: False)
                if filter_func(data.get(key), value) is False:
                    return False
            return True

        logger.debug('filter data file', data_file=data_file.file_path)
        data_files = []

        rules_config = self.args.get('filter_rules', [])
        file_reader = DataFileReader(data_file.file_path)
        for rule_config in rules_config:
            tags = rule_config.get('tags')
            rule_list = rule_config.get('rule', [])

            dst_path = os.path.join(context.temp_dir, f'{data_file.name}-filter-{tags if tags else "default"}.jsonl')
            file_saver = AtomicSaver(dst_path)
            file_saver.setup()

            for data, line in file_reader.readlines():
                try:
                    filtered = filter(data, rule_list)
                    if filtered:
                        file_saver.part_file.write(line.encode('utf-8'))
                        file_saver.part_file.write(b'\n')
                except TypeError as e:
                    logger.error(f'filter value type mismatch, exception={repr(e)}')
                except Exception as e:
                    logger.error(f'filter failed, exception={repr(e)}')

            file_saver.__exit__(None, None, None)
            data_files.append(context.create_data_file(file_path=file_saver.dest_path, tags=tags))

        return data_files



