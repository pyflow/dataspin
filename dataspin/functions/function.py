from collections import namedtuple
import os
from basepy.log import logger

from dataspin.utils import common
from boltons.fileutils import AtomicSaver, atomic_save
import json
from jinja2 import Environment, TemplateSyntaxError


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
        def write_to_group(group_names, line):
            if group_names not in group_file_savers:
                group_name = '-'.join(group_names)
                dst_path = os.path.join(context.temp_dir, f'{data_file.name}-group-{group_name}.jsonl')
                file_saver = AtomicSaver(dst_path)
                file_saver.setup()
                group_file_savers[group_names] = file_saver
            saver = group_file_savers[group_names]
            saver.part_file.write(line.encode('utf-8'))
            saver.part_file.write(b'\n')

        data_files = []
        group_file_savers = {}
        split_keys = self.args['key']
        tags = self.args['tags']
        tags_with_group = {}
        for (data, line) in data_file.readlines():
            group_names = []
            for split_key in split_keys:
                group_name = data.get(split_key)
                group_names.append(group_name)
            group_names = tuple(group_names)
            if not tags_with_group.get(group_names):
                object_name = namedtuple("DataObject", data.keys())(*data.values())
                fill_tags = {}
                for tag_k,tag_v in tags.items():
                    fill_tags[tag_k] = tag_v.format(data=object_name)
                tags_with_group[group_names] = fill_tags
            if not group_names:
                # TODO: warning
                continue
            write_to_group(group_names, line)
        for group_names,saver in group_file_savers.items():
            saver.__exit__(None, None, None)                
            tags = tags_with_group[group_names] if tags_with_group[group_names] else None
            data_files.append(context.create_data_file(file_path=saver.dest_path,tags=tags))
        return data_files


class SaveFunction(FunctionMultiMixin,Function):
    function_name = 'save'

    def process(self, data_file, context):
        logger.debug('save function process', data_file=data_file.file_path)
        location = self.args.get('location')
        path_suffix = self.args.get('path_suffix')
        trigger = self.args.get('trigger')

        if path_suffix:
            path_suffix = path_suffix.format(**data_file.tags)
            if not path_suffix.endswith('/'):
                path_suffix = path_suffix + '/'
        storage = context.get_storage(location)
        if not storage:
            raise Exception('No storage defined.')
        key = path_suffix + data_file.basename if path_suffix else data_file.basename
        path = storage.save(key, data_file.file_path)
        if trigger:
            stream = context.get_stream(trigger)
            if not stream:
                raise Exception('No stream defined.')
            stream.send_to_stream(path,data_file.tags,storage.storage_type)
        return data_file


class PkIndexFunction(FunctionMultiMixin, Function):
    function_name = 'pk_index'

    def process(self, data_file, context):
        logger.debug('index function process', data_file=data_file.file_path)
        index_key = self.args['key']
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-pk-index.jsonl')
        file_saver = AtomicSaver(dst_path)
        file_saver.setup()
        index_set = set()
        for (data, line) in data_file.readlines():
            index_data = dict()
            for key in index_key:
                index_data[key] = data.get(key)
            index_line = json.dumps(index_data)
            if index_line not in index_set:
                index_set.add(index_line)
                file_saver.part_file.write(index_line.encode('utf-8'))
                file_saver.part_file.write(b'\n')

        file_saver.__exit__(None, None, None)
        new_data_file = context.create_data_file(dst_path, file_type="index",tags= data_file.tags)
        return [data_file, new_data_file]


class FlattenFunction(FunctionMultiMixin, Function):
    function_name = 'flatten'

    def process(self, data_file, context):
        if data_file.file_format != 'jsonl':
            raise Exception('Not supported file type')
        if data_file.file_type == 'index':
            return data_file
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-flatten.jsonl')
        with atomic_save(dst_path) as f:
            for data, line in data_file.readlines():
                f.write(json.dumps(common.flatten_dict(data)).encode('utf-8'))
                f.write(b'\n')
        return context.create_data_file(file_path = dst_path,tags = data_file.tags)


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

        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-format.jsonl')
        file_saver = AtomicSaver(dst_path)
        file_saver.setup()
        for (data, line) in data_file.readlines():
            format_line = transform(data, fields, field_type_mapping)
            if format_line:
                file_saver.part_file.write(format_line.encode('utf-8'))
                file_saver.part_file.write(b'\n')
        file_saver.__exit__(None, None, None)
        return context.create_data_file(file_path=file_saver.dest_path)


class DeduplicateFunction(Function):
    function_name = 'deduplicate'

    def process(self, data_file, context):
        if data_file.file_type == 'index':
            return data_file
        pks = self.args['key']
        dst_path = os.path.join(context.temp_dir, f'{data_file.name}-deduplicate.jsonl')
        pk_values = set()
        with atomic_save(dst_path) as f:
            for data, line in data_file.readlines():
                pk_value = []
                for pk in pks:
                    pk_value.append(data[pk])
                pk_value = tuple(pk_value)
                if pk_value in pk_values:
                    continue
                pk_values.add(pk_value)
                f.write(json.dumps(data).encode('utf-8'))
                f.write(b'\n')
        return data_file, context.create_data_file(file_path=dst_path, tags=data_file.tags)


class FilterFunction(FunctionMultiMixin, Function):
    function_name = 'filter'

    def process(self, data_file, context):
        logger.debug('filter data file', data_file=data_file.file_path)
        data_files = []

        rules_config = self.args.get('filter_rules', [])
        for rule_config in rules_config:
            tags = rule_config.get('tags')
            rule = rule_config.get('rule', "False")

            dst_path = os.path.join(context.temp_dir, f'{data_file.name}-filter-{"_".join(list(tags.values())) if tags else "default"}.jsonl')
            file_saver = AtomicSaver(dst_path)
            file_saver.setup()

            # compile expression by jinja2
            compiled_expr = Environment().compile_expression(rule)

            for data, line in data_file.readlines():
                try:
                    filtered = compiled_expr(data)
                    if filtered:
                        file_saver.part_file.write(line.encode('utf-8'))
                        file_saver.part_file.write(b'\n')
                except TemplateSyntaxError as e:
                    logger.error(f'filter rule syntax error, exception={repr(e)}')
                except Exception as e:
                    logger.error(f'filter failed, exception={repr(e)}')

            file_saver.__exit__(None, None, None)
            data_files.append(context.create_data_file(file_path=file_saver.dest_path, tags=tags))

        return data_files


class MergeFunction(FunctionMultiMixin, Function):
    function_name = 'merge'
    default_file_size = 100000

    def __init__(self, args):
        super(MergeFunction, self).__init__(args)
        self.file_size = self.args.get('output_file_lines', self.default_file_size)

    def process(self, data_file, context):
        new_data_files = self.merge_group_file('default', [data_file], context)
        return new_data_files

    def process_multi(self, data_files, context):
        merge_group = {}
        result = []
        if self.args.get('tags'):
            tags = self.args.get('tags')
            for data_file in data_files:
                tag_value_list = [data_file.tags.get(tag) for tag in tags]
                group_name = '_'.join(list(filter(lambda x: x is not None, tag_value_list)))
                if group_name not in merge_group:
                    merge_group[group_name] = [data_file]
                else:
                    merge_group[group_name].append(data_file)
        else:
            merge_group['default'] = data_files

        for group_set, file_list in merge_group.items():
            new_data_files = self.merge_group_file(group_set, file_list, context)
            result.extend(new_data_files)
        return result

    def merge_group_file(self, group_name, file_list, context):
        file_count = 0
        count = 0
        new_data_files = []
        dst_path = os.path.join(context.temp_dir, f'merge-{group_name}_{file_count}.jsonl')
        file_saver = AtomicSaver(dst_path)
        file_saver.setup()
        for file in file_list:
            for (data, line) in file.readlines():
                file_saver.part_file.write(line.encode('utf-8'))
                file_saver.part_file.write(b'\n')
                if count >= self.file_size:
                    file_count += 1
                    count = 0
                    file_saver.__exit__(None, None, None)
                    new_data_files.append(context.create_data_file(file_saver.dest_path))
                    next_dst_path = os.path.join(context.temp_dir, f'merge-{group_name}_{file_count}.jsonl')
                    file_saver = AtomicSaver(next_dst_path)
                    file_saver.setup()
                count += 1

        if file_saver:
            file_saver.__exit__(None, None, None)
            new_data_files.append(context.create_data_file(file_saver.dest_path))
        return new_data_files