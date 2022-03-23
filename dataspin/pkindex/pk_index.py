import datetime
import time
from dataspin.utils import common


from string import Formatter


class PKIndexCache:

    def __init__(self, pk_keys: list, time_window: int, baseline_time: int) -> None:
        """
        For run once task,expire time is None,and for run loop task,expire time can be seconds.
        When a data file processed success, update the data file index file to caches
        when current_time minus baseline_time larger than time_window,exchange cache and precache,and refresh a new set for precache
        """
        self._cache = set()
        self._precache = set()
        self._pk_keys = pk_keys
        self._time_window = time_window
        self._baseline_time = baseline_time

    def update_pk_file(self, data_file, current_data_file_key_timestamp):
        self._expire(current_data_file_key_timestamp)
        for data, line in data_file.readlines():
            self._update_cache_value(data)

    def _update_cache_value(self, data):
        pk_value = []
        for pk_key in self._pk_keys:
            pk_value.append(data[pk_key])
        pk_value = tuple(pk_value)
        self._cache.add(pk_value)
        self._precache.add(pk_value)

    def load(self, data_files):
        for data_file in data_files:
            self.update_pk_file(data_file)

    def _expire(self, current_data_file_key_timestamp):
        if not self._time_window:
            return
        if (current_data_file_key_timestamp - self._baseline_time) >= self._time_window:
            self._cache = self._precache
            self._precache = set()
            self._baseline_time = current_data_file_key_timestamp

    def is_exists(self, data: dict):
        """
        data {"app_id":"","event_id":""}
        """
        pk_value = []
        for k in self._pk_keys:
            pk_value.append(data[k])
        pk_value = tuple(pk_value)
        return pk_value in self._cache


class IndexSource:

    def __init__(self) -> None:
        pass

    def parse_pattern(self, pattern: str, tags, start_timestamp: int, end_timestamp: int):
        """
        pattern = "datalog/event/{app_id}/{yyyy}/{MM}/{dd}/{hh}/{mm}/.."
        """
        if start_timestamp <= 0:
            raise Exception('start_timestaxmp should be larger than 0')
        if end_timestamp == None:
            end_timestamp = int(time.time())
        expire_hour = int((end_timestamp-start_timestamp)/60/60)
        end_time = datetime.datetime.utcfromtimestamp(end_timestamp)
        clean_pattern = pattern.rstrip('/..').replace('{mm}', '')
        prefixs = []
        for i in range(expire_hour):
            target_date = end_time+datetime.timedelta(hours=-i)
            year = str.zfill(str(target_date.year), 4)
            month = str.zfill(str(target_date.month), 2)
            day = str.zfill(str(target_date.day), 2)
            hour = str.zfill(str(target_date.hour), 2)
            date_pattern = {"yyyy": year, "MM": month, "dd": day, "hh": hour}
            date_pattern.update(tags)
            prefixs.append(clean_pattern.format(**date_pattern))
        return prefixs

    def get_timestamp_from_key(self, pattern, key):
        """
        pattern = "datalog/event/{app_id}/{yyyy}/{MM}/{dd}/{hh}/{mm}/.."
        key = "datalog/event/APPXXX/2022/03/19/20/01/xxxx.gz"
        """
        date_info = {}
        for literal_text, field_name, _, _ in Formatter().parse(pattern):
            if not field_name:
                continue
            res = key.lstrip(literal_text)
            value = res.split('/', 1)[0]
            key = res.split('/', 1)[1]
            date_info[field_name] = value
        year = int(date_info['yyyy'])
        month = int(date_info['MM'])
        day = int(date_info['dd'])
        hour = int(date_info['hh'])
        minute = int(date_info['mm'])
        d = datetime.datetime(year=year, month=month,
                              day=day, hour=hour, minute=minute)
        return int(datetime.datetime.timestamp(d))

    def get_index_filepath_with_date_file(self, data_file):
        if data_file.ext == '.index':
            return data_file.file_path
        return data_file.file_path.replace(data_file.ext, '.index')

    def check_index_file_in_rules(self, key, pattern, start_timestamp, end_timestamp):
        if not key.endswith('.index'):
            return False
        key_timestamp = self.get_timestamp_from_key(
            pattern, key)
        if key_timestamp > end_timestamp or key_timestamp < start_timestamp:
            return False
        return True

    def select_index_files(self, storage, pattern: str, data_file, time_window):
        seconds = common.convert_time_window_to_seconds(time_window)
        end_timestamp = self.fetch_index_file_with_date_file(data_file)
        start_timestamp = end_timestamp - seconds
        prefixs = self.parse_pattern(
            pattern, data_file.tags, start_timestamp, end_timestamp)
        target_index_files = []
        for prefix in prefixs:
            for key in storage.get(prefix=prefix):
                if not self.check_index_file_in_rules(key):
                    continue
                target_index_files.append(key)
        return target_index_files
