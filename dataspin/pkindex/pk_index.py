import datetime
import time
from dataspin.utils import common


class PKIndexCache:

    def __init__(self, pk_keys: list, time_window, baseline_time) -> None:
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

    def update_pk_files(self, data_files:list):
        for data_file in data_files:
            self._expire()
            for data, line in data_file.readlines():
                self._update_cache_value(data)

    def _update_cache_value(self, data):
        pk_value = []
        for pk_key in self._pk_keys:
            pk_value.append(data[pk_key])
        pk_value = tuple(pk_value)
        self._cache.add(pk_value)
        self._precache.add(pk_value)

    def _expire(self):
        if not self._time_window:
            return
        current_timestamp = int(time.time())
        if (current_timestamp - self._baseline_time) >= self._time_window:
            self._cache = self._precache
            self._precache = set()
            self._baseline_time = current_timestamp

    def is_exists(self, data: dict):
        """
        data {"app_id":"","event_id":""}
        """
        pk_value = []
        for k in self._pk_keys:
            pk_value.append(data[k])
        pk_value = tuple(pk_value)
        return pk_value in self._cache


class IndexSearcher:

    def __init__(self) -> None:
        pass

    def get_index_prefixs(self, pattern: str, tags, start_timestamp: int, end_timestamp: int):
        """
        pattern = "datalog/event/{app_id}/{year}/{month}/{day}/{hour}/{minute}/"
        """
        if start_timestamp <= 0:
            raise Exception('start_timestamp should be larger than 0')
        if end_timestamp == None:
            end_timestamp = int(time.time())
        expire_minutes = int((end_timestamp-start_timestamp)/60)
        end_time = datetime.datetime.utcfromtimestamp(end_timestamp)
        prefixs = []
        for i in range(expire_minutes):
            target_date = end_time+datetime.timedelta(minute=-i)
            year = str.zfill(str(target_date.year), 4)
            month = str.zfill(str(target_date.month), 2)
            day = str.zfill(str(target_date.day), 2)
            hour = str.zfill(str(target_date.hour), 2)
            minute = str.zfill(str(target_date.minute), 2)
            date_pattern = {"year": year, "month": month,
                            "day": day, "hour": hour, "minute": minute}
            date_pattern.update(tags)
            prefixs.append(pattern.format(**date_pattern))
        return prefixs

    def check_index_file(self, key):
        return key.endswith('.index')

    def select_index_files(self, storage, pattern: str, tags, start_timestamp,end_timestamp):
        prefixs = self.get_index_prefixs(
            pattern, tags, start_timestamp, end_timestamp)
        target_index_files = []
        for prefix in prefixs:
            for key in storage.get(prefix=prefix):
                if not self.check_index_file(key):
                    continue
                target_index_files.append(key)
        return target_index_files
