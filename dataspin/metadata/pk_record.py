import os
from basepy.log import logger
import json


class PKRecord:

    def __init__(self,file_path,min_timestamp,max_timestamp) -> None:
        self._file_path = file_path
        self._min_timestamp = min_timestamp
        self._max_timestamp = max_timestamp

    def save(self):
        with open('pk_index.log','a') as f:
            log = {'min_timestamp':self._min_timestamp,'max_timestamp':self._max_timestamp,'file_path':self._file_path}
            f.write(json.dumps(log))
            f.write('\n')

    @classmethod
    def load(cls)->list:
        pk_records = []
        if not os.path.exists('pk_index.log'):
            return pk_records
        with open('pk_index.log','rb') as f:
            while line := f.readline():
                if line == '\n':
                    continue
                log = json.loads(line)
                pk_records.append(PKRecord(**log))
        return pk_records
        
    @property
    def min_timestamp(self)->int:
        return self._min_timestamp

    @property
    def max_timestamp(self)->int:
        return self._max_timestamp
    
    @property
    def file_path(self):
        return self._file_path


class PKRecordSearch:

    def __init__(self) -> None:
        pass

    def search(self,start_time,end_time):
        result = set()
        for pk_record in PKRecord.load():
            logger.debug('start_time=%s,end_time=%s'%(start_time,end_time))
            if pk_record.max_timestamp<start_time or pk_record.min_timestamp>=end_time:
                continue
            result.add(pk_record.file_path)
            logger.debug('search result',pk_record = pk_record.file_path)
        return result