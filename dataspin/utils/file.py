
import os
import json
import gzip


class DataFileReader:

    def __init__(self, file_path=None, file=None, ext=None, **kwargs):
        self._file = file
        self._file_path = file_path
        self._ext = ext

    def readlines(self):
        if self._file:
            if self._ext.endswith('.gz'):
                with gzip.open(self._file, mode='rt', encoding='utf-8') as gdata:
                    for line in gdata:
                        yield (json.loads(line), line.strip())
            else:
                for line in self._file.readlines():
                    yield (json.loads(line), line.strip())
        elif self._file_path:
            if self._ext.endswith('.gz'):
                with gzip.open(self._file_path, mode='rt', encoding='utf-8') as gdata:
                    for line in gdata:
                        yield (json.loads(line), line.strip())
            else:
                with open(self._file_path, 'rt') as f:
                    for line in f.readlines():
                        yield (json.loads(line), line.strip())
