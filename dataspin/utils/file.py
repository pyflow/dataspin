
import os
import json
import gzip

class DataFileReader:
    def __init__(self, file_path, **kwargs):
        self.file_path = file_path
        self.ext = os.path.splitext(self.file_path)[-1]
    
    def readlines(self):
        if self.ext == '.gz':
            with gzip.open(self.file_path, mode='rt', encoding='utf-8') as gdata:
                for line in gdata:
                    yield (json.loads(line), line)
        else:
            with open(self.file_path, 'rt') as f:
                for line in f.readlines():
                    yield (json.loads(line), line)
