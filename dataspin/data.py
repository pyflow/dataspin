from dataclasses import asdict, dataclass, field
from datetime import datetime
from urllib.parse import urlparse
import json
from typing import Optional
from pathlib import Path
from .utils.common import random_id

@dataclass
class DataFileMessage:
    file_url: str
    tags: Optional[list] = field(default_factory=list)
    datetime_start: Optional[datetime] = None
    datetime_end: Optional[datetime] = None
    storage_type: str = field(init=False)
    bucket: str = field(init=False)
    path: str = field(init=False)

    def __post_init__(self):
        parts = urlparse(self.file_url)
        self.storage_type = parts.scheme.lower()
        if self.storage_type == 'file':
            self.bucket = ''
            if parts.netloc == '':
                self.path = parts.path
            else:
                self.path = f'{parts.netloc}{parts.path}'
        else:
            self.bucket = parts.netloc
            self.path = parts.path

    def __str__(self) -> str:
        return json.dumps(asdict(self))

    def to_dict(self):
        return {
            'data_format': 'dataspin',
            'file_url': self.file_url,
            'tags': self.tags
        }

    def marshal(self):
        return json.dumps(self.to_dict())

    @classmethod
    def unmarshal(cls, content):
        loaded = json.loads(content)
        if 'data_format' in loaded and loaded['data_format'] == 'dataspin':
            loaded.pop('data_format')
            return cls(**loaded)
        else:
            raise Exception('can not unmarshal message to DataFileMessage')

    @classmethod
    def unmarshal_data(cls, data):
        if 'data_format' in data:
            data.pop('data_format')
        return cls(**data)


class AppSystemData:
    def __init__(self):
        self.data_dir = Path.home() /  '.dataspin'
        self.data_file = self.data_dir / 'data.json'
        self.data = {}
        if self.data_dir.exists() and self.data_file.exists:
            self.load()
        else:
            self.create()

    @property
    def node_id(self):
        return self.data.get('node_id')

    def load(self):
        with self.data_file.open() as f:
            self.data = json.loads(f.read())

    def create(self):
        self.data_dir.mkdir(exist_ok=True)
        node_id = random_id('DN')
        self.data = dict(node_id=node_id, created=datetime.now().isoformat())
        self.save()
        self.load()

    def save(self):
        self.data_file.write_text(json.dumps(self.data))