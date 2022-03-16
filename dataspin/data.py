from dataclasses import asdict, dataclass, field
from datetime import datetime
from urllib.parse import urlparse
import json
from typing import Optional


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
