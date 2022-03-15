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