from dataclasses import asdict, dataclass
import json


@dataclass
class Message:
    def __str__(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class LocalStreamMessage(Message):
    file_path: str


@dataclass
class StreamMessage(Message):
    storage_type: str
    bucket: str
    key: str
    tags: list
