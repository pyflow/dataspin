from dataclasses import dataclass

@dataclass
class StreamConfig:
    name: str
    url: str

@dataclass
class StorageConfig:
    name: str
    url: str

class ProjectConfig:
    pass

