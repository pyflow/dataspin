from dataclasses import dataclass, field
import json
import dataclass_factory
from typing import List, Optional
from basepy.log import logger

@dataclass
class DataSpinConfig:
    working_dir: Optional[str] = None

@dataclass
class StreamConfig:
    name: str
    url: str
    data_format: Optional[str] = "dataspin"
    args: Optional[dict] = field(default_factory=dict)

@dataclass
class StorageConfig:
    name: str
    url: str
    args: Optional[dict] = field(default_factory=dict)

@dataclass
class PrimaryKeyCacheConfig:
    name: str
    url: str
    timeout: str
    description: Optional[str] = ""

@dataclass
class WebhookConfig:
    url: str
    default_action: str
    schema: Optional[dict] = field(default_factory=dict)

@dataclass
class ProcessFunctionConfig:
    name: str
    function: str
    args: Optional[dict] = field(default_factory=dict)

@dataclass
class DataProcessConfig:
    name: str
    source: str
    description: Optional[str] = ""
    processes: Optional[List[ProcessFunctionConfig]] = field(default_factory=list)

@dataclass
class ProjectConfig:
    dataspin: DataSpinConfig = field(default_factory=DataSpinConfig)
    streams: Optional[List[StreamConfig]] = field(default_factory=list)
    storages: Optional[List[StorageConfig]] = field(default_factory=list)
    pk_caches: Optional[List[PrimaryKeyCacheConfig]] = field(default_factory=list)
    webhooks: Optional[List[WebhookConfig]] = field(default_factory=list)
    data_processes: Optional[List[DataProcessConfig]] = field(default_factory=list)

    @classmethod
    def load(cls, project_fp):
        factory = dataclass_factory.Factory()
        with open(project_fp, 'rb') as pf:
            data = json.loads(pf.read())
            conf = factory.load(data, ProjectConfig)
            logger.debug("loaded project: ", conf=factory.dump(conf))
            return conf
