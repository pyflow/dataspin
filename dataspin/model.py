from enum import Enum
import json
from typing import List, Any, Dict

from sqlalchemy import Column, String, Integer, Boolean, Float, DateTime, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from basepy.config import settings

Base = declarative_base()

class BaseModel(Base):
    __abstract__ = True
    created = Column(DateTime, server_default=func.now())
    updated = Column(DateTime, onupdate=func.now())

class NodeModel(BaseModel):
    __tablename__ = "node"
    node_id = Column(String, primary_key=True)
    start_time = Column(DateTime, nullable=False)
    detail = Column(JSONB, nullable=False)


class ProcessModel(BaseModel):
    __tablename__ = "process"
    process_id = Column(String, primary_key=True)
    node_id = Column(String)
    project = Column(String, nullable=False)
    name = Column(String, nullable=False)
    detail = Column(JSONB, nullable=False)
    UniqueConstraint(node_id, project, name)


class ProcessRunModel(BaseModel):
    __tablename__ = "process_run"
    run_id = Column(String, primary_key=True)
    node_id = Column(String)
    project = Column(String)
    process = Column(String)
    trigger = Column(String)
    detail = Column(JSONB, nullable=False)
    UniqueConstraint(node_id, project, process, trigger)

class ProcessRunOnceModel(BaseModel):
    __tablename__ = "process_run_once"
    run_id = Column(String, primary_key=True)
    once_id = Column(String, primary_key=True)
    node_id = Column(String, index=True)
    state = Column(String)
    log = Column(JSONB, nullable=False)
    detail = Column(JSONB, nullable=False)


class SystemDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(settings.db_url)
        session_maker = sessionmaker()
        session_maker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self.session = session_maker()