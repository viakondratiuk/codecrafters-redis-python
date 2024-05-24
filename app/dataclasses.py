from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from app.datastore import DataStorePort


class Mode(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ServerConfig:
    host: str
    port: int
    data_store: DataStorePort
    mode: Mode = Mode.MASTER
    replicas: list[tuple[StreamReader, StreamWriter]] = field(default_factory=list)
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0
    offset: int = 0
    master_host: Optional[str] = None
    master_port: Optional[int] = None
    master_reader: Optional[StreamReader] = None
    master_writer: Optional[StreamWriter] = None
