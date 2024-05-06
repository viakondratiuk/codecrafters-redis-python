from dataclasses import dataclass, field
from enum import Enum


class Mode(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ServerConfig:
    host: str
    port: int
    mode: Mode = field(default=Mode.MASTER)
    master_host: str = None
    master_port: int = None
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0


@dataclass
class Result:
    data: str | list
    type: str
