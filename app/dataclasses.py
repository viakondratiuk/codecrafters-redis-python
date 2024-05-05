from dataclasses import dataclass
from enum import Enum


class Mode(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ServerConfig:
    host: str
    port: str
    mode: Mode
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0
