from dataclasses import dataclass
from enum import Enum


class Mode(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass
class ServerConfig:
    mode: Mode
