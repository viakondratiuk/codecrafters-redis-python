import asyncio
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


class Mode(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


@dataclass(frozen=True)
class Address:
    host: str
    port: int


@dataclass
class ServerConfig:
    my: Address
    mode: Mode = field(default=Mode.MASTER)
    replicas: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = field(default_factory=list)
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0
    master: Optional[Address] = None
