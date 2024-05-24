import logging
import time
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from app.constants import EMPTY_RDB
from app.dataclasses import ServerConfig
from app.encoder import RESPEncoder
from app.utils import read_db

logging.basicConfig(level=logging.INFO)


class Command(str, Enum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    INFO = "INFO"
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"
    WAIT = "WAIT"


@dataclass
class CommandHandler:
    config: ServerConfig
    is_propagated: bool = False
    is_server_answer: bool = False
    is_replica: bool = False

    @abstractmethod
    def handle(self, *args) -> list[bytes]:
        pass


@dataclass
class PingHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        return RESPEncoder.simple_string("PONG")


@dataclass
class EchoHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        if len(args) == 0:
            return RESPEncoder.error("ECHO command requires a value")
        value = args[0]
        return RESPEncoder.bulk_string(value)


@dataclass
class GetHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        if len(args) == 0:
            return RESPEncoder.error("GET command requires a key")

        key = args[0]
        _, px = self.config.data_store.get(key)
        if time.time() * 1000 > px:
            self.config.data_store.pop(key)
            return RESPEncoder.null_bulk_string()

        value, _ = self.config.data_store.get(key)
        if value is None:
            return RESPEncoder.null_bulk_string()

        return RESPEncoder.bulk_string(value)


@dataclass
class SetHandler(CommandHandler):
    def __post_init__(self):
        self.is_propagated = True

    def handle(self, *args) -> list[bytes]:
        if len(args) < 2:
            return RESPEncoder.error("SET command requires a key and value")
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return RESPEncoder.error(
                    "SET command with PX requires a valid expiration time"
                )
            px = time.time() * 1000 + int(args[3])
        self.config.data_store.set(key, value, px)

        return RESPEncoder.simple_string("OK")


@dataclass
class InfoHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        if len(args) == 0:
            return RESPEncoder.error("INFO command requires an argument")

        info = [
            f"role:{self.config.mode.value}",
            f"master_replid:{self.config.master_replid}",
            f"master_repl_offset:{self.config.master_repl_offset}",
        ]
        return RESPEncoder.bulk_string(",".join(info))


@dataclass
class GetAckHandler(CommandHandler):
    def __post_init__(self):
        self.is_server_answer = True

    def handle(self, *args) -> list[bytes]:
        response = ["REPLCONF", "ACK", str(self.config.offset)]
        return RESPEncoder.array([RESPEncoder.bulk_string(a) for a in response])


@dataclass
class ListeningPortHandler(CommandHandler):
    def __post_init__(self):
        self.is_replica = True

    def handle(self, *args) -> list[bytes]:
        return RESPEncoder.simple_string("OK")


@dataclass
class ReplConfHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        return RESPEncoder.simple_string("OK")


@dataclass
class PsyncHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        data = read_db(EMPTY_RDB)
        return RESPEncoder.simple_string(
            f"FULLRESYNC {self.config.master_replid} 0"
        ) + RESPEncoder.rdb_file(data)


@dataclass
class WaitHandler(CommandHandler):
    def handle(self, *args) -> list[bytes]:
        return RESPEncoder.integer(0)


class CommandsRunner:
    @staticmethod
    def get_handler(cmd: str, *args) -> Optional[CommandHandler]:
        match cmd:
            case Command.PING.value:
                return PingHandler
            case Command.ECHO.value:
                return EchoHandler
            case Command.GET.value:
                return GetHandler
            case Command.SET.value:
                return SetHandler
            case Command.INFO.value:
                return InfoHandler
            case Command.REPLCONF.value:
                if "GETACK" in args:
                    return GetAckHandler
                elif "listening-port" in args:
                    return ListeningPortHandler
                else:
                    return ReplConfHandler
            case Command.PSYNC.value:
                return PsyncHandler
            case Command.WAIT.value:
                return WaitHandler
            case _:
                return None

    @staticmethod
    def create(config: ServerConfig, cmd: str, *args) -> Optional[CommandHandler]:
        handler_cls = CommandsRunner.get_handler(cmd, *args)
        if handler_cls is not None:
            return handler_cls(config)
        return None

    @staticmethod
    def run(cmd_obj: Optional[CommandHandler], *args) -> list[bytes]:
        if cmd_obj is not None:
            logging.info(
                f"{cmd_obj.config.mode.value}:Run command\r\n>> {cmd_obj.__class__.__name__} {args}\r\n"
            )
            response = cmd_obj.handle(*args)
            return response
        else:
            return RESPEncoder.error("Unknown command")


class CommandBuilder:
    @staticmethod
    def build(*args):
        return RESPEncoder.array([RESPEncoder.bulk_string(a) for a in args])
