import time
from enum import Enum

from app.dataclasses import ServerConfig
from app.encoder import Encoder
from app.utils import read_db
from app.constants import EMPTY_RDB

MEMO = {}
PXS = {}


class Command(str, Enum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    INFO = "info"
    REPLCONF = "replconf"
    PSYNC = "psync"


# TODO: Maybe rename as response to, or send_ping, respond_ping
class CommandRunner:
    @staticmethod
    def run(command: Command, config: ServerConfig, *args):
        match command:
            case Command.PING.value:
                return CommandRunner.ping()
            case Command.ECHO.value:
                return CommandRunner.echo(*args)
            case Command.GET.value:
                return CommandRunner.get(*args)
            case Command.SET.value:
                return CommandRunner.set(*args)
            case Command.INFO.value:
                return CommandRunner.info(config, *args)
            case Command.REPLCONF.value:
                return CommandRunner.replconf(*args)
            case Command.PSYNC.value:
                return CommandRunner.psync(config, *args) + CommandRunner.rdb_file(*args)
            case _:
                return CommandRunner.unknown()

    def unknown():
        return Encoder.error("Unknown command")

    @staticmethod
    def ping():
        return Encoder.simple_string("PONG")

    @staticmethod
    def echo(*args):
        if len(args) == 0:
            return Encoder.error("ECHO command requires a value")
        value = args[0]
        return Encoder.bulk_string(value)

    @staticmethod
    def get(*args):
        if len(args) == 0:
            return Encoder.error("GET command requires a key")

        key = args[0]
        px = PXS.get(key, float("inf"))
        if time.time() * 1000 > px:
            MEMO.pop(key, None)
            PXS.pop(key, None)
            return Encoder.null_bulk_string()

        value = MEMO.get(key)
        if value is None:
            return Encoder.null_bulk_string()

        return Encoder.bulk_string(value)

    @staticmethod
    def set(*args):
        if len(args) < 2:
            return Encoder.error("SET command requires a key and value")
        key, value = args[0], args[1]
        px = float("inf")
        if len(args) > 2 and args[2] == "PX".lower():
            if len(args) < 4:
                return Encoder.error(
                    "SET command with PX requires a valid expiration time"
                )
            px = time.time() * 1000 + int(args[3])
        MEMO[key] = value
        PXS[key] = px

        return Encoder.simple_string("OK")

    @staticmethod
    def info(config: ServerConfig, *args):
        if len(args) == 0:
            return Encoder.error("INFO command requires an argument")

        info = [
            f"role:{config.mode.value}",
            f"master_replid:{config.master_replid}",
            f"master_repl_offset:{config.master_repl_offset}",
        ]
        return Encoder.bulk_string(",".join(info))

    @staticmethod
    def replconf(*args):
        return Encoder.simple_string("OK")

    @staticmethod
    def psync(config: ServerConfig, *args):
        return Encoder.simple_string(f"FULLRESYNC {config.master_replid} 0")
    
    @staticmethod
    def rdb_file(*args):
        data = read_db(EMPTY_RDB)
        return Encoder.rdb_file(data)


class CommandBuilder:
    @staticmethod
    def build(*args):
        # return Encoder.array([Encoder.bulk_string(a, is_encode=False) for a in args])
        return Encoder.array([Encoder.bulk_string(a) for a in args])
